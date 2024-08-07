#!/usr/bin/env python3

import argparse
import csv
import io
import json
import dataclasses
import sys
import typing
import random
import boto3


@dataclasses.dataclass
class Entry:
    s3_url: str
    size: int

    def to_json_dict(self) -> dict:
        """Returns a representation of the entry suitable as an entry in the Redshift Manifest JSON representation."""
        return dict(
            url=self.s3_url,
            mandatory=True,
            meta=dict(content_length=self.size)
        )


def parse_input_csv(file_obj: io.FileIO, bucket: typing.Optional[str]) -> typing.Iterator[Entry]:
    """Returns the CSV parsed into an iterator of entries."""
    if bucket is not None:
        raise ValueError('Bucket cannot be specified for CSV input')
    reader = csv.reader(file_obj, dialect='excel')
    # Ignore header row.
    next(reader)
    for row in reader:
        url = row[0]
        size = int(row[1])
        if size > 0:
            yield Entry(url, size)


def parse_s3_list_response(bucket: str, response: dict) -> typing.Iterator[Entry]:
    """Parses the S3 list objects response into an iterator of entries."""
    for summary in response['Contents']:
        obj_key = summary['Key']
        yield Entry(f's3://{bucket}/{obj_key}', summary['Size'])


def parse_input_json(file_obj: io.FileIO, bucket: typing.Optional[str]) -> typing.Iterator[Entry]:
    """Parses the JSON from `aws s3api list-objects --output=json` into an iterator of entries."""
    if bucket is None:
        raise ValueError('Bucket must be specified for AWS CLI list-object JSON')
    list_json = json.load(file_obj)
    yield from parse_s3_list_response(bucket, list_json)


def s3_list_entries(client: boto3.client, bucket: str, prefix: str) -> typing.Iterator[Entry]:
    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from parse_s3_list_response(bucket, page)


def generate_manifest(entries: typing.Iterable[Entry]) -> dict:
    """Generates the Redshift manifest dictionary from the given iterable of entries."""
    entries = list(entry.to_json_dict() for entry in entries)
    return dict(entries=entries)


def iter_batch(entries: typing.List[Entry], num_batches: int) -> typing.Iterator[typing.List[Entry]]:
    """Generates each batch from the list of entries, evenly divided."""
    base_size, remainder = divmod(len(entries), num_batches)
    consumed_count = 0
    for curr_batch in range(num_batches):
        batch_size = base_size
        if curr_batch < remainder:
            # spread out the remainder on the first N batches
            batch_size += 1
        curr_start = consumed_count
        curr_end = consumed_count + batch_size
        yield entries[curr_start:curr_end]
        consumed_count += batch_size
    assert consumed_count == len(entries)

def main():
    parser = argparse.ArgumentParser(
        prog='manifest.py',
        description='Generates N Redshift COPY manifests from input.'
    )
    # Either source the input from local file or directly from S3 list
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input-file', dest='input_file_name', metavar='FILENAME', type=str, default=None,
                       help='Input file name, a CSV file with S3 URL and file size or a JSON output from the AWS CLI ' +
                            'list-objects.')
    group.add_argument('--stdin', dest='use_stdin', action='store_const', default=False, const=True,
                       help='Read input from stdin instead of reading from file.')
    group.add_argument('--s3-list', dest='do_s3_list', action='store_const', const=True, default=False,
                       help='Use S3 list-objects to source the manifest.  Implies --json.')

    parser.add_argument('--input-prefix', dest='input_prefix', metavar='PREFIX', type=str, default=None,
                        help='The input prefix for --s3-list.  Only applicable for this mode.')
    parser.add_argument('--prefix', type=str, required=True,
                        help='Output file prefix')
    parser.add_argument('--num', metavar='N', type=int, required=True,
                        help='Number of manifests to generate.')
    parser.add_argument('--bucket', type=str, default=None,
                        help='S3 bucket name, required if the input is S3 list-objects JSON ' +
                             'or if --s3-list is used.')
    parser.add_argument('--csv', dest='parse_func', action='store_const',
                        const=parse_input_csv, default=parse_input_json,
                        help='Parse a CSV file into the entries for the manifest.')
    parser.add_argument('--upload-bucket', type=str, default=None,
                        help='S3 bucket to upload the resulting manifests to, uses --prefix as the prefix to write to.')
    parser.add_argument('--no-shuffle', dest='do_shuffle', action='store_const', default=True, const=False,
                        help='Does not shuffle the input entries before generating the manifests.')

    args = parser.parse_args()

    s3 = boto3.client('s3')
    if args.do_s3_list:
        if args.bucket is None:
            raise ValueError('Bucket must be specified with --s3-list')
        if args.input_prefix is None:
            raise ValueError('Input prefix must be specified with --s3-list')
        print(f'Generating manifest from listing objects in s3://{args.bucket}/{args.input_prefix}', file=sys.stderr)
        entries = list(s3_list_entries(s3, args.bucket, args.input_prefix))
    else:
        if args.input_prefix is not None:
            raise ValueError('Input prefix must not be specified in file input mode')
        if args.use_stdin:
            print(f'Parsing standard input...', file=sys.stderr)
            in_file = sys.stdin
        else:
            print(f'Parsing {args.input_file_name}...', file=sys.stderr)
            in_file = open(args.input_file_name, 'r')
        with in_file:
            # Parse out the relevant files to generate manifests from.
            entries = list(args.parse_func(in_file, args.bucket))
    if args.do_shuffle:
        # Randomize the input.
        random.shuffle(entries)
    count = len(entries)
    if count < args.num:
        raise ValueError('Number of manifests to generate is less than the total number of entries')

    for i, batch in enumerate(iter_batch(entries, args.num)):
        batch_gb = sum(x.size for x in batch) / 1024.0 / 1024.0 / 1024.0
        print(f'Generating batch {i}, {len(batch)} entries, {batch_gb:.2f} GB.', file=sys.stderr)
        manifest = generate_manifest(batch)
        manifest_text = json.dumps(manifest, indent=2)
        out_name = f'{args.prefix}-{i:03d}.json'
        if args.upload_bucket is not None:
            print(f'Uploading s3://{args.upload_bucket}/{out_name}', file=sys.stderr)
            s3.put_object(Bucket=args.upload_bucket, Key=out_name, ContentType='application/json', Body=manifest_text)
            pass
        else:
            print(f'Writing manifest to file {out_name}', file=sys.stderr)
            with open(out_name, 'w') as f:
                f.write(manifest_text)


if __name__ == '__main__':
    main()
