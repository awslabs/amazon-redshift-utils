# Manifest Generator

Data loads into Amazon Redshift tables that have a Sort Key are resource intensive, requiring the system to consume large amounts of memory to sort data. For very large loads, you may observe the run times of `COPY` commands grow non-linearly relative to the data size. Instead of single large multi-TB operations, breaking down the load into chunks could yield faster overall ingestion.

This utility script is to generate Redshift manifest files to be used for `COPY` command to split the ingestion into batches.

## Usage

This utility supports 3 modes for manifest generation, [CSV Input](#csv-input), [AWS CLI Input](#aws-cli-input), and [Direct S3 access](#direct-s3-access)

### CSV Input

Executing in the CSV Input mode requires someone to list the S3 files and construct a CSV input file with two columns, file URL and size and a header row. 

> Note: Manifest-based `COPY` for columnar formats like Parquet/ORC requires file sizes to be specified.

```csv
file_name,file_size
s3://mybucket/mytbl/file1,1004
s3://mybucket/mytbl/file2,2020
s3://mybucket/mytbl/file3,5000
...
```

Execute the script referencing the input CSV file.

```
$ ./manifestgen.py  --prefix="OUTFILE" --num=4 --csv --input-file=INPUT.csv
```

Manifest files are written locally.  

```
Parsing INPUT.csv...
Generating batch 0, 77437 entries, 8416.58 GB.
Writing manifest to file OUTFILE-000.json
Generating batch 1, 77437 entries, 8402.78 GB.
Writing manifest to file OUTFILE-001.json
Generating batch 2, 77437 entries, 8416.99 GB.
Writing manifest to file OUTFILE-002.json
Generating batch 3, 77435 entries, 8418.45 GB.
Writing manifest to file OUTFILE-003.json
```

Work with someone who has S3 write access to upload the generated files to S3. Subsequently, the `COPY` command can be executed pointing to the manifest files.

### AWS CLI Input

Executing in the AWS CLI input mode uses the `list-objects` command to list the files and pipe those results to the generator script. Notice the `--bucket` and `--prefix` parameters are specified which would match the location of the data you want to load.  The script executor needs to have S3 read access. 

```
$ aws s3api list-objects --bucket my-large-ingest-data-bkt --prefix my-large-tbl1/ --output=json | ./manifestgen.py --prefix=OUTFILE --num 4 --bucket=my-large-ingest-data-bkt --stdin
```

Manifest files are written locally.  
 
```
Parsing standard input...
Generating batch 0, 154 entries, 618.65 GB.
Writing manifest to file OUTFILE-000.json
Generating batch 1, 154 entries, 601.95 GB.
Writing manifest to file OUTFILE-001.json
Generating batch 2, 154 entries, 578.85 GB.
Writing manifest to file OUTFILE-002.json
Generating batch 3, 154 entries, 588.11 GB.
Writing manifest to file OUTFILE-003.json
```

Work with someone who has S3 write access to upload the generated manifest files to S3. Subsequently the `COPY` command can be executed pointing to the manifest files.

### Direct S3 access

Executing in Direct S3 mode, the generator script directly performs an S3 list on a prefix, generates the manifest files locally, and uploads the files to destination S3 location. The script executor requires both S3 read access as well as S3 write access. 

```
$ ./manifestgen.py --prefix=manifests/my-large-tbl1/BATCH_4 --num 4 --bucket=my-large-ingest-data-bkt --input-prefix=my-large-tbl1/ --s3-list --upload-bucket=my-large-ingest-data-bkt
```

Manifest files are written locally and subsequently uploaded to S3. 

```
Generating manifest from listing objects in s3://my-large-ingest-data-bkt/my-large-tbl1/
Generating batch 0, 192 entries, 750.44 GB.
Uploading s3://my-large-ingest-data-bkt/manifests/my-large-tbl1/BATCH_4-000.json
Generating batch 1, 192 entries, 750.29 GB.
Uploading s3://my-large-ingest-data-bkt/manifests/my-large-tbl1/BATCH_4-001.json
Generating batch 2, 192 entries, 761.92 GB.
Uploading s3://my-large-ingest-data-bkt/manifests/my-large-tbl1/BATCH_4-002.json
Generating batch 3, 192 entries, 733.61 GB.
Uploading s3://my-large-ingest-data-bkt/manifests/my-large-tbl1/BATCH_4-003.json
```

To review the S3 location for the uploaded manifest files

```
$ aws s3 ls s3://my-large-ingest-data-bkt/manifests/my-large-tbl1/
2024-07-20 23:27:40      34386 BATCH_4-000.json
2024-07-20 23:27:41      34385 BATCH_4-001.json
2024-07-20 23:27:41      34386 BATCH_4-002.json
2024-07-20 23:27:41      34387 BATCH_4-003.json
```

Used the the `COPY` command to load the target table.

For more usage help, run `./manifest -h`

## Requirements

This utility requires Python 3.12 and `boto3` to be already installed on your terminal.

## Working Details and Limitations 

This utility does not determine the optimal number of split batches, you need to provide the input `num` and the utility will provide those many manifest files.

For loading large data sets you can benchmark with the single `COPY` command, and use this utility to experiment with 2/4/8/n batches. Compare the total time and determine the right number of batches that meet your performance needs.
 
# License

Amazon Redshift Utils - Manifest Generator

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This project is licensed under the Apache-2.0 License.
