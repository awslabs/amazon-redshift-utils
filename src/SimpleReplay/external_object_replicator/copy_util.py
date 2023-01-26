import csv
import os
import sys
import threading

import boto3
from tqdm import tqdm
from util import (
    bucket_dict, logger,
)
from helper.aws_service import s3_copy_object, s3_get_bucket_contents,s3_upload

def copy_parallel(dest_bucket, dest_prefix, source_location, obj_type):
    threads = [None] * len(source_location)
    pbar = tqdm(range(len(source_location)))
    for i in pbar:
        threads[i] = threading.Thread(target=s3_copy_object, args=(source_location[i]['source_bucket'],
                                                                source_location[i]['source_key'],
                                                                dest_bucket, (
                                                                    f"{dest_prefix}{source_location[i]['source_key']}")))
        threads[i].start()
        pbar.set_description(f"Cloning {source_location[i]['source_key']} - {i + 1} out of {len(source_location)} {obj_type}")

    for i in range(len(source_location)):
        threads[i].join()

def get_s3_folder_size(copy_file_list):
    """
    This function will calculate size of every folder inside the copy_replacement.csv
    @param copy_file_location:
    @return:
    """
    total_size = 0
    size_of_data_value = 0
    for record in copy_file_list:
        prefix = record["source_key"]
        # total_size = total_size + sys.getsizeof(prefix)
        if not prefix.endswith('/'):
            total_size = total_size + record['bytes']
    return size_of_data(total_size)

def check_file_existence(response, obj_type):
    source_location = []
    objects_not_found = []
    for record in response["Records"]:
        if obj_type == "copyfiles":
            source_url = bucket_dict(record[0]["stringValue"])
            source_bucket = source_url['bucket_name']
            source_key = (source_url['prefix'])[:-1]
        else:
            source_bucket = record[0]["stringValue"]
            source_key = record[1]["stringValue"][:-1]
        objects = s3_get_bucket_contents(source_bucket, source_key)
        if len(objects) == 0:  # if no object is found, add it to objects_not_found list
            objects_not_found.append({'source_bucket': source_bucket, 'source_key': source_key})
        else:  # if object is found, append it to source_location to be cloned
            for object in objects:
                source_location.append({'source_bucket': source_bucket, 'source_key': object['Key'],
                                        'e_tag': object['ETag'], 'size': size_of_data(bytes=object['Size']),
                                        'bytes':object['Size'],
                                        'last_modified': object['LastModified']})
    return source_location, objects_not_found

def clone_objects_to_s3(target_dest, obj_type, source_location, objects_not_found):
    dest_location = bucket_dict(target_dest)
    dest_prefix = f"{dest_location['prefix']}{obj_type}/"
    dest_bucket = dest_location['bucket_name']
    if obj_type == 'copyfiles':
        file_output = "Final_Copy_Objects.csv"
        full_object_type = "COPY command files"
    else:
        file_output = "Spectrum_objects_copy_report.csv"
        full_object_type = "Spectrum files"
    copy_parallel(dest_bucket=dest_bucket, dest_prefix=dest_prefix, source_location=source_location,
                  obj_type=full_object_type)
    logger.info(f"{len(source_location)} {full_object_type} cloned to s3://{dest_bucket}/{dest_prefix}")

    with open(file_output, "w") as fp:
        writer = csv.DictWriter(
            fp,fieldnames=["Source Location", "Destination Location", "Size", "Etag", "Last Modified Date"]
        )
        writer.writeheader()

        if (len(source_location) > 0):
            fp.write(f"Cloned below objects: \n")
            for obj in source_location:
                fp.write(f"s3://{obj['source_bucket']}/{obj['source_key']},"
                         f"s3://{dest_bucket}{dest_prefix}{obj['source_key']},{obj['size']},{obj['e_tag']},"
                         f"{obj['last_modified']}\n")
            fp.write(f"Number of objects cloned: {len(source_location)}\n")
        if (len(objects_not_found) > 0):
            fp.write(f"Failed to clone below objects: \n")
            for obj in objects_not_found:
                fp.write(
                    f"s3://{obj['source_bucket']}/{obj['source_key']},Object not found,N/A,N/A,N/A\n")
            fp.write(f"Number of objects not found: {len(objects_not_found)}")
    s3_upload(file_output, bucket=f"{dest_bucket}", key=f"{dest_prefix}{file_output}")
    logger.info(
        f"Details of {full_object_type} cloning uploaded to {dest_bucket}/{dest_prefix}{file_output}"
    )
    logger.info(f"== Finished cloning {full_object_type} ==")
def size_of_data( bytes):
    """
    Categorise bytes in GB,MB,TB
    @param bytes:
    @return:
    """
    bytes = float(bytes)
    kilobytes = float(1024)
    megabytes = float(kilobytes ** 2)  # 1,048,576
    gigabytes = float(kilobytes ** 3)  # 1,073,741,824
    terabytes = float(kilobytes ** 4)  # 1,099,511,627,776

    if bytes < kilobytes:
        return "{0} {1}".format(bytes, "B" if 0 == bytes > 1 else "Byte")
    elif kilobytes <= bytes < megabytes:
        return "{0:.2f} KB".format(bytes / kilobytes)
    elif megabytes <= bytes < gigabytes:
        return "{0:.2f} MB".format(bytes / megabytes)
    elif gigabytes <= bytes < terabytes:
        return "{0:.2f} GB".format(bytes / gigabytes)
    elif terabytes <= bytes:
        return "{0:.2f} TB".format(bytes / terabytes)
