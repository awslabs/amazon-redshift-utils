import uuid
import boto3
from tqdm import tqdm

from audit_logs_parsing import logger
from util import (
    init_logging,
    bucket_dict,
    load_config,
    set_log_level,
    add_logfile,
    log_version,
    logging, logger,
)
from helper.aws_service import (
    glue_get_table, glue_get_partition_indexes,
    glue_create_table,glue_get_database,glue_create_database)
def clone_glue_catalog(records, dest_location, region):
    """
    It reads through the systems table to create clone of the database,tables and partitions
    record[3]['stringValue'] : glue database vale from table
    record[2]['stringValue'] : external glue table value
    @param records:
    @return:
    """
    glue_db_append_name = uuid.uuid1()
    new_glue_db_list = []
    checked_db_list = []
    pbar = tqdm(range(len(records)))
    for i in pbar:
        record = records[i]
        original_glue_db = record[3]["stringValue"]
        original_glue_table = record[2]["stringValue"]
        new_glue_db = f"{glue_db_append_name}-{original_glue_db}"
        pbar.set_description_str(f"Cloning {original_glue_table} in {original_glue_db} - {i + 1} out of {len(records)} glue objects")
        # if the database hasn't been checked yet
        if original_glue_db not in checked_db_list:
            database_copy(new_glue_db, original_glue_db, original_glue_table, region)
            checked_db_list.append(original_glue_db)
            new_glue_db_list.append(new_glue_db)
        new_s3_loc = glue_table_copy(
            original_glue_db, new_glue_db, original_glue_table, dest_location, region
        )
    logger.debug(f"New Glue database created: {new_glue_db_list}.")
    logger.info("== Finished cloning Glue databases and tables ==")
    return new_glue_db_list

def database_copy(new_glue_db, original_glue_db, original_glue_table, region):
    """
    create a new database
    @param record: original_glue_db, new_glue_db, original_glue_table
    @return:
    """
    try:
        glue_get_database(name=new_glue_db, region=region)
    except:
        glue_create_database(new_glue_db, "Database clone created by External Object Replicator", region)

    return original_glue_db, new_glue_db, original_glue_table

def glue_table_copy(original_glue_db, new_glue_db, original_glue_table, dest_location, region):
    """
    CHeck if glue table exists in the new glue database, if not create the table structure along with the partitions
    @param orig_glue_db:
    @param new_glue_db:
    @param orig_glue_table:
    @return:
    """
    dest_bucket = bucket_dict(dest_location)['bucket_name']
    try:
        table_get_response = glue_get_table(database=new_glue_db,table=original_glue_table, region=region)
        new_s3_loc = table_get_response["Table"]["StorageDescriptor"]["Location"]
    except:
        table_get_response = glue_get_table(
            database=original_glue_db,
            table=original_glue_table,
            region=region,
        )
        index_response = glue_get_partition_indexes(
            database=original_glue_db,
            table=original_glue_table,
            region=region
        )
        orig_s3_loc = table_get_response["Table"]["StorageDescriptor"][
            "Location"
        ].split("/")
        new_s3_loc = (
            f"{dest_bucket}/spectrumfiles/{'/'.join(orig_s3_loc[2:])}"
        )
        table_input = (
            {
                "Name": table_get_response["Table"]["Name"],
                "Description": "For use with Redshfit candidate release testing",
                "StorageDescriptor": {
                    "Columns": table_get_response["Table"]["StorageDescriptor"][
                        "Columns"
                    ],
                    "Location": new_s3_loc,
                },
                "PartitionKeys": table_get_response["Table"]["PartitionKeys"],
            },
        )
        if index_response["PartitionIndexDescriptorList"]:
            glue_create_table(
                new_database=new_glue_db,
                table_input=table_input.update(
                    {
                        'PartitionIndexes"': index_response[
                            "PartitionIndexDescriptorList"
                        ]
                    }
                ),
                region=region)
        else:
            glue_create_table(
                new_database=new_glue_db,
                table_input=table_input[0],
                region=region)
    return new_s3_loc