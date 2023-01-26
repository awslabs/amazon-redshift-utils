import sys
import dateutil
import yaml
import threading
from collections import OrderedDict
sys.path.insert(1, '../')

from util import (
    add_logfile,
    logging,
    cluster_dict
)
from helper.config import get_config_file_from_args
from glue_util import clone_glue_catalog
from helper.aws_service import redshift_execute_query
from copy_util import clone_objects_to_s3, get_s3_folder_size, check_file_existence
from helper.log import init_logging
logger = logging.getLogger("SimpleReplayLogger")
g_disable_progress_bar = None
global_lock = threading.Lock()
g_bar_format = "{desc}: {percentage:3.0f}%|{bar}"

def main():
    # Parse config file
    file_config = get_config_file_from_args()

    # Setup Logging
    level = logging.getLevelName(file_config.get("log_level", "INFO").upper())
    if file_config.get("logfile_level") != "none":
        level = logging.getLevelName(file_config.get("logfile_level", "DEBUG").upper())
    init_logging(filename="external_replicator.log", dir="external_replicator_logs",
                 level=level, preamble=yaml.dump(file_config),
                 backup_count=file_config.get("backup_count", 2), script_type='external object replicator')

    cluster_object = cluster_dict(endpoint=file_config["source_cluster_endpoint"])
    start_time = dateutil.parser.parse(file_config["start_time"]).astimezone(dateutil.tz.tzutc())
    end_time = dateutil.parser.parse(file_config["end_time"]).astimezone(dateutil.tz.tzutc())
    redshift_user = file_config["redshift_user"]

    STL_LOAD_response, copy_objects_not_found, copy_source_location = execute_stl_load_query(cluster_object, end_time, file_config,
                                                                           redshift_user, start_time)
    SVL_S3LIST_result, spectrum_source_location, external_table_response, spectrum_obj_not_found = execute_SVL_query(
        cluster_object, end_time, file_config, redshift_user, start_time)

    options = ["1. Yes - Proceed with cloning", "2. No - Exit"]
    print("Would you like to proceed with cloning?")
    print(options[0])
    print(options[1])
    for idx, element in enumerate(options):
        choice = input("Enter your choice: ")
        if int(choice) == 1:
            if STL_LOAD_response["TotalNumRows"] > 0:
                logger.info(f"== Begin to clone COPY files to {file_config['target_s3_location']} ==")
                clone_objects_to_s3(file_config["target_s3_location"], obj_type='copyfiles',
                                    source_location=copy_source_location, objects_not_found=copy_objects_not_found)
            if SVL_S3LIST_result["TotalNumRows"] > 0:
                logger.info("== Begin to clone Glue databases and tables ==")
                new_gluedb_list = clone_glue_catalog(external_table_response["Records"], file_config["target_s3_location"],
                                                     file_config["region"])
                logger.info(f"== Begin to clone Spectrum files to {file_config['target_s3_location']} ==")
                clone_objects_to_s3(file_config["target_s3_location"],objects_not_found=spectrum_obj_not_found,
                                    source_location=spectrum_source_location, obj_type='spectrumfiles')
            if SVL_S3LIST_result["TotalNumRows"] == 0 and STL_LOAD_response["TotalNumRows"] ==0:
                logger.info("No object found to be replicated")
            exit(-1)
        else:
            logger.info("Customer decided not to proceed with cloning")
            exit(-1)

def execute_SVL_query(cluster_object, end_time, file_config, redshift_user, start_time):
    with open('sql/external_table_query.sql', "r") as svv_external_table:
        external_table_query = svv_external_table.read().format(start=start_time, end=end_time, db=cluster_object['database'])
    logger.info("Scanning system tables to find Glue databases and tables")
    external_table_response = redshift_execute_query(cluster_object["id"], redshift_user, cluster_object['database'],
                                                     file_config["region"], external_table_query)
    spectrum_source_location = []
    spectrum_obj_not_found = []
    # Query Spectrum files
    logger.info("Scanning system tables to find Spectrum files queried by source cluster")
    SVL_S3LIST_result = OrderedDict()
    with open('sql/svl_s3_list.sql', "r") as svl_s3_list:
        SVL_S3LIST_query = svl_s3_list.read().format(start=start_time, end=end_time, db=cluster_object['database'])
    SVL_S3LIST_result = redshift_execute_query(cluster_object["id"], redshift_user, cluster_object['database'],
                                               file_config["region"], SVL_S3LIST_query)
    total_SVL_S3_List_scan = []
    for record in SVL_S3LIST_result["Records"]:
        total_SVL_S3_List_scan.append([{'stringValue': f"{record[0]['stringValue']}{'/'}{record[1]['stringValue']}"}])
    if SVL_S3LIST_result["TotalNumRows"] > 0:
        logger.info(
            f"{len(SVL_S3LIST_result['Records'])} folders detected across Spectrum queries from the SVL_S3LIST system "
            f"table between {start_time} and {end_time}")
        spectrum_source_location, spectrum_obj_not_found = check_file_existence(SVL_S3LIST_result, 'spectrumfiles')
        logger.info(
            f"Number of Spectrum files that can be replicated: "
            f"{(len(spectrum_source_location))} files"
        )
        logger.info(
            f"Total size of Spectrum files that can be replicated: "
            f"{get_s3_folder_size(copy_file_list=spectrum_source_location)}"
        )
    else:
        logger.info("No Spectrum files found.")
    return SVL_S3LIST_result, spectrum_source_location, external_table_response, spectrum_obj_not_found


def execute_stl_load_query(cluster_object, end_time, file_config, redshift_user, start_time):
    # Query COPY objects
    copy_objects_not_found = []
    copy_source_location = []
    STL_LOAD_response = OrderedDict()
    with open('sql/stl_load_query.sql', "r") as stl_load:
        STL_LOAD_query = stl_load.read().format(start=start_time, end=end_time, db=cluster_object['database'])
    STL_LOAD_response = redshift_execute_query(cluster_object["id"], redshift_user, cluster_object['database'],
                                               file_config["region"], STL_LOAD_query)
    logger.info("Scanning system tables to find COPY files")
    logger.debug(f"Executing SQL Query to find COPY files")
    if STL_LOAD_response["TotalNumRows"] > 0:
        logger.info(
            f"{len(STL_LOAD_response['Records'])} files detected across COPY queries from the STL_LOAD_COMMIT system "
            f"table between {start_time} and {end_time}")
        copy_source_location, copy_objects_not_found = check_file_existence(STL_LOAD_response, 'copyfiles')
        logger.info(f"Percentage of COPY files that can be replicated: "
                    f"{((len(copy_source_location)) / len(STL_LOAD_response['Records'])) * 100}%")
        logger.info(
            f"Total size of COPY files that can be replicated: "
            f"{get_s3_folder_size(copy_file_list=copy_source_location)}"
        )
    else:
        logger.info("No COPY files found.")
    return STL_LOAD_response, copy_objects_not_found, copy_source_location


if __name__ == "__main__":
    main()