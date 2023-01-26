import argparse
import logging
import yaml
import sys
import dateutil

logger = logging.getLogger("SimpleReplayLogger")


def get_config_file_from_args():
    """
    getting the CLI arguments
    :return: CLI argumets
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config_file",
        type=argparse.FileType("r"),
        help="Location of extraction config file.",
    )
    args = parser.parse_args()
    config_file = args.config_file
    with config_file as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exception:
            sys.exit(f"Failed to parse extraction config yaml file: {exception}")


def validate_config_file_for_extract(config):
    """
    Validates the parameters from extract.yaml file
    :param config: extract.yaml file
    :return:
    """
    if config["source_cluster_endpoint"]:
        if "redshift-serverless" in config["source_cluster_endpoint"]:
            if (
                    not len(config["source_cluster_endpoint"].split(".")) == 6
                    or not len(config["source_cluster_endpoint"].split(":")) == 2
                    or not len(config["source_cluster_endpoint"].split("/")) == 2
            ):
                logger.error(
                    'Config file value for "source_cluster_endpoint" is not a valid endpoint. Endpoints must be in '
                    'the format of <identifier>.<region>.redshift-serverless.amazonaws.com:<port>/<database-name>. '
                )
                exit(-1)
        elif (
                not len(config["source_cluster_endpoint"].split(".")) == 6
                or not len(config["source_cluster_endpoint"].split(":")) == 2
                or not len(config["source_cluster_endpoint"].split("/")) == 2
                or ".redshift.amazonaws.com:" not in config["source_cluster_endpoint"]
        ):
            logger.error(
                'Config file value for "source_cluster_endpoint" is not a valid endpoint. Endpoints must be in the '
                'format of <cluster-name>.<identifier>.<region>.redshift.amazonaws.com:<port>/<database-name>. '
            )
            exit(-1)
        if not config["master_username"]:
            logger.error(
                'Config file missing value for "master_username". Please provide a value or remove the '
                '"source_cluster_endpoint" value. '
            )
            exit(-1)
    else:
        if not config["log_location"]:
            logger.error(
                'Config file missing value for "log_location". Please provide a value for "log_location", or provide '
                'a value for "source_cluster_endpoint". '
            )
            exit(-1)
    if config["start_time"]:
        try:
            dateutil.parser.isoparse(config["start_time"])
        except ValueError:
            logger.error(
                'Config file "start_time" value not formatted as ISO 8601. Please format "start_time" as ISO 8601 or '
                'remove its value. '
            )
            exit(-1)
    if config["end_time"]:
        try:
            dateutil.parser.isoparse(config["end_time"])
        except ValueError:
            logger.error(
                'Config file "end_time" value not formatted as ISO 8601. Please format "end_time" as ISO 8601 or '
                'remove its value. '
            )
            exit(-1)
    if not config["workload_location"]:
        logger.error(
            'Config file missing value for "workload_location". Please provide a value for "workload_location".'
        )
        exit(-1)
    if config["source_cluster_system_table_unload_location"] and not config[
        "source_cluster_system_table_unload_location"
    ].startswith("s3://"):
        logger.error(
            'Config file value for "source_cluster_system_table_unload_location" must be an S3 location (starts with '
            '"s3://"). Please remove this value or put in an S3 location. '
        )
        exit(-1)
    if (
            config["source_cluster_system_table_unload_location"]
            and not config["source_cluster_system_table_unload_iam_role"]
    ):
        logger.error(
            'Config file missing value for "source_cluster_system_table_unload_iam_role". Please provide a value for '
            '"source_cluster_system_table_unload_iam_role", or remove the value for '
            '"source_cluster_system_table_unload_location". '
        )
        exit(-1)
    if (
            config["source_cluster_system_table_unload_location"]
            and not config["unload_system_table_queries"]
    ):
        logger.error(
            'Config file missing value for "unload_system_table_queries". Please provide a value for '
            '"unload_system_table_queries", or remove the value for "source_cluster_system_table_unload_location". '
        )
        exit(-1)
    if config["unload_system_table_queries"] and not config[
        "unload_system_table_queries"
    ].endswith(".sql"):
        logger.error(
            'Config file value for "unload_system_table_queries" does not end with ".sql". Please ensure the value '
            'for "unload_system_table_queries" ends in ".sql". See the provided "unload_system_tables.sql" as an '
            'example. '
        )
        exit(-1)
