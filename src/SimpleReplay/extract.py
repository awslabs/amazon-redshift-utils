import logging
import sys

import yaml

import helper.config as config_helper
import helper.log as log_helper
from extract.extractor import Extractor


def main():
    # Parse config file
    config = config_helper.get_config_file_from_args()
    config_helper.validate_config_file_for_extract(config)

    # Setup Logging
    level = logging.getLevelName(config.get("log_level", "INFO").upper())
    log_helper.init_logging("extract.log", level=level,
                            preamble=yaml.dump(config),
                            backup_count=config.get("backup_count", 2))
    log_helper.log_version()

    e = Extractor(config)
    if not e.load_driver():
        sys.exit("Failed to load driver")

    # Run extract job
    (extraction_name, start_time, end_time, log_location) = e.get_parameters_for_log_extraction()
    (connections, audit_logs, databases, last_connections) = e.get_extract(log_location, start_time, end_time)

    e.validate_log_result(connections, audit_logs)
    e.retrieve_cluster_endpoint_info(extraction_name)

    e.save_logs(
        audit_logs,
        last_connections,
        config["workload_location"] + "/" + extraction_name,
        connections,
        start_time,
        end_time,
    )


if __name__ == "__main__":
    main()
