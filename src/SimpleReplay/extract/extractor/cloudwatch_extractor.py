import logging
import tempfile
import sys
import gzip

import helper.aws_service as aws_service_helper
from .extract_parser import parse_log

logger = logging.getLogger("SimpleReplayLogger")


class CloudwatchExtractor:
    config = None

    def __init__(self, config):
        self.config = config

    def get_extract_from_cloudwatch(self, start_time, end_time):
        cloudwatch_logs = []
        if self.config.get("source_cluster_endpoint"):
            logger.info(f"Extracting logs from source cluster endpoint: {self.config['source_cluster_endpoint']}")
            source_cluster_endpoint = self.config.get("source_cluster_endpoint")
            region = source_cluster_endpoint.split(".")[2]
            endpoint = source_cluster_endpoint.split(".")[0]
            response = aws_service_helper.cw_describe_log_groups(region=region)
            cloudwatch_logs = self._read_cloudwatch_logs(response, endpoint, start_time, end_time, region)
        elif self.config.get("log_location"):
            logger.info(f"Extracting logs for {self.config['log_location']}")
            response = aws_service_helper.cw_describe_log_groups(log_group_name=self.config.get("log_location"),
                                                                 region=self.config.get("region"))
            for log_group in response["logGroups"]:
                log_group_name = log_group["logGroupName"]
                response_stream = aws_service_helper.cw_describe_log_streams(log_group_name, self.config.get("region"))
                endpoint = response_stream['logStreams'][0]['logStreamName']
                cloudwatch_logs = self._read_cloudwatch_logs(response, endpoint, start_time, end_time,
                                                             self.config.get("region"))
        else:
            logger.error(
                "For Cloudwatch Log Extraction, one of source_cluster_endpoint or log_location must be provided"
            )
            sys.exit(-1)
        return cloudwatch_logs

    def _read_cloudwatch_logs(self, response, endpoint, start_time, end_time, region):
        connections = {}
        last_connections = {}
        logs = {}
        databases = set()
        for log_group in response["logGroups"]:
            log_group_name = log_group["logGroupName"]
            stream_batch = aws_service_helper.cw_describe_log_streams(log_group_name=log_group_name,
                                                                      region=region)["logStreams"]
            for stream in stream_batch:
                stream_name = stream["logStreamName"]
                if endpoint == stream_name:
                    logger.info(
                        f"Extracting for log group: {log_group_name} between time {start_time} and {end_time}"
                    )

                    log_list = aws_service_helper.cw_get_paginated_logs(log_group_name, stream["logStreamName"],
                                                                        start_time,
                                                                        end_time,
                                                                        region)
                    if 'useractivitylog' in log_group_name:
                        log_type = 'useractivitylog'
                    elif 'connectionlog' in log_group_name:
                        log_type = 'connectionlog'
                    else:
                        logger.warning(f'Unsupported log file {log_group_name}, cannot determine type')
                        continue

                    with tempfile.TemporaryDirectory(suffix='SimpleReplay') as tempdir:
                        with gzip.open(f"{tempdir}/{log_type}.gz",'wt') as gzip_file:
                            gzip_file.write("\n".join(log_list))
                        
                        with gzip.open(f"{tempdir}/{log_type}.gz","r") as gzip_file:
                            parse_log(gzip_file, f"{log_type}.gz", connections,
                                    last_connections,
                                    logs,
                                    databases, start_time, end_time)

        return connections, logs, databases, last_connections
