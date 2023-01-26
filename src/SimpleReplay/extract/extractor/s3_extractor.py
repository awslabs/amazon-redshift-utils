import gzip
import logging

import helper.aws_service as aws_service_helper
from tqdm import tqdm

import log_validation
from .extract_parser import parse_log

logger = logging.getLogger("SimpleReplayLogger")

class S3Extractor:
    disable_progress_bar = None
    bar_format = (
        "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
    )

    def __init__(self, config):
        self.disable_progress_bar = config.get("disable_progress_bar")

    def get_extract_from_s3(self, log_bucket, log_prefix, start_time, end_time):
        """
        getting logs from s3 and passing it to get_s3_audit_logs()
        :param log_bucket:
        :param log_prefix:
        :param start_time:
        :param end_time:
        :return:
        """
        connections = {}
        logs = {}
        last_connections = {}
        databases = set()

        bucket_objects = aws_service_helper.s3_get_bucket_contents(log_bucket, log_prefix)

        s3_connection_logs = []
        s3_user_activity_logs = []

        for log in bucket_objects:
            filename = log["Key"].split("/")[-1]
            if "connectionlog" in filename:
                s3_connection_logs.append(log)
            elif "useractivitylog" in filename:
                s3_user_activity_logs.append(log)

        logger.info("Parsing connection logs")
        self._get_s3_audit_logs(
            log_bucket,
            log_prefix,
            start_time,
            end_time,
            s3_connection_logs,
            connections,
            logs,
            databases,
            last_connections,
        )
        logger.info("Parsing user activity logs")
        self._get_s3_audit_logs(
            log_bucket,
            log_prefix,
            start_time,
            end_time,
            s3_user_activity_logs,
            connections,
            logs,
            databases,
            last_connections,
        )
        return connections, logs, databases, last_connections

    def _get_s3_audit_logs(
            self,
            log_bucket,
            log_prefix,
            start_time,
            end_time,
            audit_objects,
            connections,
            logs,
            databases,
            last_connections,
    ):
        """
        Getting  audit logs from S3 for the cluster from get_s3_logs  and calling the pasrse_log()

        :param log_bucket:
        :param log_prefix:
        :param start_time:
        :param end_time:
        :param audit_objects:
        :param connections:
        :param logs:
        :param databases:
        :param last_connections:
        :return:
        """

        index_of_last_valid_log = len(audit_objects) - 1

        log_filenames = log_validation.get_logs_in_range(audit_objects, start_time, end_time)

        logger.info(f"Processing {len(log_filenames)} files")

        curr_index = index_of_last_valid_log
        for filename in tqdm(
                log_filenames,
                disable=self.disable_progress_bar,
                unit="files",
                desc="Files processed",
                bar_format=self.bar_format,
        ):
            log_object = aws_service_helper.s3_get_object(log_bucket, filename)
            log_file = gzip.GzipFile(fileobj=log_object.get()["Body"])

            parse_log(
                log_file,
                filename,
                connections,
                last_connections,
                logs,
                databases,
                start_time,
                end_time,
            )

        logger.debug(
            f'First audit log in start_time range: {audit_objects[curr_index]["Key"].split("/")[-1]}'
        )
        return connections, logs, databases, last_connections
