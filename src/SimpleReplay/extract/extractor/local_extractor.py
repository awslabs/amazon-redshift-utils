import gzip
import logging
import os

from tqdm import tqdm

from extract.extractor import extract_parser

logger = logging.getLogger("SimpleReplayLogger")


class LocalExtractor:
    disable_progress_bar = None
    bar_format = (
        "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}{postfix}]"
    )

    def __init__(self, config):
        self.config = config

    def get_extract_locally(self, log_directory_path, start_time, end_time):
        """

        :param log_directory_path:
        :param start_time:
        :param end_time:
        :return:
        """
        connections = {}
        last_connections = {}
        logs = {}
        databases = set()

        unsorted_list = os.listdir(log_directory_path)
        log_directory = sorted(unsorted_list)

        for filename in tqdm(
                log_directory,
                disable=self.disable_progress_bar,
                unit="files",
                desc="Files processed",
                bar_format=self.bar_format,
        ):
            if self.disable_progress_bar:
                logger.info(f"Processing {filename}")
            if "start_node" in filename:
                log_file = gzip.open(
                    log_directory_path + "/" + filename, "rt", encoding="ISO-8859-1"
                )
            else:
                log_file = gzip.open(log_directory_path + "/" + filename, "r")

            extract_parser.parse_log(
                log_file,
                filename,
                connections,
                last_connections,
                logs,
                databases,
                start_time,
                end_time,
            )
            log_file.close()

        return connections, logs, databases, last_connections
