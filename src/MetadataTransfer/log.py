import logging
import sys

def setup_custom_logger(name):
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(message)s',
        '%m-%d %H:%M:%S'
    )

    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)
    
    return logger
