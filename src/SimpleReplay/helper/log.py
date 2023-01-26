import logging
import logging.handlers
import time
import os

log_date_format = "%Y-%m-%d %H:%M:%S"

def log_version():
    """ Read the VERSION file and log it """
    logger = logging.getLogger("SimpleReplayLogger")
    try:
        with open("VERSION", "r") as fp:
            logger.info(f"Version {fp.read().strip()}")
    except:
        logger.warning(f"Version unknown")

def init_logging(filename, dir="simplereplay_logs", level=logging.DEBUG, backup_count=2, preamble='',
                 script_type='extract'):
    """ Initialize logging to stdio """
    logger = logging.getLogger("SimpleReplayLogger")
    logger.setLevel(level)
    logging.Formatter.converter = time.gmtime
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(get_log_formatter())
    logger.addHandler(ch)

    """ Additionally log to a logfile """
    os.makedirs(dir, exist_ok=True)
    filename = f"{dir}/{filename}"
    file_exists = os.path.isfile(filename)
    fh = logging.handlers.RotatingFileHandler(filename, backupCount=backup_count)

    # if the file exists from a previous run, rotate it
    if file_exists:
        fh.doRollover()

    # dump the preamble to the file first
    if preamble:
        with open(filename, "w") as fp:
            fp.write(preamble.rstrip() + '\n\n' + '-' * 40 + "\n")

    fh.setLevel(level)
    fh.setFormatter(get_log_formatter())
    logger = logging.getLogger("SimpleReplayLogger")
    logger.info(f"Starting the {script_type}")
    logger.info(f"Logging to {filename}")
    logger.addHandler(fh)
    logger.info("== Initializing logfile ==")

def get_log_formatter(process_idx=None, job_id=None):
    """ Define the log format, with the option to prepend process and job/thread to each message """
    format = "[%(levelname)s] %(asctime)s"
    if process_idx is not None:
        format += f" [{process_idx}]"
    if job_id is not None:
        format += " (%(threadName)s)"
    format += " %(message)s"
    formatter = logging.Formatter(format, datefmt=log_date_format)
    formatter.converter = time.gmtime
    return formatter