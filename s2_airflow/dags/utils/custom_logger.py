import logging
import os
import datetime

def init_logger(__name__):

    # Init logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    add_stream_handler(logger)
    add_file_handler(logger)

    return logger

def add_stream_handler(logger):

    # Set up to stream logging info to the console
    stream_handler = logging.StreamHandler()
    stream_format = logging.Formatter("%(name)s:%(levelname)s:%(message)s")
    stream_handler.setFormatter(stream_format)
    logger.addHandler(stream_handler)

def add_file_handler(logger):

    # set up to log to file
    log_path = "./logs"
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%s")
    log_filename = f"{now}.log"

    file_handler = logging.FileHandler(filename=os.path.join(log_path, log_filename))
    file_format = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
