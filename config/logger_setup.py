import logging

logger = None


def setup_logger():
    global logger
    if logger:
        return logger

    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.INFO)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a file handler
    file_handler = logging.FileHandler("logfile.log")
    file_handler.setLevel(logging.INFO)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter("[%(asctime)s %(levelname)s] %(message)s")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
