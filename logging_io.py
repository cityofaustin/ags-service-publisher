import datetime
import logging
import os

default_log_dir = os.getenv('AGS_SERVICE_PUBLISHER_LOG_DIR',
                            os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs')))


def setup_logger(namespace=None, level='DEBUG'):
    logger = logging.getLogger(namespace)
    logger.setLevel(logging.getLevelName(level))
    logger.addHandler(logging.NullHandler())
    return logger


def setup_console_log_handler(logger=None, verbose=False):
    log_console_format = '%(levelname)s: %(message)s'
    log_console_level = 'DEBUG' if verbose else 'INFO'
    if not logger:
        logger = setup_logger()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_console_format))
    console_handler.setLevel(logging.getLevelName(log_console_level))
    logger.addHandler(console_handler)
    return console_handler


def setup_file_log_handler(logger=None, config_name=None, log_dir=default_log_dir):
    log_file_format = '%(asctime)s|%(levelname)s|%(processName)s|%(module)s|%(funcName)s|%(message)s'
    log_file_datetime_format = '%Y%m%d-%H%M%S'
    log_file_level = 'DEBUG'
    log_file_name = (config_name + '_' if config_name else '') + datetime.datetime.now().strftime(log_file_datetime_format) + '.log'
    log_file_path = os.path.join(log_dir, log_file_name)
    if not logger:
        logger = setup_logger()
    if not os.path.isdir(log_dir):
        log.debug('Creating log directory: {}'.format(log_dir))
        os.mkdir(log_dir)
    log.debug('Logging to file: {}'.format(log_file_path))
    log_file_handler = logging.FileHandler(log_file_path, mode='w')
    log_file_handler.setFormatter(logging.Formatter(log_file_format))
    log_file_handler.setLevel(logging.getLevelName(log_file_level))
    logger.addHandler(log_file_handler)
    return log_file_handler

log = setup_logger(__name__)
