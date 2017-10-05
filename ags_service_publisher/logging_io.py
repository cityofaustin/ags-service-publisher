from __future__ import unicode_literals

import datetime
import logging
import os

default_log_dir = os.getenv(
    'AGS_SERVICE_PUBLISHER_LOG_DIR',
    os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs'))
)


def setup_logger(namespace=None, level='DEBUG', handler=None):
    logger = logging.getLogger(namespace)
    logger.setLevel(level)
    logger.addHandler(logging.NullHandler() if handler is None else handler)
    return logger


def setup_console_log_handler(logger=None, verbose=False):
    log_console_format = '%(levelname)s: %(message)s'
    log_console_level = 'DEBUG' if verbose else 'INFO'
    if not logger:
        logger = setup_logger()
    console_handler = None

    has_console_handler = False
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            console_handler = handler
            console_handler.setLevel(log_console_level)
            has_console_handler = True
            break
    if not has_console_handler:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_console_format))
        console_handler.setLevel(log_console_level)
        logger.addHandler(console_handler)
    return console_handler


def setup_file_log_handler(logger=None, base_filename=None, log_dir=default_log_dir):
    log_file_format = '%(asctime)s|%(levelname)s|%(processName)s|%(module)s|%(funcName)s|%(message)s'
    log_file_datetime_format = '%Y%m%d-%H%M%S'
    log_file_level = 'DEBUG'
    log_file_name = ''.join(
        (
            base_filename + '_' if base_filename else '',
            datetime.datetime.now().strftime(log_file_datetime_format),
            '.log'
        )
    )
    log_file_path = os.path.join(log_dir, log_file_name)
    if not logger:
        logger = setup_logger()
    if not os.path.isdir(log_dir):
        log.debug('Creating log directory: {}'.format(log_dir))
        os.mkdir(log_dir)
    log.debug('Logging to file: {}'.format(log_file_path))
    log_file_handler = logging.FileHandler(log_file_path, mode='w')
    log_file_handler.setFormatter(logging.Formatter(log_file_format))
    log_file_handler.setLevel(log_file_level)
    logger.addHandler(log_file_handler)
    return log_file_handler


log = setup_logger(__name__)
