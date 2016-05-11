import datetime
import logging
import os

default_log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))


def setup_console_logger():
    log_console_format = '%(message)s'
    log_console_level = 'INFO'
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_console_format))
    console_handler.setLevel(logging.getLevelName(log_console_level))
    root_logger.addHandler(console_handler)
    return root_logger


def setup_file_logger(root_logger, config_name, log_dir=default_log_dir):
    log_file_format = '%(asctime)s|%(levelname)s|%(module)s|%(message)s'
    log_file_datetime_format = '%Y%m%d-%H%M%S'
    log_file_level = 'DEBUG'
    log_file_name = config_name + '_' + datetime.datetime.now().strftime(log_file_datetime_format) + '.log'
    log_file_path = os.path.join(log_dir, log_file_name)
    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)
    log_file_handler = logging.FileHandler(log_file_path, mode='w')
    log_file_handler.setFormatter(logging.Formatter(log_file_format))
    log_file_handler.setLevel(logging.getLevelName(log_file_level))
    root_logger.addHandler(log_file_handler)
    return log_file_handler
