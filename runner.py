import logging
import os
import datetime

import publisher
from helpers import asterisk_tuple

log = logging.getLogger(__name__)

default_log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'logs'))
default_config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config'))


def run_batch_publishing_job(configs_to_publish=asterisk_tuple, envs_to_publish=asterisk_tuple,
                             instances_to_publish=asterisk_tuple,
                             services_to_publish=asterisk_tuple, config_dir=default_config_dir, log_to_file=True,
                             log_dir=default_log_dir):
    root_logger = setup_console_logger()

    configs = publisher.get_configs(configs_to_publish)

    log.info('Batch publishing configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

    for config_name, config in configs.iteritems():
        log_file_handler = setup_file_logger(root_logger, config_name, log_dir) if log_to_file else None
        try:
            publisher.publish_config(config, config_dir, included_envs=envs_to_publish,
                                     included_instances=instances_to_publish,
                                     included_services=services_to_publish)
        except Exception:
            log.exception('An error occurred while publishing config \'{}\''.format(config_name))
            raise
        finally:
            if log_file_handler:
                root_logger.removeHandler(log_file_handler)


def setup_console_logger():
    log_console_format = '%(message)s'
    log_console_level = 'INFO'
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
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


if __name__ == '__main__':
    run_batch_publishing_job()
