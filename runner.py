import logging

import publisher
from config_io import get_configs, default_config_dir
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_console_logger, setup_file_logger, default_log_dir

log = logging.getLogger(__name__)


def run_batch_publishing_job(included_configs=asterisk_tuple, excluded_configs=empty_tuple,
                             included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                             included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                             included_services=asterisk_tuple, excluded_services=empty_tuple,
                             cleanup_services=False,
                             config_dir=default_config_dir,
                             log_to_file=True,
                             log_dir=default_log_dir):
    root_logger = setup_console_logger()

    configs = get_configs(included_configs, excluded_configs, config_dir)

    log.info('Batch publishing configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

    for config_name, config in configs.iteritems():
        log_file_handler = setup_file_logger(root_logger, config_name, log_dir) if log_to_file else None
        try:
            publisher.publish_config(config, config_dir, included_envs, excluded_envs, included_instances,
                                     excluded_instances, included_services, excluded_services, cleanup_services)
        except Exception:
            log.exception('An error occurred while publishing config \'{}\''.format(config_name))
            raise
        finally:
            if log_file_handler:
                root_logger.removeHandler(log_file_handler)


def run_batch_cleanup_job(included_configs=asterisk_tuple, excluded_configs=empty_tuple,
                          included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                          included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                          config_dir=default_config_dir,
                          log_to_file=True,
                          log_dir=default_log_dir):
    root_logger = setup_console_logger()

    configs = get_configs(included_configs, excluded_configs, config_dir)

    log.info('Batch cleaning configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

    for config_name, config in configs.iteritems():
        log_file_handler = setup_file_logger(root_logger, config_name, log_dir) if log_to_file else None
        try:
            publisher.cleanup_config(config, included_envs, excluded_envs, included_instances, excluded_instances)
        except Exception:
            log.exception('An error occurred while cleaning config \'{}\''.format(config_name))
            raise
        finally:
            if log_file_handler:
                root_logger.removeHandler(log_file_handler)


if __name__ == '__main__':
    run_batch_publishing_job()
