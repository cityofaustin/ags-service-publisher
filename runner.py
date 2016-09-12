import csv
import logging
import os

import publisher
from ags_utils import prompt_for_credentials, generate_token, import_sde_connection_file
from config_io import get_config, get_configs, set_config, default_config_dir
from datasources import list_sde_connection_files_in_folder
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple, file_or_stdout
from logging_io import setup_logger, setup_console_log_handler, setup_file_log_handler, default_log_dir

log = setup_logger(__name__)
root_logger = setup_logger()

def run_batch_publishing_job(included_configs=asterisk_tuple, excluded_configs=empty_tuple,
                             included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                             included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                             included_services=asterisk_tuple, excluded_services=empty_tuple,
                             cleanup_services=False,
                             service_prefix='',
                             service_suffix='',
                             config_dir=default_config_dir,
                             log_to_file=True,
                             log_dir=default_log_dir,
                             verbose=False,
                             quiet=False):
    if not quiet:
        setup_console_log_handler(root_logger, verbose)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARNING)

    log.debug('Using config directory: {}'.format(config_dir))
    if log_to_file:
        log.debug('Using log directory: {}'.format(log_dir))

    configs = get_configs(included_configs, excluded_configs, config_dir)

    log.info('Batch publishing configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

    for config_name, config in configs.iteritems():
        log_file_handler = setup_file_log_handler(root_logger, config_name, log_dir) if log_to_file else None
        try:
            publisher.publish_config(config, config_dir, included_envs, excluded_envs, included_instances,
                                     excluded_instances, included_services, excluded_services, cleanup_services,
                                     service_prefix, service_suffix)
        except:
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
                          log_dir=default_log_dir,
                          verbose=False,
                          quiet=False):
    if not quiet:
        setup_console_log_handler(root_logger, verbose)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARNING)

    log.debug('Using config directory: {}'.format(config_dir))
    if log_to_file:
        log.debug('Using log directory: {}'.format(log_dir))

    configs = get_configs(included_configs, excluded_configs, config_dir)

    log.info('Batch cleaning configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

    for config_name, config in configs.iteritems():
        log_file_handler = setup_file_log_handler(root_logger, config_name, log_dir) if log_to_file else None
        try:
            publisher.cleanup_config(config,
                                     included_envs, excluded_envs,
                                     included_instances, excluded_instances,
                                     config_dir)
        except:
            log.exception('An error occurred while cleaning config \'{}\''.format(config_name))
            raise
        finally:
            if log_file_handler:
                root_logger.removeHandler(log_file_handler)


def run_dataset_usages_report(included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
                              included_services=asterisk_tuple, excluded_services=empty_tuple,
                              included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
                              included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                              included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                              output_filename=None,
                              output_format='csv',
                              verbose=False,
                              quiet=False,
                              config_dir=default_config_dir):
    if not quiet:
        setup_console_log_handler(root_logger, verbose)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARNING)

    log.info('Generating dataset usages report')
    log.debug('Using config directory: {}'.format(config_dir))

    def get_report_data():
        found_datasets = publisher.find_dataset_usages(included_datasets, excluded_datasets,
                                                       included_services, excluded_services,
                                                       included_service_folders, excluded_service_folders,
                                                       included_instances, excluded_instances,
                                                       included_envs, excluded_envs,
                                                       config_dir)

        return sorted(found_datasets, key=lambda x: (x[4], x[1], x[2], x[0]))

    if output_format == 'csv':
        report_data = get_report_data()

        with file_or_stdout(output_filename, 'wb') as csvfile:
            header_row = ('AGS Instance', 'Service Folder', 'Service Name', 'Service Type', 'Dataset Name',
                          'Dataset Path')
            rows = report_data
            csvwriter = csv.writer(csvfile, lineterminator='\n')
            csvwriter.writerow(header_row)
            csvwriter.writerows(rows)
    else:
        raise RuntimeError('Unsupported output format: {}'.format(output_format))
    log.info('Successfully generated dataset usages report{}'
             .format(': {}'.format(os.path.abspath(output_filename))
                     if output_filename and os.path.isfile(output_filename) else ''))


def generate_tokens(included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                    username=None,
                    password=None,
                    reuse_credentials=False,
                    expiration=15,
                    verbose=False,
                    quiet=False,
                    config_dir=default_config_dir):
    if not quiet:
        setup_console_log_handler(root_logger, verbose)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARNING)
    log.debug('Using config directory: {}'.format(config_dir))

    user_config = get_config('userconfig')
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    if reuse_credentials:
        username, password = prompt_for_credentials(username, password)
    needs_save = False
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)

        log.info('Refreshing tokens for ArcGIS Server instances: {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            new_token = generate_token(ags_instance_props['url'], username, password, expiration, ags_instance)
            if new_token:
                ags_instance_props['token'] = new_token
                if not needs_save:
                    needs_save = True
    if needs_save:
        set_config(user_config, 'userconfig', config_dir)


def batch_import_sde_connection_files(included_connection_files=asterisk_tuple, excluded_connection_files=empty_tuple,
                                      included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                                      included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                                      verbose=False,
                                      quiet=False,
                                      config_dir=default_config_dir):
    if not quiet:
        setup_console_log_handler(root_logger, verbose)
    if not verbose:
        logging.getLogger('requests').setLevel(logging.WARNING)
    log.debug('Using config directory: {}'.format(config_dir))

    user_config = get_config('userconfig')
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        sde_connections_dir = env['sde_connections_dir']
        sde_connection_files = superfilter(
            [os.path.splitext(os.path.basename(sde_connection_file))[0] for sde_connection_file in
             list_sde_connection_files_in_folder(sde_connections_dir)],
            included_connection_files, excluded_connection_files)
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Importing SDE connection files for ArcGIS Server instances: {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            ags_connection = ags_instance_props['ags_connection']
            for sde_connection_file in sde_connection_files:
                import_sde_connection_file(ags_connection,
                                           os.path.join(sde_connections_dir, sde_connection_file + '.sde'))
