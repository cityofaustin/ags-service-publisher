from __future__ import unicode_literals

import logging
import os

from ags_utils import prompt_for_credentials, generate_token, import_sde_connection_file
from config_io import get_config, get_configs, set_config, default_config_dir
from datasources import list_sde_connection_files_in_folder
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger, setup_console_log_handler, setup_file_log_handler, default_log_dir
from publishing import cleanup_config, publish_config
from reports import find_mxd_data_sources, write_report
from services import analyze_services, find_service_dataset_usages, restart_services, test_services

log = setup_logger(__name__)
root_logger = setup_logger()


class Runner:
    def __init__(
        self,
        verbose=False,
        quiet=False,
        log_to_file=True,
        log_dir=default_log_dir,
        config_dir=default_config_dir
    ):
        self.verbose = verbose
        self.quiet = quiet
        self.log_to_file = log_to_file
        self.log_dir = log_dir
        self.config_dir = config_dir

        if not self.quiet:
            setup_console_log_handler(root_logger, self.verbose)
        if not self.verbose:
            logging.getLogger('requests').setLevel(logging.WARNING)
        if self.log_to_file:
            log.debug('Using log directory: {}'.format(self.log_dir))
        log.debug('Using config directory: {}'.format(self.config_dir))

    def run_batch_publishing_job(
        self,
        included_configs=asterisk_tuple, excluded_configs=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        copy_source_files_from_staging_folder=True,
        cleanup_services=False,
        service_prefix='',
        service_suffix='',
        warn_on_validation_errors=False
    ):
        configs = get_configs(included_configs, excluded_configs, self.config_dir)
        log.info('Batch publishing configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

        for config_name, config in configs.iteritems():
            log_file_handler = setup_file_log_handler(root_logger, config_name, self.log_dir) if self.log_to_file else None
            try:
                publish_config(
                    config,
                    self.config_dir,
                    included_envs, excluded_envs,
                    included_instances, excluded_instances,
                    included_services, excluded_services,
                    copy_source_files_from_staging_folder,
                    cleanup_services,
                    service_prefix,
                    service_suffix,
                    warn_on_validation_errors
                )
            except StandardError:
                log.exception('An error occurred while publishing config \'{}\''.format(config_name))
                log.error('See the log file at {}'.format(log_file_handler.baseFilename))
                raise
            finally:
                if log_file_handler:
                    root_logger.removeHandler(log_file_handler)

    def run_batch_cleanup_job(
        self,
        included_configs=asterisk_tuple, excluded_configs=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    ):
        configs = get_configs(included_configs, excluded_configs, self.config_dir)
        log.info('Batch cleaning configs: {}'.format(', '.join(config_name for config_name in configs.keys())))

        for config_name, config in configs.iteritems():
            log_file_handler = setup_file_log_handler(root_logger, config_name, self.log_dir) if self.log_to_file else None
            try:
                cleanup_config(
                    config,
                    included_envs, excluded_envs,
                    included_instances, excluded_instances,
                    self.config_dir
                )
            except StandardError:
                log.exception('An error occurred while cleaning config \'{}\''.format(config_name))
                log.error('See the log file at {}'.format(log_file_handler.baseFilename))
                raise
            finally:
                if log_file_handler:
                    root_logger.removeHandler(log_file_handler)

    def run_dataset_usages_report(
        self,
        included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
        included_users=asterisk_tuple, excluded_users=empty_tuple,
        included_databases=asterisk_tuple, excluded_databases=empty_tuple,
        included_versions=asterisk_tuple, excluded_versions=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        output_filename=None,
        output_format='csv',
    ):
        header_row = (
            'AGS Instance',
            'Service Folder',
            'Service Name',
            'Service Type',
            'Dataset Name',
            'User',
            'Database',
            'Version',
            'Dataset Path'
        )
        report_data = sorted(
            find_service_dataset_usages(
                included_datasets, excluded_datasets,
                included_users, excluded_users,
                included_databases, excluded_databases,
                included_versions, excluded_versions,
                included_services, excluded_services,
                included_service_folders, excluded_service_folders,
                included_instances, excluded_instances,
                included_envs, excluded_envs,
                self.config_dir
            ),
            key=lambda x: (x[4], x[1], x[2], x[0])
        )
        return write_report(report_data, header_row, 'dataset usages', output_filename, output_format)

    def run_mxd_data_sources_report(
        self,
        included_configs=asterisk_tuple, excluded_configs=empty_tuple,
        included_users=asterisk_tuple, excluded_users=empty_tuple,
        included_databases=asterisk_tuple, excluded_databases=empty_tuple,
        included_versions=asterisk_tuple, excluded_versions=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
        include_staging_mxds=True,
        output_filename=None,
        output_format='csv',
        warn_on_validation_errors=False
    ):
        report_data = find_mxd_data_sources(
            included_configs, excluded_configs,
            included_users, excluded_users,
            included_databases, excluded_databases,
            included_versions, excluded_versions,
            included_services, excluded_services,
            included_envs, excluded_envs,
            included_datasets, excluded_datasets,
            include_staging_mxds,
            warn_on_validation_errors,
            self.config_dir
        )
        header_row = (
            'Config',
            'Environment',
            'Service Name',
            'MXD Path',
            'MXD Type',
            'Layer Name',
            'Dataset Name',
            'User',
            'Database',
            'Version',
            'Workspace Path',
            'Definition Query'
        )
        return write_report(report_data, header_row, output_filename, output_format)

    def generate_tokens(
        self,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        username=None,
        password=None,
        reuse_credentials=False,
        expiration=15
    ):
        user_config = get_config('userconfig', self.config_dir)
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
            set_config(user_config, 'userconfig', self.config_dir)

    def batch_import_sde_connection_files(
        self,
        included_connection_files=asterisk_tuple, excluded_connection_files=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple
    ):
        log.info('Batch importing SDE connection files')

        user_config = get_config('userconfig', self.config_dir)
        env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
        if len(env_names) == 0:
            raise RuntimeError('No environments specified!')
        for env_name in env_names:
            env = user_config['environments'][env_name]
            sde_connections_dir = env['sde_connections_dir']
            sde_connection_files = superfilter(
                [
                    os.path.splitext(os.path.basename(sde_connection_file))[0] for
                    sde_connection_file in
                    list_sde_connection_files_in_folder(sde_connections_dir)
                ],
                included_connection_files, excluded_connection_files
            )
            ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
            log.info('Importing SDE connection files for ArcGIS Server instances: {}'.format(', '.join(ags_instances)))
            for ags_instance in ags_instances:
                ags_instance_props = env['ags_instances'][ags_instance]
                ags_connection = ags_instance_props['ags_connection']
                for sde_connection_file in sde_connection_files:
                    import_sde_connection_file(
                        ags_connection,
                        os.path.join(sde_connections_dir, sde_connection_file + '.sde')
                    )

    def batch_restart_services(
        self,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        include_running_services=True
    ):
        log.info('Batch restarting services')

        restart_services(
            included_services, excluded_services,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_envs, excluded_envs,
            include_running_services,
            self.config_dir
        )

    def batch_test_services(
        self,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        warn_on_errors=False,
    ):
        log.info('Batch testing services')

        list(test_services(
            included_services, excluded_services,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_envs, excluded_envs,
            warn_on_errors,
            self.config_dir
        ))

    def run_service_health_report(
        self,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        output_filename=None,
        output_format='csv',
        warn_on_errors=False
    ):
        header_row = (
            'Environment',
            'Instance',
            'Service Folder',
            'Service Name',
            'Service Type',
            'Configured State',
            'Realtime State',
            'Request URL',
            'Request Method',
            'HTTP Status Code',
            'HTTP Status Reason',
            'Error Message',
            'Response Time'
        )

        report_data = test_services(
            included_services, excluded_services,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_envs, excluded_envs,
            warn_on_errors,
            self.config_dir
        )

        return write_report(report_data, header_row, 'service health', output_filename, output_format)

    def run_service_analysis_report(
        self,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        output_filename=None,
        output_format='csv',
        warn_on_errors=False
    ):
        header_row = (
            'Environment',
            'Instance',
            'Service Folder',
            'Service Name',
            'Service Type',
            'File Path',
            'Severity',
            'Code',
            'Message',
            'Layer',
            'Dataset',
            'Data Source'
        )

        report_data = analyze_services(
            included_envs, excluded_envs,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_services, excluded_services,
            warn_on_errors,
            self.config_dir
        )

        return write_report(report_data, header_row, 'service analysis', output_filename, output_format)
