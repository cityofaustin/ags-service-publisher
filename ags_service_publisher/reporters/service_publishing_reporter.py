from __future__ import unicode_literals

import collections

from ..config_io import default_config_dir, get_configs
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..publishing import publish_config_name
from ..reporters.base_reporter import BaseReporter

log = setup_logger(__name__)


class ServicePublishingReporter(BaseReporter):
    report_type = 'Service publishing'
    column_mappings = collections.OrderedDict((
        ('env_name', 'Environment'),
        ('ags_instance', 'AGS Instance'),
        ('config_name', 'Config'),
        ('service_folder', 'Service Folder'),
        ('service_name', 'Service Name'),
        ('service_type', 'Service Type'),
        ('file_path', 'File Path'),
        ('succeeded', 'Succeeded'),
        ('error', 'Error'),
        ('timestamp', 'Timestamp')
    ))
    record_class_name = 'ServicePublishingRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_configs=asterisk_tuple, excluded_configs=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        copy_source_files_from_staging_folder=True,
        cleanup_services=False,
        service_prefix='',
        service_suffix='',
        warn_on_validation_errors=False,
        warn_on_publishing_errors=False,
        config_dir=default_config_dir,
        create_backups=True
    ):
        for config_name, config in get_configs(included_configs, excluded_configs, config_dir).iteritems():
            for result in publish_config_name(
                config_name,
                config_dir,
                included_envs, excluded_envs,
                included_instances, excluded_instances,
                included_services, excluded_services,
                copy_source_files_from_staging_folder,
                cleanup_services,
                service_prefix,
                service_suffix,
                warn_on_publishing_errors,
                warn_on_validation_errors,
                create_backups
            ):
                yield result
