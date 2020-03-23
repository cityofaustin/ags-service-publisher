from __future__ import unicode_literals

import collections

from ..config_io import default_config_dir
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..reporters.base_reporter import BaseReporter
from ..services import find_service_dataset_usages

log = setup_logger(__name__)


class DatasetUsagesReporter(BaseReporter):
    report_type = 'dataset usages'
    column_mappings = collections.OrderedDict((
        ('env_name', 'Environment'),
        ('ags_instance', 'AGS Instance'),
        ('service_folder', 'Service Folder'),
        ('service_name', 'Service Name'),
        ('service_type', 'Service Type'),
        ('dataset_name', 'Dataset Name'),
        ('dataset_type', 'Dataset Type'),
        ('user', 'User'),
        ('database', 'Database'),
        ('version', 'Version'),
        ('dataset_path', 'Dataset Path'),
        ('by_reference', 'By Reference'),
    ))
    record_class_name = 'DatasetUsagesRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
        included_users=asterisk_tuple, excluded_users=empty_tuple,
        included_databases=asterisk_tuple, excluded_databases=empty_tuple,
        included_versions=asterisk_tuple, excluded_versions=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        config_dir=default_config_dir
    ):
        return sorted(
            find_service_dataset_usages(
                included_datasets, excluded_datasets,
                included_users, excluded_users,
                included_databases, excluded_databases,
                included_versions, excluded_versions,
                included_services, excluded_services,
                included_service_folders, excluded_service_folders,
                included_instances, excluded_instances,
                included_envs, excluded_envs,
                config_dir
            ),
            key=lambda record: tuple(
                record[field].lower() for field in (
                    'dataset_name',
                    'ags_instance',
                    'service_folder',
                    'env_name'
                )
            )
        )
