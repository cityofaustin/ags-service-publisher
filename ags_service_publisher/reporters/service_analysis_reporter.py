from __future__ import unicode_literals

import collections

from ..config_io import default_config_dir
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..reporters.base_reporter import BaseReporter
from ..services import analyze_services

log = setup_logger(__name__)


class ServiceAnalysisReporter(BaseReporter):
    report_type = 'service analysis'
    column_mappings = collections.OrderedDict((
        ('env_name', 'Environment'),
        ('ags_instance', 'AGS Instance'),
        ('service_folder', 'Service Folder'),
        ('service_name', 'Service Name'),
        ('service_type', 'Service Type'),
        ('file_path', 'File Path'),
        ('severity', 'Severity'),
        ('code', 'Code'),
        ('message', 'Message'),
        ('layer_name', 'Layer Name'),
        ('dataset_name', 'Dataset Name'),
        ('workspace_path', 'Workspace Path')
    ))

    record_class_name = 'ServiceAnalysisRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        warn_on_errors=False,
        config_dir=default_config_dir
    ):
        return analyze_services(
            included_envs, excluded_envs,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_services, excluded_services,
            warn_on_errors,
            config_dir
        )
