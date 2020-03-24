from __future__ import unicode_literals

import collections

from ..config_io import default_config_dir
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..services import generate_data_stores_inventory
from ..reporters.base_reporter import BaseReporter

log = setup_logger(__name__)


class DataStoresReporter(BaseReporter):
    report_type = 'data stores'
    column_mappings = collections.OrderedDict((
        ('env_name', 'Environment'),
        ('ags_instance', 'AGS Instance'),
        ('item_path', 'Item Path'),
        ('item_type', 'Item Type'),
        ('file_path', 'File Path'),
        ('user', 'User'),
        ('database', 'Database'),
        ('version', 'Version'),
    ))
    record_class_name = 'DataStoresRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        config_dir=default_config_dir
    ):
        return generate_data_stores_inventory(
            included_instances, excluded_instances,
            included_envs, excluded_envs,
            config_dir
        )
