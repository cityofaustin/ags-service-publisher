from __future__ import unicode_literals

import collections
import itertools

from ..config_io import default_config_dir
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..reporters.base_reporter import BaseReporter
from ..services import generate_service_inventory

log = setup_logger(__name__)


class ServiceComparisonReporter(BaseReporter):
    report_type = 'service comparison'
    column_mappings = collections.OrderedDict((
        ('env_name', 'Environment (Found)'),
        ('ags_instance', 'AGS Instance (Found)'),
        ('env_name_missing', 'Environment (Missing)'),
        ('ags_instance_missing', 'AGS Instance (Missing)'),
        ('service_folder', 'Service Folder'),
        ('service_name', 'Service Name'),
        ('service_type', 'Service Type'),
        ('message', 'Message'),
    ))
    record_class_name = 'ServiceComparisonRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        case_insensitive=False,
        config_dir=default_config_dir
    ):
        records = generate_service_inventory(
            included_services, excluded_services,
            included_service_folders, excluded_service_folders,
            included_instances, excluded_instances,
            included_envs, excluded_envs,
            config_dir
        )

        group_keys = (
            'env_name',
            'ags_instance',
        )

        comparison_keys = (
            'service_folder',
            'service_name',
            'service_type',
        )

        unique_keys = []
        groups = []

        def comparator(x, keys):
            return tuple((x[k].lower() if case_insensitive else x[k] for k in keys))

        def matcher(base_group, test_group, test_group_keys):
            for base_record in base_group:
                match_found = False
                for test_record in test_group:
                    if comparator(base_record, comparison_keys) == comparator(test_record, comparison_keys):
                        match_found = True
                        break
                if not match_found:
                    result = dict(
                        env_name_missing=test_group_keys[0],
                        ags_instance_missing=test_group_keys[1],
                        **base_record
                    )
                    message = '{service_type} service {service_folder}/{service_name} found on AGS instance ' \
                              '{ags_instance}, environment {env_name} is missing from AGS instance ' \
                              '{ags_instance_missing}, environment {env_name_missing}'.format(**result)
                    log.warn(message)
                    result['message'] = message
                    yield result

        for key, group in itertools.groupby(
            records,
            key=lambda x: comparator(x, group_keys)
        ):
            unique_keys.append(key)
            groups.append(list(group))

        if len(groups) > 2:
            log.warn('More than two groups were found in the comparison, omitting all but first two.')
            del groups[2:]

        if len(groups) < 2:
            log.warn('Less than two groups were found in the comparison, aborting.')
            return tuple()

        group_1 = groups[0]
        group_2 = groups[1]

        return itertools.chain(
            matcher(group_1, group_2, unique_keys[1]),
            matcher(group_2, group_1, unique_keys[0])
        )
