from __future__ import unicode_literals

import collections
from itertools import chain

from ..datasources import get_geometry_statistics
from ..logging_io import setup_logger
from ..reporters.base_reporter import BaseReporter
from ..services import find_service_dataset_usages

log = setup_logger(__name__)


class DatasetGeometryStatisticsReporter(BaseReporter):
    report_type = 'dataset geometry statistics'
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
        ('error', 'Error'),
        ('shape_type', 'Shape Type'),
        ('feature_count', 'Feature Count'),
        ('avg_part_count', 'Average Part Count'),
        ('avg_vertex_count', 'Average Vertex Count')
    ))
    record_class_name = 'DatasetGeometryStatisticsRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(*args, **kwargs):
        cache = {}
        for dataset_props in find_service_dataset_usages(*args, **kwargs):
            error = None
            key = tuple(dataset_props[field] for field in ('dataset_name', 'database', 'version'))
            if key in cache:
                log.debug('Geometry statistics for dataset {} found in cache'.format(dataset_props['dataset_name']))
                geometry_stats = cache[key]
            else:
                try:
                    geometry_stats = get_geometry_statistics(dataset_props['dataset_path'])
                    cache[key] = geometry_stats
                except StandardError as e:
                    log.exception(
                        'An error occurred while getting the statistics for dataset: {dataset_name}, '
                        'AGS instance: {ags_instance}, '
                        'service: {service_folder}/{service_name} ({service_type}), '
                        'dataset path: {dataset_path}'
                        .format(**dataset_props)
                    )
                    error = e.message
            yield dict(chain(
                dataset_props.iteritems(),
                geometry_stats.iteritems()
            ),
                error=error
            )
