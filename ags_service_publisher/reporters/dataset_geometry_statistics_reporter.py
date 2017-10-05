from __future__ import unicode_literals

import collections

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
        for (
            env_name,
            ags_instance,
            service_folder,
            service_name,
            service_type,
            dataset_name,
            dataset_type,
            user,
            database,
            version,
            dataset_path
        ) in (
            find_service_dataset_usages(*args, **kwargs)
        ):
            error = None
            if (dataset_name, database, version) in cache:
                log.debug('Geometry statistics for dataset {} found in cache'.format(dataset_name))
                shape_type, feature_count, avg_part_count, avg_vertex_count = cache[(dataset_name, database, version)]
            else:
                shape_type = feature_count = avg_part_count = avg_vertex_count = None
                try:
                    shape_type, feature_count, avg_part_count, avg_vertex_count = get_geometry_statistics(dataset_path)
                    cache[(dataset_name, database, version)] = (shape_type, feature_count, avg_part_count, avg_vertex_count)
                except StandardError as e:
                    log.exception(
                        'An error occurred while getting the statistics for dataset: {}, AGS instance: {}, service: '
                        '{}/{} ({}), dataset path: {}'
                        .format(dataset_name, ags_instance, service_folder, service_name, service_type, dataset_path)
                    )
                    error = e.message
            yield (
                env_name,
                ags_instance,
                service_folder,
                service_name,
                service_type,
                dataset_name,
                dataset_type,
                user,
                database,
                version,
                dataset_path,
                error,
                shape_type,
                feature_count,
                avg_part_count,
                avg_vertex_count
            )
