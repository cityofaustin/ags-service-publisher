import collections
import tempfile
from pathlib import Path
from shutil import rmtree


from ..config_io import default_config_dir, get_configs
from ..datasources import convert_mxd_to_aprx, get_aprx_data_sources
from ..extrafilters import superfilter
from ..helpers import asterisk_tuple, empty_tuple
from ..logging_io import setup_logger
from ..services import get_source_info, normalize_services
from .base_reporter import BaseReporter

log = setup_logger(__name__)


class MapDataSourcesReporter(BaseReporter):
    report_type = 'Map data sources'
    column_mappings = collections.OrderedDict((
        ('config_name', 'Config'),
        ('env_name', 'Environment'),
        ('service_name', 'Service Name'),
        ('file_path', 'File Path'),
        ('file_type', 'File Type'),
        ('source_or_target', 'Source or Target'),
        ('layer_name', 'Layer Name'),
        ('dataset_name', 'Dataset Name'),
        ('is_broken', 'Data Source Is Broken'),
        ('user', 'User'),
        ('database', 'Database'),
        ('version', 'Version'),
        ('definition_query', 'Definition Query')
    ))
    record_class_name = 'MapDataSourcesRecord'
    record_class, header_row = BaseReporter.setup_subclass(column_mappings, record_class_name)

    @staticmethod
    def generate_report_records(
        included_configs=asterisk_tuple, excluded_configs=empty_tuple,
        included_users=asterisk_tuple, excluded_users=empty_tuple,
        included_databases=asterisk_tuple, excluded_databases=empty_tuple,
        included_versions=asterisk_tuple, excluded_versions=empty_tuple,
        included_services=asterisk_tuple, excluded_services=empty_tuple,
        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
        included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
        include_staging_files=True,
        warn_on_validation_errors=False,
        config_dir=default_config_dir
    ):
        for config_name, config in get_configs(included_configs, excluded_configs, config_dir).items():
            env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
            services = superfilter(config['services'], included_services, excluded_services)
            default_service_properties = config.get('default_service_properties')

            if not default_service_properties:
                log.debug('No default service properties specified')

            for env_name in env_names:
                log.debug(f'Finding map data sources for config {config_name}, environment {env_name}')
                env = config['environments'][env_name]
                env_service_properties = env.get('service_properties', {})
                staging_dir = env.get('staging_dir')
                source_dir = env['source_dir']
                source_info, errors = get_source_info(
                    services,
                    source_dir,
                    staging_dir,
                    default_service_properties,
                    env_service_properties
                )
                if len(errors) > 0:
                    message = (
                        f'One or more errors occurred while validating the {env_name} '
                        f'environment for config name {config_name}:\n'
                        f'{chr(10).join(errors)}'
                    )
                    if warn_on_validation_errors:
                        log.warn(message)
                    else:
                        raise RuntimeError(message)
                for (
                    service_name,
                    service_type,
                    service_properties
                ) in normalize_services(
                    services,
                    default_service_properties,
                    env_service_properties
                ):
                    if service_type in ('MapServer', 'ImageServer'):
                        def generate_map_data_sources_report_rows(file_path, source_or_target):
                            file_path = Path(file_path)
                            tempdir = None
                            temp_aprx_path = None
                            if file_path.suffix.lower() == '.mxd':
                                file_type = 'MXD'
                                tempdir = Path(tempfile.mkdtemp())
                                log.debug(f'Temporary directory created: {tempdir}')
                                temp_aprx_path = tempdir / f'{service_name}.aprx'
                                convert_mxd_to_aprx(file_path, temp_aprx_path)
                            elif file_path.suffix.lower() == '.aprx':
                                file_type = 'ArcGIS Pro project file'
                            else:
                                raise RuntimeError(f'Unrecognized file type for {file_path}')
                            try:
                                for layer_properties in get_aprx_data_sources(temp_aprx_path if temp_aprx_path else file_path):
                                    if (
                                        superfilter((layer_properties['dataset_name'],), included_datasets, excluded_datasets) and
                                        superfilter((layer_properties['user'],), included_users, excluded_users) and
                                        superfilter((layer_properties['database'],), included_databases, excluded_databases) and
                                        superfilter((layer_properties['version'],), included_versions, excluded_versions)
                                    ):
                                        yield dict(
                                            config_name=config_name,
                                            env_name=env_name,
                                            service_name=service_name,
                                            file_path=str(file_path),
                                            file_type=file_type,
                                            source_or_target=source_or_target,
                                            **layer_properties
                                        )
                            finally:
                                if tempdir:
                                    log.debug(f'Cleaning up temporary directory: {tempdir}')
                                    rmtree(tempdir, ignore_errors=True)

                        if include_staging_files:
                            for staging_file_path in source_info[service_name]['staging_files']:
                                for row in generate_map_data_sources_report_rows(staging_file_path, 'staging'):
                                    yield row
                        source_file_path = source_info[service_name]['source_file']
                        if source_file_path:
                            for row in generate_map_data_sources_report_rows(source_file_path, 'source'):
                                yield row
                        else:
                            log.warn(
                                f'No source file found for service {config_name}/{service_name} '
                                f'in the {env_name} environment!'
                            )
                    else:
                        log.debug(
                            f'Unsupported service type {service_type} of service {service_name} will be skipped'
                        )
