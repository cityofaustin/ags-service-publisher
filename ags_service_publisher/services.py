import collections
import datetime
import os
import tempfile
from copy import deepcopy
from itertools import chain
from pathlib import Path
from shutil import rmtree

from .ags_utils import (
    create_session,
    get_service_manifest,
    get_service_status,
    list_service_folders,
    list_service_workspaces,
    list_services,
    restart_service,
    test_service
)
from .config_io import get_config, default_config_dir
from .datasources import (
    convert_mxd_to_aprx,
    open_aprx,
    list_layers_in_map,
    get_layer_fields,
    get_layer_properties,
)
from .extrafilters import superfilter
from .helpers import asterisk_tuple, empty_tuple
from .logging_io import setup_logger

log = setup_logger(__name__)


def generate_service_inventory(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    config_dir=default_config_dir
):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Listing services on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        if superfilter((service_name,), included_services, excluded_services):
                            yield dict(
                                env_name=env_name,
                                ags_instance=ags_instance,
                                service_folder=service_folder,
                                service_name=service_name,
                                service_type=service_type
                            )


def analyze_services(
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    warn_on_errors=True,
    config_dir=default_config_dir
):
    import arcpy
    arcpy.env.overwriteOutput = True
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)

    for env_name in env_names:
        log.debug('Analyzing services for environment {}'.format(env_name))
        env = user_config['environments'][env_name]
        for ags_instance in superfilter(env['ags_instances'], included_instances, excluded_instances):
            ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
            ags_connection = ags_instance_props['ags_connection']
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        if (
                            service_type in ('MapServer', 'GeocodeServer') and
                            superfilter((service_name,), included_services, excluded_services)
                        ):
                            service_props = dict(
                                env_name=env_name,
                                ags_instance=ags_instance,
                                service_folder=service_folder,
                                service_name=service_name,
                                service_type=service_type
                            )
                            try:
                                service_manifest = get_service_manifest(server_url, token, service_name, service_folder, service_type, session=session)
                                service_props['file_path'] = file_path = service_manifest['resources'][0]['onPremisePath']
                                file_path = Path(file_path)
                                file_type = {
                                    'MapServer': 'ArcGIS Pro project file' if file_path.suffix.lower() == '.aprx' else 'MXD',
                                    'GeocodeServer': 'Locator'
                                }[service_type]
                                log.info(
                                    f'Analyzing {service_type} service {service_folder}/{service_name} '
                                    f'on ArcGIS Server instance {ags_instance} (Connection File: {ags_connection}, '
                                    f'{file_type} Path: {file_path})'
                                )
                                if not arcpy.Exists(file_path):
                                    raise RuntimeError(f'{file_type} {file_path} does not exist!')
                                try:
                                    tempdir = Path(tempfile.mkdtemp())
                                    log.debug(f'Temporary directory created: {tempdir}')
                                    sddraft = tempdir / f'{service_name}.sddraft'
                                    sd = tempdir / f'{service_name}.sd'
                                    log.debug(f'Creating SDDraft file: {sddraft}')

                                    if service_type == 'MapServer':
                                        if file_path.suffix.lower() == '.aprx':
                                            aprx = open_aprx(file_path)
                                        elif file_path.suffix.lower() == '.mxd':
                                            temp_aprx_path = tempdir / f'{service_name}.aprx'
                                            convert_mxd_to_aprx(file_path, temp_aprx_path)
                                            aprx = open_aprx(temp_aprx_path)
                                        else:
                                            raise RuntimeError(f'Unrecognized file type for {file_path}')

                                        map_ = aprx.listMaps()[0]
                                        map_service_draft = arcpy.sharing.CreateSharingDraft(
                                            server_type='STANDALONE_SERVER',
                                            service_type='MAP_SERVICE',
                                            service_name=service_name,
                                            draft_value=map_
                                        )
                                        map_service_draft.targetServer = ags_connection
                                        map_service_draft.exportToSDDraft(str(sddraft))
                                        log.debug(f'Staging SDDraft file: {sddraft} to SD file: {sd}')
                                        result = arcpy.StageService_server(str(sddraft), str(sd))
                                        analysis = {}
                                        for key, severity in (('messages', 0), ('warnings', 1), ('errors', 2)):
                                            analysis[key] = {}
                                            for i in range(result.messageCount):
                                                if result.getSeverity(i) == severity:
                                                    analysis[key][(result.getMessage(i), 0)] = None
                                    elif service_type == 'GeocodeServer':
                                        locator_path = file_path
                                        analysis = arcpy.CreateGeocodeSDDraft(
                                            str(locator_path),
                                            str(sddraft),
                                            service_name,
                                            'FROM_CONNECTION_FILE',
                                            ags_connection,
                                            False,
                                            service_folder
                                        )
                                    else:
                                        raise RuntimeError('Unsupported service type {}!'.format(service_type))

                                    for key, log_method in (('messages', log.info), ('warnings', log.warn), ('errors', log.error)):
                                        items = analysis[key]
                                        severity = key[:-1].title()
                                        if items:
                                            log.info('----' + key.upper() + '---')
                                            for ((message, code), layerlist) in items.items():
                                                code = '{:05d}'.format(code)
                                                log_method('    {} (CODE {})'.format(message, code))
                                                code = '="{}"'.format(code)
                                                issue_props = dict(
                                                    severity=severity,
                                                    code=code,
                                                    message=message
                                                )
                                                if not layerlist:
                                                    yield dict(chain(
                                                        service_props.items(),
                                                        issue_props.items()
                                                    ))
                                                else:
                                                    log_method('       applies to:')
                                                    for layer in layerlist:
                                                        layer_name = getattr(layer, 'longName', layer.name)
                                                        layer_props = dict(
                                                            dataset_name=layer.datasetName,
                                                            workspace_path=layer.workspacePath,
                                                            layer_name=layer_name
                                                        )
                                                        log_method('           {}'.format(layer_name))
                                                        yield dict(chain(
                                                            service_props.items(),
                                                            issue_props.items(),
                                                            layer_props.items()
                                                        ))

                                    if analysis['errors']:
                                        error_message = 'Analysis failed for service {}/{} at {:%#m/%#d/%y %#I:%M:%S %p}' \
                                            .format(service_folder, service_name, datetime.datetime.now())
                                        log.error(error_message)
                                        raise RuntimeError(error_message, analysis['errors'])
                                finally:
                                    log.debug(f'Cleaning up temporary directory: {tempdir}')
                                    rmtree(tempdir, ignore_errors=True)
                            except Exception as e:
                                log.exception(
                                    'An error occurred while analyzing {} service {}/{} on ArcGIS Server instance {}'
                                    .format(service_type, service_folder, service_name, ags_instance)
                                )
                                if not warn_on_errors:
                                    raise
                                else:
                                    yield dict(
                                        severity='Error',
                                        message=str(e),
                                        **service_props
                                    )


def list_service_layer_fields(
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    warn_on_errors=False,
    config_dir=default_config_dir
):
    import arcpy
    arcpy.env.overwriteOutput = True
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)

    for env_name in env_names:
        log.debug('Listing service layers and fields for environment {}'.format(env_name))
        env = user_config['environments'][env_name]
        for ags_instance in superfilter(env['ags_instances'], included_instances, excluded_instances):
            ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
            ags_connection = ags_instance_props['ags_connection']
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        if (
                            service_type == 'MapServer' and
                            superfilter((service_name,), included_services, excluded_services)
                        ):
                            service_props = dict(
                                env_name=env_name,
                                ags_instance=ags_instance,
                                service_folder=service_folder,
                                service_name=service_name,
                                service_type=service_type,
                                ags_connection=ags_connection
                            )
                            try:
                                service_manifest = get_service_manifest(server_url, token, service_name, service_folder, service_type, session=session)
                                service_props['file_path'] = file_path = service_manifest['resources'][0]['onPremisePath']
                                file_path = Path(file_path)
                                log.info(
                                    f'Listing layers and fields for {service_type} service {service_folder}/{service_name} '
                                    f'on ArcGIS Server instance {ags_instance} '
                                    f'(Connection File: {ags_connection}, File Path: {file_path})'
                                )
                                file_type = 'ArcGIS Pro project file' if file_path.suffix.lower() == '.aprx' else 'MXD'
                                if not arcpy.Exists(file_path):
                                    raise RuntimeError(f'{file_type} {file_path} does not exist!')
                                try:
                                    if file_path.suffix.lower() == '.aprx':
                                        aprx = open_aprx(file_path)
                                    elif file_path.suffix.lower() == '.mxd':
                                        tempdir = Path(tempfile.mkdtemp())
                                        log.debug(f'Temporary directory created: {tempdir}')
                                        temp_aprx_path = tempdir / f'{service_name}.aprx'
                                        convert_mxd_to_aprx(file_path, temp_aprx_path)
                                        aprx = open_aprx(temp_aprx_path)
                                    else:
                                        raise RuntimeError(f'Unrecognized file type for {file_path}')

                                    for layer in list_layers_in_map(aprx.listMaps()[0]):
                                        if not (
                                            getattr(layer, 'isGroupLayer', False) or
                                            getattr(layer, 'isRasterLayer', False)
                                        ):
                                            layer_name = getattr(layer, 'longName', layer.name)
                                            try:
                                                layer_props = get_layer_properties(layer)
                                            except Exception as e:
                                                log.exception(
                                                    f'An error occurred while retrieving properties for layer {layer_name} in {file_type} {file_path}'
                                                )
                                                if not warn_on_errors:
                                                    raise
                                                else:
                                                    yield dict(
                                                        error=f'Error retrieving layer properties: {e}',
                                                        layer_name=layer_name,
                                                        **service_props
                                                    )
                                                    continue
                                            try:
                                                if layer_props['is_broken']:
                                                    raise RuntimeError(
                                                        'Layer\'s data source is broken (Layer: {}, Data Source: {})'.format(
                                                            layer_name,
                                                            getattr(layer, 'dataSource', 'n/a')
                                                        )
                                                    )
                                                for field_props in get_layer_fields(layer):
                                                    field_props['needs_index'] = not field_props['has_index'] and (
                                                        field_props['in_definition_query'] or
                                                        field_props['in_label_class_expression'] or
                                                        field_props['in_label_class_sql_query'] or
                                                        field_props['field_name'] in layer_props['symbology_fields'] or
                                                        field_props['field_type'] == 'Geometry'
                                                    )

                                                    yield dict(chain(
                                                        service_props.items(),
                                                        layer_props.items(),
                                                        field_props.items()
                                                    ))
                                            except Exception as e:
                                                log.exception(
                                                    f'An error occurred while listing fields for layer {layer_name} in {file_type} {file_path}'
                                                )
                                                if not warn_on_errors:
                                                    raise
                                                else:
                                                    yield dict(chain(
                                                        service_props.items(),
                                                        layer_props.items()
                                                    ),
                                                        error=f'Error retrieving layer fields: {e}'
                                                    )
                                finally:
                                    if tempdir:
                                        log.debug(f'Cleaning up temporary directory: {tempdir}')
                                        rmtree(tempdir, ignore_errors=True)
                            except Exception as e:
                                log.exception(
                                    f'An error occurred while listing layers and fields for '
                                    f'{service_type} service {service_folder}/{service_name} on '
                                    f'ArcGIS Server instance {ags_instance} (Connection File: {ags_connection})'
                                )
                                if not warn_on_errors:
                                    raise
                                else:
                                    yield dict(
                                        error=str(e),
                                        **service_props
                                    )


def find_service_dataset_usages(
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
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Finding service dataset usages on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        service_props = dict(
                            env_name=env_name,
                            ags_instance=ags_instance,
                            service_folder=service_folder,
                            service_name=service_name,
                            service_type=service_type
                        )
                        if superfilter((service_name,), included_services, excluded_services):
                            for dataset_props in list_service_workspaces(
                                server_url,
                                token,
                                service_name,
                                service_folder,
                                service_type,
                                session=session
                            ):
                                if (
                                    superfilter((dataset_props['dataset_name'],), included_datasets, excluded_datasets) and
                                    superfilter((dataset_props['user'],), included_users, excluded_users) and
                                    superfilter((dataset_props['database'],), included_databases, excluded_databases) and
                                    superfilter((dataset_props['version'],), included_versions, excluded_versions)
                                ):
                                    yield dict(chain(
                                        service_props.items(),
                                        dataset_props.items()
                                    ))


def restart_services(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    include_running_services=True,
    delay=30,
    max_retries=3,
    test_after_restart=True,
    config_dir=default_config_dir
):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Restarting services on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        if superfilter((service_name,), included_services, excluded_services):
                            if not include_running_services:
                                status = get_service_status(server_url, token, service_name, service_folder, service_type, session=session)
                                configured_state = status.get('configuredState')
                                if configured_state == 'STARTED':
                                    log.debug(
                                        'Skipping restart of service {}/{} ({}) because its configured state is {} and include_running_services is {}'
                                        .format(service_folder, service_name, service_type, configured_state, include_running_services)
                                    )
                                    continue
                                restart_service(server_url, token, service_name, service_folder, service_type, delay, max_retries, test_after_restart, session=session)
                            restart_service(server_url, token, service_name, service_folder, service_type, delay, max_retries, test_after_restart, session=session)


def test_services(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    warn_on_errors=False,
    config_dir=default_config_dir
):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Testing services on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                service_folders = list_service_folders(server_url, token, session=session)
                for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                    for service in list_services(server_url, token, service_folder, session=session):
                        service_name = service['serviceName']
                        service_type = service['type']
                        if superfilter((service_name,), included_services, excluded_services):
                            test_data = test_service(server_url, token, service_name, service_folder, service_type, warn_on_errors, session=session)
                            yield dict(
                                env_name=env_name,
                                ags_instance=ags_instance,
                                service_folder=service_folder,
                                service_name=service_name,
                                service_type=service_type,
                                **test_data
                            )


def normalize_services(services, default_service_properties=None, env_service_properties=None):
    for service in services:
        yield normalize_service(service, default_service_properties, env_service_properties)


def normalize_service(service, default_service_properties=None, env_service_properties=None):
    is_mapping = isinstance(service, collections.abc.Mapping)
    service_name = next(iter(service.keys())) if is_mapping else service
    merged_service_properties = deepcopy(default_service_properties) if default_service_properties else {}
    if env_service_properties:
        log.debug(
            'Overriding default service properties with environment-level properties for service {}'
            .format(service_name)
        )
        merged_service_properties.update(env_service_properties)
    if is_mapping:
        service_properties = service.get(service_name)
        if service_properties:
            log.debug('Overriding default service properties with service-level properties for service {}'.format(service_name))
            merged_service_properties.update(service_properties)
        else:
            log.warn(
                'No service-level properties specified for service {} even though it was specified as a mapping'
                .format(service_name)
            )
    else:
        log.debug('No service-level properties specified for service {}'.format(service_name))
    service_type = merged_service_properties.get('service_type', 'MapServer')
    return service_name, service_type, merged_service_properties


def get_source_info(services, source_dir, staging_dir, default_service_properties, env_service_properties):
    log.debug(
        f'Getting source info for services {services}, source directory: {source_dir}, staging directory {staging_dir}'
    )

    source_info = {}
    errors = []

    for (
        service_name,
        service_type,
        service_properties
    ) in normalize_services(
        services,
        default_service_properties,
        env_service_properties
    ):
        service_info = source_info[service_name] = {
            'source_file': None,
            'staging_files': []
        }
        if staging_dir:
            staging_files = service_info['staging_files']
            # If multiple staging folders are provided, look for the source item in each staging folder
            staging_dirs = (staging_dir,) if isinstance(staging_dir, str) else staging_dir
            for _staging_dir in staging_dirs:
                log.debug(f'Finding staging items in directory: {_staging_dir}')
                _staging_dir = Path(_staging_dir)
                if service_type == 'MapServer':
                    # First look for APRX file
                    staging_file = _staging_dir / f'{service_name}.aprx'
                    if not staging_file.is_file():
                        # Fall back to MXD file
                        staging_file = _staging_dir / f'{service_name}.mxd'
                elif service_type == 'GeocodeServer':
                    staging_file = _staging_dir / f'{service_name}.loc'
                else:
                    log.debug(f'Unsupported service type {service_type} of service {service_name} will be skipped')
                    continue

                if staging_file.is_file():
                    log.debug(f'Staging file found: {staging_file}')
                    staging_files.append(str(staging_file))
                else:
                    log.debug(f'Staging file missing: {staging_file}')

            if len(staging_files) == 0:
                errors.append(f'- No staging file found for service {service_name}')
            elif len(staging_files) > 1:
                errors.append(
                    '- More than one staging file found for service {}: \n{}'
                    .format(
                        service_name,
                        '\n'.join('  - {}'.format(staging_file) for staging_file in staging_files)
                    )
                )

        if source_dir:
            log.debug(f'Finding source files in directory: {source_dir}')
            source_dir = Path(source_dir)
            if service_type == 'MapServer':
                # First look for APRX file
                source_file = source_dir / f'{service_name}.aprx'
                if not source_file.is_file():
                    # Fall back to MXD file
                    source_file = source_dir / f'{service_name}.mxd'
            elif service_type == 'GeocodeServer':
                source_file = source_dir / f'{service_name}.loc'
            else:
                log.debug(f'Unsupported service type {service_type} of service {service_name} will be skipped')
                continue
            source_file = source_file.resolve()
            if source_file.is_file():
                log.debug(f'Source file found: {source_file}')
                service_info['source_file'] = str(source_file)
            else:
                log.debug(f'Source file missing: {source_file}')
                errors.append(f'- Source file {source_file} for service {service_name} does not exist!')

    return source_info, errors
