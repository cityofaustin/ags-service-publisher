from __future__ import unicode_literals

import collections
import datetime
import os
import tempfile
from copy import deepcopy
from shutil import rmtree

from ags_utils import (
    get_service_manifest,
    get_service_status,
    list_service_folders,
    list_service_workspaces,
    list_services,
    restart_service,
    test_service
)
from config_io import get_config, default_config_dir
from datasources import open_mxd, list_layers_in_mxd, get_layer_fields, get_layer_properties
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger

log = setup_logger(__name__)


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
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if (
                        service_type in ('MapServer', 'GeocodeServer') and
                        superfilter((service_name,), included_services, excluded_services)
                    ):
                        file_path = None
                        try:
                            service_manifest = get_service_manifest(server_url, token, service_name, service_folder, service_type)
                            file_path = service_manifest['resources'][0]['onPremisePath']
                            file_type = {
                                'MapServer': 'MXD',
                                'GeocodeServer': 'Locator'
                            }[service_type]
                            log.info(
                                'Analyzing {} service {}/{} on ArcGIS Server instance {} (Connection File: {}, {} Path: {})'
                                .format(service_type, service_folder, service_name, ags_instance, ags_connection, file_type, file_path)
                            )
                            if not arcpy.Exists(file_path):
                                raise RuntimeError('{} {} does not exist!'.format(file_type, file_path))
                            try:
                                tempdir = tempfile.mkdtemp()
                                log.debug('Temporary directory created: {}'.format(tempdir))
                                sddraft = os.path.join(tempdir, service_name + '.sddraft')
                                log.debug('Creating SDDraft file: {}'.format(sddraft))

                                if service_type == 'MapServer':
                                    mxd = open_mxd(file_path)
                                    analysis = arcpy.mapping.CreateMapSDDraft(
                                        mxd,
                                        sddraft,
                                        service_name,
                                        'FROM_CONNECTION_FILE',
                                        ags_connection,
                                        False,
                                        service_folder
                                    )
                                elif service_type == 'GeocodeServer':
                                    locator_path = file_path
                                    analysis = arcpy.CreateGeocodeSDDraft(
                                        locator_path,
                                        sddraft,
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
                                    if items:
                                        log.info('----' + key.upper() + '---')
                                        for ((message, code), layerlist) in items.iteritems():
                                            code = '{:05d}'.format(code)
                                            log_method('    {} (CODE {})'.format(message, code))
                                            code = '="{}"'.format(code)
                                            if not layerlist:
                                                yield (
                                                    env_name,
                                                    ags_instance,
                                                    service_folder,
                                                    service_name,
                                                    service_type,
                                                    file_path,
                                                    key[:-1].title(),
                                                    code,
                                                    message,
                                                    None,
                                                    None,
                                                    None
                                                )
                                            else:
                                                log_method('       applies to:')
                                                for layer in layerlist:
                                                    layer_name = layer.longName if hasattr(layer, 'longName') else layer.name
                                                    log_method('           {}'.format(layer_name))
                                                    yield (
                                                        env_name,
                                                        ags_instance,
                                                        service_folder,
                                                        service_name,
                                                        service_type,
                                                        file_path,
                                                        key[:-1].title(),
                                                        code,
                                                        message,
                                                        layer_name,
                                                        layer.datasetName,
                                                        layer.workspacePath
                                                    )
                                            log_method('')

                                if analysis['errors']:
                                    error_message = 'Analysis failed for service {}/{} at {:%#m/%#d/%y %#I:%M:%S %p}' \
                                        .format(service_folder, service_name, datetime.datetime.now())
                                    log.error(error_message)
                                    raise RuntimeError(error_message, analysis['errors'])
                            finally:
                                log.debug('Cleaning up temporary directory: {}'.format(tempdir))
                                rmtree(tempdir, ignore_errors=True)
                        except StandardError as e:
                            log.exception(
                                'An error occurred while analyzing {} service {}/{} on ArcGIS Server instance {}'
                                .format(service_type, service_folder, service_name, ags_instance)
                            )
                            if not warn_on_errors:
                                raise
                            else:
                                yield (
                                    env_name,
                                    ags_instance,
                                    service_folder,
                                    service_name,
                                    service_type,
                                    file_path,
                                    'Error',
                                    None,
                                    e.message,
                                    None,
                                    None,
                                    None
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
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if (
                        service_type == 'MapServer' and
                        superfilter((service_name,), included_services, excluded_services)
                    ):
                        mxd_path = None
                        try:
                            service_manifest = get_service_manifest(server_url, token, service_name, service_folder, service_type)
                            mxd_path = service_manifest['resources'][0]['onPremisePath']
                            log.info(
                                'Listing layers and fields for {} service {}/{} on ArcGIS Server instance {} '
                                '(Connection File: {}, MXD Path: {})'
                                .format(service_type, service_folder, service_name, ags_instance, ags_connection, mxd_path)
                            )
                            if not arcpy.Exists(mxd_path):
                                raise RuntimeError('MXD {} does not exist!'.format(mxd_path))
                            mxd = open_mxd(mxd_path)
                            for layer in list_layers_in_mxd(mxd):
                                if not (
                                    (hasattr(layer, 'isGroupLayer') and layer.isGroupLayer) or
                                    (hasattr(layer, 'isRasterLayer') and layer.isRasterLayer)
                                ):
                                    layer_name = layer.longName if hasattr(layer, 'longName') else layer.name
                                    try:
                                        (
                                            layer_name,
                                            dataset_name,
                                            workspace_path,
                                            is_broken,
                                            user,
                                            database,
                                            version,
                                            definition_query,
                                            show_labels,
                                            symbology_type,
                                            symbology_field
                                        ) = get_layer_properties(layer)
                                    except StandardError as e:
                                        log.exception(
                                            'An error occurred while retrieving properties for layer {} in MXD {}'
                                            .format(layer_name, mxd_path)
                                        )
                                        if not warn_on_errors:
                                            raise
                                        else:
                                            yield (
                                                env_name,
                                                ags_instance,
                                                service_folder,
                                                service_name,
                                                service_type,
                                                'Error retrieving layer properties: {}'.format(e.message),
                                                mxd_path,
                                                layer_name
                                            )
                                            continue
                                    try:
                                        if is_broken:
                                            raise RuntimeError(
                                                'Layer\'s data source is broken (Layer: {}, Data Source: {})'.format(
                                                    layer_name,
                                                    layer.dataSource if hasattr(layer, 'dataSource') else 'n/a'
                                                )
                                            )
                                        for (
                                            field_name,
                                            field_type,
                                            has_index,
                                            field_in_definition_query,
                                            field_in_label_class_expression,
                                            field_in_label_class_sql_query
                                        ) in get_layer_fields(layer):
                                            needs_index = not has_index and (
                                                field_in_definition_query or
                                                field_in_label_class_expression or
                                                field_in_label_class_sql_query or
                                                field_name == symbology_field or
                                                field_type == 'Geometry'
                                            )

                                            yield (
                                                env_name,
                                                ags_instance,
                                                service_folder,
                                                service_name,
                                                service_type,
                                                None,
                                                mxd_path,
                                                layer_name,
                                                dataset_name,
                                                workspace_path,
                                                is_broken,
                                                user,
                                                database,
                                                version,
                                                definition_query,
                                                show_labels,
                                                symbology_type,
                                                symbology_field,
                                                field_name,
                                                field_type,
                                                has_index,
                                                needs_index,
                                                field_in_definition_query,
                                                field_in_label_class_expression,
                                                field_in_label_class_sql_query
                                            )
                                    except StandardError as e:
                                        log.exception(
                                            'An error occurred while listing fields for layer {} in MXD {}'
                                            .format(layer_name, mxd_path)
                                        )
                                        if not warn_on_errors:
                                            raise
                                        else:
                                            yield (
                                                env_name,
                                                ags_instance,
                                                service_folder,
                                                service_name,
                                                service_type,
                                                'Error retrieving layer fields: {}'.format(e.message),
                                                mxd_path,
                                                layer_name,
                                                dataset_name,
                                                workspace_path,
                                                is_broken,
                                                user,
                                                database,
                                                version,
                                                definition_query,
                                                show_labels,
                                                symbology_type,
                                                symbology_field
                                            )
                        except StandardError as e:
                            log.exception(
                                'An error occurred while listing layers and fields for {} service {}/{} on '
                                'ArcGIS Server instance {} (Connection File: {})'
                                .format(service_type, service_folder, service_name, ags_instance, ags_connection)
                            )
                            if not warn_on_errors:
                                raise
                            else:
                                yield (
                                    env_name,
                                    ags_instance,
                                    service_folder,
                                    service_name,
                                    service_type,
                                    e.message,
                                    mxd_path
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
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if superfilter((service_name,), included_services, excluded_services):
                        for (
                            dataset_name,
                            dataset_type,
                            dataset_path,
                            user,
                            database,
                            version
                        ) in list_service_workspaces(
                            server_url,
                            token,
                            service_name,
                            service_folder,
                            service_type
                        ):
                            if (
                                superfilter((dataset_name,), included_datasets, excluded_datasets) and
                                superfilter((user,), included_users, excluded_users) and
                                superfilter((database,), included_databases, excluded_databases) and
                                superfilter((version,), included_versions, excluded_versions)
                            ):
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
                                    dataset_path
                                )


def restart_services(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    include_running_services=True,
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
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if superfilter((service_name,), included_services, excluded_services):
                        if not include_running_services:
                            status = get_service_status(server_url, token, service_name, service_folder, service_type)
                            if status.get('configuredState') == 'STARTED':
                                pass
                            restart_service(server_url, token, service_name, service_folder, service_type)
                        restart_service(server_url, token, service_name, service_folder, service_type)


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
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if superfilter((service_name,), included_services, excluded_services):
                        test_data = test_service(server_url, token, service_name, service_folder, service_type, warn_on_errors)
                        yield (
                            env_name,
                            ags_instance,
                            service_folder,
                            service_name,
                            service_type,
                            test_data.get('configured_state'),
                            test_data.get('realtime_state'),
                            test_data.get('request_url'),
                            test_data.get('request_method'),
                            test_data.get('http_status_code'),
                            test_data.get('http_status_reason'),
                            test_data.get('error_message'),
                            test_data.get('response_time')
                        )


def normalize_services(services, default_service_properties):
    for service in services:
        yield normalize_service(service, default_service_properties)


def normalize_service(service, default_service_properties):
    is_mapping = isinstance(service, collections.Mapping)
    service_name = service.keys()[0] if is_mapping else service
    merged_service_properties = deepcopy(default_service_properties) if default_service_properties else {}
    if is_mapping:
        service_properties = service.items()[0][1]
        if service_properties:
            log.debug('Overriding default service properties for service {}'.format(service_name))
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


def get_source_info(services, source_dir, staging_dir, default_service_properties):
    log.debug(
        'Getting source info for services {}, source directory: {}, staging directory {}'
        .format(services, source_dir, staging_dir)
    )

    source_info = {}
    errors = []

    for service_name, service_type, service_properties in normalize_services(services, default_service_properties):
        service_info = source_info[service_name] = {
            'source_file': None,
            'staging_files': []
        }
        if staging_dir:
            staging_files = service_info['staging_files']
            # If multiple staging folders are provided, look for the source item in each staging folder
            staging_dirs = (staging_dir,) if isinstance(staging_dir, basestring) else staging_dir
            for _staging_dir in staging_dirs:
                log.debug('Finding staging items in directory: {}'.format(_staging_dir))
                if service_type == 'MapServer':
                    staging_file = os.path.abspath(os.path.join(_staging_dir, service_name + '.mxd'))
                elif service_type == 'GeocodeServer':
                    staging_file = os.path.abspath(os.path.join(_staging_dir, service_name + '.loc'))
                else:
                    log.debug('Unsupported service type {} of service {} will be skipped'.format(service_type, service_name))

                if os.path.isfile(staging_file):
                    log.debug('Staging file found: {}'.format(staging_file))
                    staging_files.append(staging_file)
                else:
                    log.debug('Staging file missing: {}'.format(staging_file))

            if len(staging_files) == 0:
                errors.append('- No staging file found for service {}'.format(service_name))
            elif len(staging_files) > 1:
                errors.append(
                    '- More than one staging file found for service {}: \n{}'
                    .format(
                        service_name,
                        '\n'.join('  - {}'.format(staging_file) for staging_file in staging_files)
                    )
                )

        if source_dir:
            log.debug('Finding source files in directory: {}'.format(source_dir))
            if service_type == 'MapServer':
                source_file = os.path.abspath(os.path.join(source_dir, service_name + '.mxd'))
            elif service_type == 'GeocodeServer':
                source_file = os.path.abspath(os.path.join(source_dir, service_name + '.loc'))
            else:
                log.debug('Unsupported service type {} of service {} will be skipped'.format(service_type, service_name))
            if os.path.isfile(source_file):
                log.debug('Source file found: {}'.format(source_file))
                service_info['source_file'] = source_file
            else:
                log.debug('Source file missing: {}'.format(source_file))
                errors.append('- Source file {} for service {} does not exist!'.format(source_file, service_name))

    return source_info, errors
