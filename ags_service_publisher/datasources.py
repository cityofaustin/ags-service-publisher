import os
import re

from helpers import list_files_in_dir
from logging_io import setup_logger

log = setup_logger(__name__)


def list_mxds_in_folder(mxd_dir):
    log.debug('Listing MXDs in folder: {}'.format(mxd_dir))
    return list_files_in_dir(mxd_dir, ext='.mxd')


def list_sde_connection_files_in_folder(sde_connections_dir):
    log.debug('Listing SDE connection files in folder: {}'.format(sde_connections_dir))
    return list_files_in_dir(sde_connections_dir, ext='.sde')


def get_unique_data_sources(mxd_paths):
    log.debug('Getting unique data sources for MXD paths: {}'.format(mxd_paths))
    data_sources = []
    for mxd_path in mxd_paths:
        data_sources.extend([data_source[2] for data_source in get_data_sources(mxd_path)])
    unique_data_sources = list(set(data_sources))
    return unique_data_sources


def get_data_sources(mxd_path):
    log.debug('Getting data sources for MXD: {}'.format(mxd_path))
    if not os.path.isfile(mxd_path):
        raise RuntimeError('MXD {} does not exist!'.format(mxd_path))

    import arcpy
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    layers.extend(arcpy.mapping.ListTableViews(mxd))
    for layer in layers:
        if hasattr(layer, 'workspacePath'):
            user = 'n/a'
            database = 'n/a'
            version = 'n/a'
            definition_query = layer.definitionQuery if hasattr(layer, 'definitionQuery') else 'n/a'
            if hasattr(layer, 'serviceProperties'):
                service_props = layer.serviceProperties
                user = service_props.get('UserName', 'n/a')
                version = service_props.get('Version', 'n/a')
                database = parse_database_from_service_string(service_props.get('Service', 'n/a'))

            log.debug(
                'Layer name: {}, Dataset name: {}, Workspace path: {}, User: {}, Database: {}, Version: {}, Definition Query: {}'
                .format(layer.name, layer.datasetName, layer.workspacePath, user, database, version, definition_query)
            )
            yield (
                layer.name,
                layer.datasetName,
                layer.workspacePath,
                user,
                database,
                version,
                definition_query
            )


def parse_database_from_service_string(database):
    if database != 'n/a':
        pattern = re.compile(r'^(?:sde:\w+\$)?(?:sde:\w+:)(?:\\;\w+=)?([^;:\$]+)[;:\$]?.*$', re.IGNORECASE)
        match = re.match(pattern, database)
        if match:
            database = match.group(1)
    return database


def update_data_sources(mxd_path, data_source_mappings):
    log.info('Updating data sources in MXD: {}'.format(mxd_path))

    import arcpy
    mxd = arcpy.mapping.MapDocument(mxd_path)
    layers = arcpy.mapping.ListLayers(mxd)
    layers.extend(arcpy.mapping.ListTableViews(mxd))
    for layer in layers:
        if hasattr(layer, 'workspacePath'):
            try:
                new_workspace_path = data_source_mappings[layer.workspacePath]
                log.info(
                    'Updating workspace path for layer {}, dataset name: {}, '
                    'current workspace path: {}, new workspace path: {}'
                    .format(layer.name, layer.datasetName, layer.workspacePath, new_workspace_path)
                )
                layer.findAndReplaceWorkspacePath(layer.workspacePath, new_workspace_path, False)
            except KeyError:
                log.warn(
                    'No match for layer {}, dataset name: {}, workspace path: {}'
                    .format(layer.name, layer.datasetName, layer.workspacePath)
                )
    mxd.save()
