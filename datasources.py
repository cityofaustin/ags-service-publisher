import arcpy
import logging

log = logging.getLogger(__name__)


def get_unique_data_sources(mxd_paths):
    log.info('Getting unique data sources for MXD paths: {}'.format(mxd_paths))
    data_sources = []
    for mxd_path in mxd_paths:
        mxd = arcpy.mapping.MapDocument(mxd_path)
        try:
            data_sources.extend(get_data_sources(mxd))
        finally:
            del mxd
    unique_data_sources = list(set(data_sources))
    return unique_data_sources


def get_data_sources(mxd):
    log.info('Getting data sources for MXD: {}'.format(mxd.filePath))
    layers = arcpy.mapping.ListLayers(mxd)
    for layer in layers:
        if layer.supports('workspacePath'):
            log.info('Layer name: {}, Dataset name: {}, Workspace path: {}'
                     .format(layer.name, layer.datasetName, layer.workspacePath))
            yield layer.workspacePath


def update_data_sources(mxd, data_source_mappings):
    log.info('Updating data sources in MXD: {}'.format(mxd.filePath))
    layers = arcpy.mapping.ListLayers(mxd)
    for layer in layers:
        if layer.supports('workspacePath'):
            try:
                new_workspace_path = data_source_mappings[layer.workspacePath]
                log.info(
                    'Updating workspace path for layer {}, dataset name: {}, current workspace path: {}, new workspace path: {}'
                        .format(layer.name, layer.datasetName, layer.workspacePath, new_workspace_path))
                layer.replaceDataSource(new_workspace_path, 'SDE_WORKSPACE', layer.datasetName, False)
            except KeyError:
                log.warn('No match for layer {}, dataset name: {}, workspace path: {}'
                         .format(layer.name, layer.datasetName, layer.workspacePath))
