import collections
import fnmatch
from pathlib import Path

from .helpers import list_files_in_dir, deep_get
from .logging_io import setup_logger

log = setup_logger(__name__)


def list_mxds_in_folder(mxd_dir):
    log.debug(f'Listing MXDs in folder: {mxd_dir}')
    return list_files_in_dir(mxd_dir, ext='.mxd')


def list_sde_connection_files_in_folder(sde_connections_dir):
    log.debug(f'Listing SDE connection files in folder: {sde_connections_dir}')
    return list_files_in_dir(sde_connections_dir, ext='.sde')


def get_unique_data_sources(aprx_paths, include_table_views=True):
    log.debug(f'Getting unique data sources for ArcGIS Pro project files: {aprx_paths}')
    data_sources = []
    for aprx_path in aprx_paths:
        data_sources.extend([data_source[2] for data_source in get_aprx_data_sources(aprx_path, include_table_views)])
    unique_data_sources = list(set(data_sources))
    return unique_data_sources


def open_aprx(aprx_path):
    log.debug(f'Opening ArcGIS Pro project file {aprx_path}')
    if not aprx_path == 'CURRENT':
        aprx_path = Path(aprx_path)
        if not aprx_path.is_file():
            raise RuntimeError(f'ArcGIS Pro project file {aprx_path} does not exist!')

    log.debug('Importing arcpy...')
    try:
        import arcpy
    except Exception:
        log.exception('An error occurred importing arcpy')
        raise
    log.debug('Successfully imported arcpy')
    return arcpy.mp.ArcGISProject(str(aprx_path))


def convert_mxd_to_aprx(mxd_path, aprx_path):
    log.info(f'Converting MXD {mxd_path} to ArcGIS Pro project file {aprx_path}')
    blank_aprx_path = Path(__file__) / '../../resources/arcgis/projects/blank/blank.aprx'
    blank_aprx_path = blank_aprx_path.resolve()
    blank_aprx = open_aprx(blank_aprx_path)
    blank_aprx.importDocument(mxd_path)
    blank_aprx.saveACopy(aprx_path)
    log.info(f'Successfully converted MXD {mxd_path} to ArcGIS Pro project file {aprx_path}')


def list_layers_in_map(map_, include_table_views=True):
    log.debug(f'Listing layers in map: {map_.name}')

    layers = map_.listLayers()
    if include_table_views:
        layers.extend(map_.listTables())
    for layer in layers:
        yield layer


def get_aprx_data_sources(aprx_path, include_table_views=True):
    log.debug(f'Getting data sources for ArcGIS Pro project file: {aprx_path}')

    for layer in list_layers_in_map(open_aprx(aprx_path).listMaps()[0], include_table_views):
        if deep_get(layer, 'dataSource', False):
            yield get_layer_properties(layer)


def get_layer_properties(layer):
    layer_name = deep_get(layer, 'longName', layer.name)
    log.debug(f'Getting properties for layer: {layer_name}')

    if deep_get(layer, 'dataSource', False):
        (
            definition_query,
            show_labels,
            symbology_type,
            symbology_fields,
            dataset_name,
            user,
            version,
            database,
            db_connection_properties,
        ) = (
            deep_get(layer, attr, 'n/a') for attr in (
                'definitionQuery',
                'showLabels',
                'symbology.renderer.type',
                'symbology.renderer.fields',
                'connectionProperties.dataset',
                'connectionProperties.connection_info.user',
                'connectionProperties.connection_info.version',
                'connectionProperties.connection_info.database',
                'connectionProperties.connection_info.db_connection_properties',
            )
        )

        # Provide a fallback for layers without a database property
        if database == 'n/a':
            database = db_connection_properties

        result = dict(
            layer_name=layer_name,
            dataset_name=dataset_name,
            is_broken=layer.isBroken,
            user=user,
            database=database,
            version=version,
            definition_query=definition_query,
            show_labels=show_labels,
            symbology_type=symbology_type,
            symbology_fields=symbology_fields
        )

        log.debug(
            'Layer name: {layer_name}, '
            'Dataset name: {dataset_name}, '
            'Data source is broken: {is_broken}, '
            'User: {user}, '
            'Database: {database}, '
            'Version: {version}, '
            'Definition query: {definition_query}, '
            'Show labels: {show_labels}, '
            'Symbology type: {symbology_type}, '
            'Symbology fields: {symbology_fields}'
            .format(**result)
        )

        return result
    else:
        raise RuntimeError(f'Unsupported layer: {layer_name}')


def get_layer_fields(layer):
    layer_name = deep_get(layer, 'longName', layer.name)
    log.debug(f'Getting fields for layer: {layer_name}')
    log.debug('Importing arcpy...')
    try:
        import arcpy
    except Exception:
        log.exception('An error occurred importing arcpy')
        raise
    log.debug('Successfully imported arcpy')
    desc = arcpy.Describe(layer)
    fields = desc.fields
    indexes = desc.indexes
    for field in fields:
        in_definition_query = field.name.lower() in layer.definitionQuery if hasattr(layer, 'definitionQuery') else False
        yield dict(
            field_name=field.name,
            field_type=field.type,
            has_index=get_field_index(field, indexes),
            in_definition_query=in_definition_query,
            **find_field_in_label_classes(layer, field)
        )


def get_field_index(field, indexes):
    field_name = field.name
    log.debug(f'Getting index for field: {field_name}')
    has_index = False
    for index in indexes:
        for index_field in index.fields:
            if has_index:
                break
            if index_field.name == field_name:
                has_index = True
                break
        if has_index:
            break
    return has_index


def find_field_in_label_classes(layer, field):
    in_label_class_expression = in_label_class_sql_query = False
    if deep_get(layer, 'showLabels', False):
        label_classes = layer.listLabelClasses()
        field_name = field.name
        log.debug(f'Finding occurrences of field {field_name} in label classes')
        for label_class in label_classes:
            if in_label_class_expression and in_label_class_sql_query:
                break
            if label_class.visible:
                if not in_label_class_expression and label_class.expression:
                    in_label_class_expression = field_name.lower() in label_class.expression.lower()
                if not in_label_class_sql_query and label_class.SQLQuery:
                    in_label_class_sql_query = field_name.lower() in label_class.SQLQuery.lower()
    return dict(
        in_label_class_expression=in_label_class_expression,
        in_label_class_sql_query=in_label_class_sql_query
    )


def update_data_sources(aprx_path, data_source_mappings):
    log.info(f'Updating data sources in ArcGIS Pro project file: {aprx_path}')

    try:
        aprx = open_aprx(aprx_path)
        map_ = aprx.listMaps()[0]
        for layer in list_layers_in_map(map_):
            if deep_get(layer, 'dataSource', False):
                layer_props = get_layer_properties(layer)
                layer_name = layer_props.get('layer_name')
                dataset_name = layer_props.get('dataset_name')
                current_database = layer_props.get('database')
                current_version = layer_props.get('version')
                match_found = False

                if isinstance(data_source_mappings, collections.abc.Mapping):
                    for source, target in data_source_mappings.items():
                        if match_data_source_mapping(layer_props, source, target):
                            match_found = True
                            break
                else:
                    for data_source_mapping in data_source_mappings:
                        if isinstance(data_source_mapping, collections.abc.Mapping):
                            source = data_source_mapping.get('source')
                            target = data_source_mapping.get('target')
                            if not source or not target:
                                for source, target in data_source_mapping.items():
                                    if match_data_source_mapping(layer_props, source, target):
                                        match_found = True
                                        break
                            if match_found:
                                break
                            if match_data_source_mapping(layer_props, source, target):
                                match_found = True
                                break

                if match_found:
                    new_database = target
                    log.info(
                        f'Updating connection properties for layer {layer_name}, dataset name: {dataset_name}, '
                        f'current database: {current_database}, current version: {current_version}, new database: {new_database}'
                    )
                    update_layer_data_source(map_, layer, target)
                else:
                    log.warn(
                        f'No match for layer {layer_name}, dataset name: {dataset_name}, database: {current_database}, version: {current_version}'
                    )
        log.debug(f'Saving ArcGIS Pro project {aprx_path}')
        aprx.save()
    except Exception:
        log.exception(f'An error occurred while updating data sources in ArcGIS Pro project file: {aprx_path}')
        raise


def match_data_source_mapping(layer_props, source, target):
    match_found = False
    if isinstance(source, collections.abc.Mapping):
        if all(
            (
                fnmatch.fnmatch(layer_props.get(key), value)
                for key, value in source.items()
            )
        ):
            match_found = True
    elif fnmatch.fnmatch(layer_props.get('database'), source):
        match_found = True
    return match_found


def get_geometry_statistics(dataset_path):
    log.debug(f'Getting geometry statistics for dataset: {dataset_path}')

    log.debug('Importing arcpy...')
    try:
        import arcpy
    except Exception:
        log.exception('An error occurred importing arcpy')
        raise
    log.debug('Successfully imported arcpy')
    desc = arcpy.Describe(dataset_path)
    data_type = desc.dataType

    feature_count = 0
    part_count = 0
    vertex_count = 0

    if data_type == 'Table':
        shape_type = 'n/a'
        feature_count = int(arcpy.GetCount_management(dataset_path).getOutput(0))
    else:
        shape_type = desc.shapeType
        with arcpy.da.SearchCursor(dataset_path, ('SHAPE@',)) as cursor:
            for (shape,) in cursor:
                feature_count += 1
                if shape:
                    part_count += shape.partCount
                    if shape_type == 'Polygon':
                        # Exclude last vertex from each polygon part
                        vertex_count += (shape.pointCount - shape.partCount)
                    else:
                        vertex_count += shape.pointCount

    avg_part_count = part_count / feature_count if feature_count > 0 else 0
    avg_vertex_count = vertex_count / feature_count if feature_count > 0 else 0

    return dict(
        shape_type=shape_type,
        feature_count=feature_count,
        avg_part_count=avg_part_count,
        avg_vertex_count=avg_vertex_count
    )


def update_layer_data_source(map_, layer, workspace):
    '''
    Workaround for Esri BUG-000112574 (https://support.esri.com/en/bugs/nimbus/QlVHLTAwMDExMjU3NA==)
    '''
    try:
        workspace_path = Path(workspace)
        dataset_name = deep_get(layer, 'connectionProperties.dataset')
        cim = layer.getDefinition('V2')
        feature_dataset = getattr(cim.featureTable.dataConnection, 'featureDataset', None)
        dummy_layer_path = ((workspace_path / feature_dataset) if feature_dataset else workspace_path) / dataset_name
        dummy_layer = map_.addDataFromPath(str(dummy_layer_path))
        dummy_cim = dummy_layer.getDefinition('V2')
        current_connection_string = cim.featureTable.dataConnection.workspaceConnectionString
        new_connection_string = dummy_cim.featureTable.dataConnection.workspaceConnectionString

        log.debug(
            f'Updating connection properties for layer {layer.name}, dataset name: {dataset_name}, '
            f'current connection string: \n{current_connection_string}, new connection string: \n{new_connection_string}'
        )

        cim.featureTable.dataConnection.workspaceConnectionString = new_connection_string
        layer.setDefinition(cim)
        map_.removeLayer(dummy_layer)
    except Exception:
        log.exception(f'An error occurred while updating the data source for layer {layer.name}, workspace: {workspace}')
        raise
