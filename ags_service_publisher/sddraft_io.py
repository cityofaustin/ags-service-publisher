from __future__ import unicode_literals

from xml.etree import ElementTree

from helpers import snake_case_to_pascal_case
from logging_io import setup_logger

log = setup_logger(__name__)


def modify_sddraft(sddraft, service_properties=None):
    if not service_properties:
        log.debug('No service properties specified, SDDraft will not be modified.')
        return
    log.debug('Modifying service definition draft file: {}'.format(sddraft))
    tree = ElementTree.parse(sddraft)

    # Handle special configuration service properties
    copy_data_to_server = service_properties.get('copy_data_to_server', False)
    replace_service = service_properties.get('replace_service', False)
    tile_scheme_file = service_properties.get('tile_scheme_file')
    cache_tile_format = service_properties.get('cache_tile_format')
    compression_quality = service_properties.get('compression_quality')
    keep_existing_cache = service_properties.get('keep_existing_cache', False)
    feature_access = service_properties.get('feature_access')
    calling_context = service_properties.get('calling_context')

    if feature_access:
        log.debug('Feature access properties specified')
        feature_access_enabled = feature_access.get('enabled', False)
        feature_access_capabilities = feature_access.get('capabilities')

        feature_access_element = tree.find(
            "./Configurations/SVCConfiguration/Definition/Extensions/SVCExtension[TypeName='FeatureServer']"
        )

        if feature_access_enabled:
            log.debug('Enabling feature access')
            feature_access_element.find('Enabled').text = 'true'

        if feature_access_capabilities:
            feature_access_capabilities = [capability.capitalize() for capability in feature_access_capabilities]
            log.debug('Setting feature access capabilities {}'.format(feature_access_capabilities))
            feature_access_element.find(
                "./Info/PropertyArray/PropertySetProperty[Key='WebCapabilities']/Value"
            ).text = ','.join(feature_access_capabilities)
    else:
        log.debug('No feature access properties specified')

    # Copy data to server if specified
    if copy_data_to_server:
        log.debug('Copying data to server')
        tree.find('ByReference').text = 'true'
        tree.find(
            "./StagingSettings/PropertyArray/PropertySetProperty[Key='IncludeDataInSDFile']/Value"
        ).text = 'true'
    else:
        log.debug('Data will not be copied to server')

    # Set calling context
    if calling_context is not None:
        log.debug('Setting calling context to {}'.format(calling_context))
        tree.find(
            "./StagingSettings/PropertyArray/PropertySetProperty[Key='CallingContext']/Value"
        ).text = str(calling_context)

    # Replace the service if specified
    if replace_service:
        log.debug('Replacing existing service')
        tree.find('Type').text = 'esriServiceDefinitionType_Replacement'
    else:
        log.debug('Publishing new service')

    # Update the tile cache scheme if specified
    if tile_scheme_file:
        log.debug('Tile scheme file {} specified'.format(tile_scheme_file))
        tile_tree = ElementTree.parse(tile_scheme_file)
        tile_cache_info_element = tile_tree.find('TileCacheInfo')
        cache_schema_element = tree.find('CacheSchema')
        old_tile_cache_info_element = cache_schema_element.find('TileCacheInfo')
        cache_schema_element.remove(old_tile_cache_info_element)
        cache_schema_element.append(tile_cache_info_element)
    else:
        log.debug('No tile scheme file specified')

    # Update the cache tile format if specified
    if cache_tile_format:
        log.debug('Cache tile format {} specified'.format(cache_tile_format))
        cache_schema_element = tree.find('CacheSchema')
        tile_image_info_element = cache_schema_element.find('TileImageInfo')
        cache_tile_format_element = tile_image_info_element.find('CacheTileFormat')
        cache_tile_format_element.text = cache_tile_format
    else:
        log.debug('No cache tile format specified')

    # Update the cache image compression quality if specified
    if compression_quality:
        log.debug('Cache image compression quality {} specified'.format(compression_quality))
        cache_schema_element = tree.find('CacheSchema')
        tile_image_info_element = cache_schema_element.find('TileImageInfo')
        compression_quality_element = tile_image_info_element.find('CompressionQuality')
        compression_quality_element.text = compression_quality
    else:
        log.debug('No cache image compression quality specified')

    # Keep the existing cache if specified
    if keep_existing_cache:
        log.debug('Keeping existing map cache')
        keep_existing_map_cache_element = tree.find('KeepExistingMapCache')
        keep_existing_map_cache_element.text = 'true'
    else:
        log.debug('Replacing existing map cache')

    xpath = "./Configurations/SVCConfiguration/Definition/*/PropertyArray/PropertySetProperty"
    log.debug('Searching for elements matching XPath expression: {}'.format(xpath))
    property_elements = tree.findall(xpath)
    log.debug('{} match(es) for XPath expression: {}'.format(len(property_elements), xpath))
    for property_element in property_elements:
        property_name = property_element.find('Key').text
        for key, value in service_properties.iteritems():
            if snake_case_to_pascal_case(key).lower() == property_name.lower():
                if str(value) == 'True' or str(value) == 'False':
                    value = str(value).lower()
                log.debug('Setting value of property name {} to {}'.format(property_name, value))
                property_element.find('Value').text = str(value)

    # Add the namespaces which get stripped back into the .SD
    root_elem = tree.getroot()
    root_elem.attrib['xmlns:typens'] = 'http://www.esri.com/schemas/ArcGIS/10.1'
    root_elem.attrib['xmlns:xs'] = 'http://www.w3.org/2001/XMLSchema'
    log.debug('Writing service definition file: {}'.format(sddraft))
    tree.write(sddraft)
