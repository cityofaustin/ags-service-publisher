import json

from xml.etree import ElementTree

from .helpers import snake_case_to_pascal_case
from .logging_io import setup_logger

log = setup_logger(__name__)


def modify_sddraft(sddraft, service_properties=None):
    if not service_properties:
        log.debug('No service properties specified, SDDraft will not be modified.')
        return
    log.debug('Modifying service definition draft file: {}'.format(sddraft))
    tree = ElementTree.parse(sddraft)

    # Handle special configuration service properties
    copy_data_to_server = service_properties.pop('copy_data_to_server', False)
    replace_service = service_properties.pop('replace_service', False)
    tile_scheme_file = service_properties.pop('tile_scheme_file', None)
    cache_tile_format = service_properties.pop('cache_tile_format', None)
    compression_quality = service_properties.pop('compression_quality', None)
    keep_existing_cache = service_properties.pop('keep_existing_cache', False)
    feature_access = service_properties.pop('feature_access', None)
    extensions = service_properties.pop('extensions', None)
    calling_context = service_properties.pop('calling_context', None)
    java_heap_size = service_properties.pop('java_heap_size', None)
    date_field_settings = service_properties.pop('date_field_settings', None)

    # Remove special service properties handled elsewhere
    for property in (
        'network_analysis_layers',
        'network_data_sources',
        'network_dataset_path',
        'network_dataset_template',
        'recreate_network_dataset',
        'update_network_analysis_layers',
    ):
        service_properties.pop(property, None)

    if date_field_settings:
        log.debug('Date field settings specified')
        valid_date_field_keys = (
            'dateFieldsRespectsDayLightSavingTime',
            'dateFieldsTimezoneID',
            'datesInUnknownTimeZone',
            'preferredTimeZoneID',
            'preferredTimeZoneRespectsDayLightSavingTime',
        )
        for key, value in date_field_settings.items():
            for valid_key in valid_date_field_keys:
                if key == valid_key or snake_case_to_pascal_case(key).lower() == valid_key.lower():
                    if str(value) == 'True' or str(value) == 'False':
                        value = str(value).lower()
                    log.debug(f'Setting {valid_key} to {value}')
                    service_properties_array = tree.find('./Configurations/SVCConfiguration/Definition/ConfigurationProperties/PropertyArray')
                    date_field_settings_element = service_properties_array.find(f"./PropertySetProperty[Key='{valid_key}']")
                    if date_field_settings_element:
                        date_field_settings_element.find('Value').text = str(value)
                    else:
                        date_field_settings_element = ElementTree.SubElement(service_properties_array, 'PropertySetProperty', {'xsi:type': 'typens:PropertySetProperty'})
                        ElementTree.SubElement(date_field_settings_element, 'Key').text = valid_key
                        ElementTree.SubElement(date_field_settings_element, 'Value', {'xsi:type': 'xs:string'}).text = str(value)
                    break
            else:
                log.warn(f'Unrecognized date field setting: {key}')

    if extensions:
        log.debug('Extensions specified')
        for extension_name, extension_properties in extensions.items():
            extension_enabled = extension_properties.pop('enabled', False)
            if extension_enabled:
                extension_element = tree.find(
                    f"./Configurations/SVCConfiguration/Definition/Extensions/SVCExtension[TypeName='{extension_name}']"
                )
                log.debug(f'Enabling {extension_name} extension')
                extension_element.find('Enabled').text = 'true'
                set_service_properties(extension_properties, extension_element, './*/PropertyArray/PropertySetProperty')

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
        log.debug(f'Setting calling context to {calling_context}')
        tree.find(
            "./StagingSettings/PropertyArray/PropertySetProperty[Key='CallingContext']/Value"
        ).text = str(calling_context)

    # Set Java heap size
    if java_heap_size is not None:
        log.debug(f'Setting Java heap size to {java_heap_size}')
        service_properties_array = tree.find('./Configurations/SVCConfiguration/Definition/Props/PropertyArray')
        framework_properties_element = service_properties_array.find("./PropertySetProperty[Key='frameworkProperties']")
        if framework_properties_element:
            framework_properties = json.loads(framework_properties_element.find('Value').text)
            framework_properties['javaHeapSize'] = str(java_heap_size)
            framework_properties_element.find('Value').text = json.dumps(framework_properties)
        else:
            framework_properties_element = ElementTree.SubElement(service_properties_array, 'PropertySetProperty', {'xsi:type': 'typens:PropertySetProperty'})
            ElementTree.SubElement(framework_properties_element, 'Key').text = 'frameworkProperties'
            framework_properties = {'javaHeapSize': str(java_heap_size)}
            ElementTree.SubElement(framework_properties_element, 'Value', {'xsi:type': 'xs:string'}).text = json.dumps(framework_properties)

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

    set_service_properties(service_properties, tree, './Configurations/SVCConfiguration/Definition/*/PropertyArray/PropertySetProperty')

    # Add the namespaces which get stripped back into the .SD
    root_elem = tree.getroot()
    root_elem.attrib['xmlns:typens'] = 'http://www.esri.com/schemas/ArcGIS/3.2.0'
    root_elem.attrib['xmlns:xs'] = 'http://www.w3.org/2001/XMLSchema'
    log.debug('Writing service definition file: {}'.format(sddraft))
    tree.write(sddraft)

def set_service_properties(service_properties, parent, xpath):
    log.debug('Searching for elements matching XPath expression: {}'.format(xpath))
    property_elements = parent.findall(xpath)
    log.debug('{} match(es) for XPath expression: {}'.format(len(property_elements), xpath))
    for key, value in service_properties.items():
        for property_element in property_elements:
            property_name = property_element.find('Key').text
            if key == property_name or snake_case_to_pascal_case(key).lower() == property_name.lower():
                if str(value) == 'True' or str(value) == 'False':
                    value = str(value).lower()
                log.debug('Setting value of property name {} to {}'.format(property_name, value))
                property_element.find('Value').text = str(value)
                break
        else:
            log.warn(f'No matches for service property {key}')
