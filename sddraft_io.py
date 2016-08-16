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
    xpath = "./Configurations/SVCConfiguration/Definition/*/PropertyArray/PropertySetProperty"
    log.debug('Searching for elements matching XPath expression: {}'.format(xpath))
    property_elements = tree.findall(xpath)
    log.debug('{} match(es) for XPath expression: {}'.format(len(property_elements), xpath))
    for property_element in property_elements:
        property_name = property_element.find('Key').text
        for key, value in service_properties.iteritems():
            if snake_case_to_pascal_case(key).lower() == property_name.lower():
                log.debug('Setting value of property name {} to {}'.format(property_name, value))
                property_element.find('Value').text = str(value)

    # Add the namespaces which get stripped back into the .SD
    root_elem = tree.getroot()
    root_elem.attrib['xmlns:typens'] = 'http://www.esri.com/schemas/ArcGIS/10.1'
    root_elem.attrib['xmlns:xs'] = 'http://www.w3.org/2001/XMLSchema'
    log.debug('Writing service definition file: {}'.format(sddraft))
    tree.write(sddraft)
