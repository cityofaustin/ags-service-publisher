from xml.etree import ElementTree
import logging

log = logging.getLogger(__name__)


def modify_sddraft(sddraft, xpath_pairs=None):
    log.info('Modifying service definition draft file: {}'.format(sddraft))
    if not xpath_pairs:
        log.warn('No XPath Key/Value pairs specified, aborting...')
        return
    log.debug('Parsing service definition file: {}'.format(sddraft))
    tree = ElementTree.parse(sddraft)
    for xpath, value in xpath_pairs.iteritems():
        log.debug('Searching for elements matching XPath expression: {}'.format(xpath))
        subelements = tree.findall(xpath)
        log.debug('{} match(es) for XPath expression: {}'.format(len(subelements), xpath))
        for i, subelement in enumerate(subelements):
            log.debug('Setting value of subelement {} to {}'.format(i, value))
            subelement.text = value

    # Add the namespaces which get stripped back into the .SD
    root_elem = tree.getroot()
    root_elem.attrib['xmlns:typens'] = 'http://www.esri.com/schemas/ArcGIS/10.1'
    root_elem.attrib['xmlns:xs'] = 'http://www.w3.org/2001/XMLSchema'
    log.debug('Writing service definition file: {}'.format(sddraft))
    tree.write(sddraft)
