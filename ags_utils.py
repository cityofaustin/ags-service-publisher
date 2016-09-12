import os
import getpass
from xml.etree import ElementTree

import requests
from requests.compat import urljoin

from logging_io import setup_logger

log = setup_logger(__name__)


def generate_token(server_url, username=None, password=None, expiration=15, ags_instance=None):
    username, password = prompt_for_credentials(username, password, ags_instance)
    log.info('Generating token (URL: {}, user: {})'.format(server_url, username))
    url = urljoin(server_url, '/arcgis/admin/generateToken')
    log.debug(url)
    try:
        r = requests.post(url, {'username': username, 'password': password, 'client': 'requestip',
                                'expiration': str(expiration), 'f': 'json'})
        assert r.status_code == 200
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info('Successfully generated token (URL: {}, user: {}, expires: {}'
                 .format(server_url, username, data['expires']))
        return data['token']
    except:
        log.exception('An error occurred while generating token (URL: {}, user: {})'.format(server_url, username))
        raise

def list_service_folders(server_url, token):
    log.debug('Listing service folders (URL: {})'.format(server_url))
    url = urljoin(server_url, '/arcgis/admin/services')
    log.debug(url)
    try:
        r = requests.get(url, {'token': token, 'f': 'json'})
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        service_folders = data['folders']
        return service_folders
    except:
        log.exception('An error occurred while listing service folders (URL: {})'.format(server_url))
        raise


def list_services(server_url, token, service_folder=None):
    log.debug('Listing services (URL: {}, Folder: {})'.format(server_url, service_folder))
    url = urljoin(server_url, '/'.join((part for part in ('/arcgis/admin/services', service_folder) if part)))
    log.debug(url)
    try:
        r = requests.get(url, {'token': token, 'f': 'json'})
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        services = data['services']
        return services
    except:
        log.exception('An error occurred while listing services (URL: {}, Folder: {})'
                      .format(server_url, service_folder))
        raise


def list_service_workspaces(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    if service_type == 'GeometryServer':
        log.warn('Unsupported service type {} for service {} in folder {}'
                 .format(service_type, service_name, service_folder))
        return ()
    log.debug('Listing workspaces for service {} (URL: {}, Folder: {})'
             .format(service_name, server_url, service_folder))
    url = urljoin(server_url, '/'.join((part for part in (
        '/arcgis/admin/services', service_folder, '{}.{}'.format(service_name, service_type),
        'iteminfo/manifest/manifest.xml') if part)))
    log.debug(url)
    try:
        r = requests.get(url, {'token': token})
        assert (r.status_code == 200)
        data = r.text
        return parse_datasets_from_service_manifest(data)
    except:
        log.exception('An error occurred while listing workspaces for service {}{}'
                      .format(service_folder, service_name))
        raise


def delete_service(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.info('Deleting service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(server_url, '/'.join((part for part in (
        '/arcgis/admin/services', service_folder, '{}.{}'.format(service_name, service_type), 'delete') if part)))
    log.debug(url)
    try:
        r = requests.post(url, {'token': token, 'f': 'json'})
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info('Service {} successfully deleted (URL {}, Folder: {})'
                 .format(service_name, server_url, service_folder))
    except:
        log.exception('An error occurred while deleting service {}{}'
                      .format(service_folder, service_name))
        raise


def parse_datasets_from_service_manifest(data):
    xpath = './Databases/SVCDatabase/Datasets/SVCDataset/OnPremisePath'
    tree = ElementTree.fromstring(data)
    subelements = tree.findall(xpath)
    for subelement in subelements:
        yield subelement.text


def prompt_for_credentials(username=None, password=None, ags_instance=None):
    if not username:
        username = raw_input('User name{}: '
                             .format(' for ArcGIS Server instance {}'
                                     .format(ags_instance) if ags_instance else ''))
    if not password:
        password = getpass.getpass(prompt='Password{}: '
                                   .format(' for ArcGIS Server instance {}'
                                        .format(ags_instance) if ags_instance else ''))
    return username, password


def import_sde_connection_file(ags_connection_file, sde_connection_file):
    log.info('Importing SDE connection file {} to ArcGIS Server connection file {})'
             .format(sde_connection_file, ags_connection_file))

    import arcpy
    data_store_name = os.path.splitext(os.path.basename(sde_connection_file))[0]
    try:
        arcpy.AddDataStoreItem(
            ags_connection_file,
            "DATABASE",
            data_store_name,
            sde_connection_file,
            sde_connection_file)
    except:
        log.exception('An error occurred while importing SDE connection file {} to ArcGIS Server connection file {})'
                      .format(sde_connection_file, ags_connection_file))
        raise
