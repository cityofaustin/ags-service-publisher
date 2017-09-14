import os
import getpass
import json
import time
from xml.etree import ElementTree

import requests
from requests.compat import urljoin

from datasources import parse_database_from_service_string
from helpers import split_quoted_string, unquote_string
from logging_io import setup_logger

log = setup_logger(__name__)


def generate_token(server_url, username=None, password=None, expiration=15, ags_instance=None):
    username, password = prompt_for_credentials(username, password, ags_instance)
    log.info('Generating token (URL: {}, user: {})'.format(server_url, username))
    url = urljoin(server_url, '/arcgis/admin/generateToken')
    try:
        r = requests.post(
            url,
            {
                'username': username,
                'password': password,
                'client': 'requestip',
                'expiration': str(expiration),
                'f': 'json'
            }
        )
        log.debug('Request URL: {}'.format(r.url))
        assert r.status_code == 200
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info(
            'Successfully generated token (URL: {}, user: {}, expires: {}'
            .format(server_url, username, data['expires'])
        )
        return data['token']
    except StandardError:
        log.exception('An error occurred while generating token (URL: {}, user: {})'.format(server_url, username))
        raise


def list_service_folders(server_url, token):
    log.debug('Listing service folders (URL: {})'.format(server_url))
    url = urljoin(server_url, '/arcgis/admin/services')
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        service_folders = data.get('folders')
        log.debug(
            'Service folders (URL {}): {}'
            .format(r.url, json.dumps(service_folders, indent=4))
        )
        return service_folders
    except StandardError:
        log.exception('An error occurred while listing service folders (URL: {})'.format(server_url))
        raise


def list_services(server_url, token, service_folder=None):
    log.debug('Listing services (URL: {}, Folder: {})'.format(server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.debug(
            '{} services (URL {}): {}'
            .format(service_folder, r.url, json.dumps(data, indent=4))
        )
        services = data['services']
        return services
    except StandardError:
        log.exception(
            'An error occurred while listing services (URL: {}, Folder: {})'
            .format(server_url, service_folder)
        )
        raise


def list_service_workspaces(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    if service_type == 'GeometryServer':
        log.warn(
            'Unsupported service type {} for service {} in folder {}'
            .format(service_type, service_name, service_folder)
        )
        return
    log.debug(
        'Listing workspaces for service {} (URL: {}, Folder: {})'
        .format(service_name, server_url, service_folder)
    )
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'iteminfo/manifest/manifest.xml'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.text
        datasets = parse_datasets_from_service_manifest(data)
        conn_props = parse_connection_properties_from_service_manifest(data)

        for dataset in datasets:
            yield (
                dataset,
                conn_props.get('USER', 'n/a'),
                parse_database_from_service_string(conn_props.get('INSTANCE', 'n/a')),
                conn_props.get('VERSION', 'n/a')
            )
    except StandardError:
        log.exception(
            'An error occurred while listing workspaces for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def delete_service(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.info('Deleting service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'delete'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info(
            'Service {} successfully deleted (URL {}, Folder: {})'
            .format(service_name, server_url, service_folder)
        )
    except StandardError:
        log.exception(
            'An error occurred while deleting service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def get_service_info(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.debug('Getting info for service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/rest/services',
                    service_folder,
                    '{}/{}'.format(service_name, service_type)
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'. format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        if data.get('error'):
            raise RuntimeError(data.get('error').get('message'))
        log.debug(
            'Service {} info (URL {}, Folder: {}): {}'
            .format(service_name, server_url, service_folder, json.dumps(data, indent=4))
        )
        return data
    except StandardError:
        log.exception(
            'An error occurred while getting info for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def get_service_manifest(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.debug('Getting manifest for service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'iteminfo/manifest/manifest.json'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'. format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        if data.get('error'):
            raise RuntimeError(data.get('error').get('message'))
        log.debug(
            'Service {} manifest (URL {}, Folder: {}): {}'
            .format(service_name, server_url, service_folder, json.dumps(data, indent=4))
        )
        return data
    except StandardError:
        log.exception(
            'An error occurred while getting manifest for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def get_service_status(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.debug('Getting status of service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'status'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.debug(
            'Service {} status (URL {}, Folder: {}): {}'
            .format(service_name, server_url, service_folder, json.dumps(data, indent=4))
        )
        return data
    except StandardError:
        log.exception(
            'An error occurred while getting the status of service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def test_service(server_url, token, service_name, service_folder=None, service_type='MapServer', warn_on_errors=False):
    log.info('Testing {} service {} (URL {}, Folder: {})'.format(service_type, service_name, server_url, service_folder))

    def perform_service_health_check(operation, params, service_status):
        url = urljoin(
            server_url,
            '/'.join(
                (
                    part for part in (
                        '/arcgis/rest/services',
                        service_folder,
                        '{}/{}'.format(service_name, service_type),
                        operation
                    ) if part
                )
            )
        )
        start_time = time.time()
        r = requests.post(url, params=params, data={'token': token})
        end_time = time.time()
        response_time = end_time - start_time
        log.debug(
            'Request URL: {}, HTTP Status: {}, Response Time: {:.2f}'
            .format(r.url, r.status_code, response_time)
        )
        data = r.json()
        error = data.get('error')
        error_message = error.get('message') if error else None
        if not warn_on_errors:
            r.raise_for_status()
            if error_message:
                raise RuntimeError(error_message)
        if not error_message and r.status_code == 200:
            log.info('{} service {}/{} tested successfully'.format(service_type, service_folder, service_name))
        elif error_message:
            log.warn(
                'An error occurred while testing {} service {}/{}: {}'
                .format(service_type, service_folder, service_name, error_message)
            )
        elif r.status_code != 200:
            log.warn(
                '{} service {}/{} responded with a status code of {} ({})'
                .format(service_type, service_folder, service_name, r.status_code, r.reason)
            )
        return {
            'request_url': r.url,
            'request_method': r.request.method,
            'http_status_code': r.status_code,
            'http_status_reason': r.reason,
            'response_time': response_time,
            'configured_state': service_status.get('configuredState'),
            'realtime_state': service_status.get('realTimeState'),
            'error_message': error_message
        }

    try:
        service_status = get_service_status(server_url, token, service_name, service_folder, service_type)
        configured_state = service_status.get('configuredState'),
        realtime_state = service_status.get('realTimeState')
        if realtime_state != 'STARTED':
            log.warn(
                '{} service {}/{} is not running (configured state: {}, realtime state: {})!'
                .format(service_type, service_folder, service_name, configured_state, realtime_state)
            )
            return {
                'configured_state': configured_state,
                'realtime_state': realtime_state
            }
        if service_type == 'MapServer':
            service_info = get_service_info(server_url, token, service_name, service_folder, service_type)
            initial_extent = json.dumps(service_info.get('initialExtent'))
            return perform_service_health_check(
                'identify',
                {
                    'f': 'json',
                    'geometry': initial_extent,
                    'geometryType': 'esriGeometryEnvelope',
                    'tolerance': '0',
                    'layers': 'all',
                    'mapExtent': initial_extent,
                    'imageDisplay': '400,300,96',
                    'returnGeometry': 'false'
                },
                service_status
            )
        elif service_type == 'GeocodeServer':
            service_info = get_service_info(server_url, token, service_name, service_folder, service_type)
            address_fields = service_info.get('addressFields')
            first_address_field_name = address_fields[0].get('name')
            return perform_service_health_check(
                'findAddressCandidates',
                {
                    'f': 'json',
                    first_address_field_name: '100 Main St'
                },
                service_status
            )
        else:
            log.warn(
                'Unsupported service type {} for service {} in folder {}'
                .format(service_type, service_name, service_folder)
            )
            return {
                'configured_state': configured_state,
                'realtime_state': realtime_state
            }
    except StandardError as e:
        log.exception(
            'An error occurred while testing {} service {}/{}'
            .format(service_type, service_folder, service_name)
        )
        if not warn_on_errors:
            raise
        return {
            'error_message': e.message
        }


def stop_service(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.info('Stopping service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'stop'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info(
            'Service {} successfully stopped (URL {}, Folder: {})'
            .format(service_name, server_url, service_folder)
        )
    except StandardError:
        log.exception(
            'An error occurred while stopping service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def start_service(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.info('Starting service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'start'
                ) if part
            )
        )
    )
    try:
        r = requests.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        log.info(
            'Service {} successfully started (URL {}, Folder: {})'
            .format(service_name, server_url, service_folder)
        )
    except StandardError:
        log.exception(
            'An error occurred while starting service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def restart_service(server_url, token, service_name, service_folder=None, service_type='MapServer'):
    log.info('Restarting service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    stop_service(server_url, token, service_name, service_folder, service_type)
    start_service(server_url, token, service_name, service_folder, service_type)
    service_status = get_service_status(server_url, token, service_name, service_folder, service_type)
    configured_state = service_status.get('configuredState'),
    realtime_state = service_status.get('realTimeState')
    if realtime_state != 'STARTED':
        raise RuntimeError(
            '{} service {}/{} was not successfully restarted! (configured state: {}, realtime state: {})'
            .format(service_type, service_folder, service_name, configured_state, realtime_state)
        )


def parse_datasets_from_service_manifest(data):
    tree = ElementTree.fromstring(data)
    datasets_xpath = './Databases/SVCDatabase/Datasets/SVCDataset/OnPremisePath'
    subelements = tree.findall(datasets_xpath)
    for subelement in subelements:
        yield subelement.text


def parse_connection_properties_from_service_manifest(data):
    tree = ElementTree.fromstring(data)
    conn_string_xpath = './Databases/SVCDatabase/OnServerConnectionString'
    conn_string_element = tree.find(conn_string_xpath)
    if conn_string_element is not None:
        conn_string = conn_string_element.text
        return parse_connection_string(conn_string)
    else:
        log.warn('No connection string element found!')
        return {}


def parse_connection_string(conn_string):
    properties = {}

    for pair in split_quoted_string(conn_string, ';'):
        key, value = split_quoted_string(pair, '=')
        properties[key] = unquote_string(value)
    return properties


def prompt_for_credentials(username=None, password=None, ags_instance=None):
    if not username:
        username = raw_input(
            'User name{}: '
            .format(
                ' for ArcGIS Server instance {}'
                .format(ags_instance) if ags_instance else ''
            )
        )
    if not password:
        password = getpass.getpass(
            prompt='Password{}: '
            .format(
                ' for ArcGIS Server instance {}'
                .format(ags_instance) if ags_instance else ''
            )
        )
    return username, password


def import_sde_connection_file(ags_connection_file, sde_connection_file):
    log.info(
        'Importing SDE connection file {} to ArcGIS Server connection file {})'
        .format(sde_connection_file, ags_connection_file)
    )

    import arcpy
    data_store_name = os.path.splitext(os.path.basename(sde_connection_file))[0]
    try:
        arcpy.AddDataStoreItem(
            ags_connection_file,
            "DATABASE",
            data_store_name,
            sde_connection_file,
            sde_connection_file
        )
    except StandardError as e:
        if e.message == 'Client database entry is already registered.':
            log.warn(e.message)
        else:
            log.exception(
                'An error occurred while importing SDE connection file {} to ArcGIS Server connection file {})'
                .format(sde_connection_file, ags_connection_file)
            )
            raise
