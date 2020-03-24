from __future__ import unicode_literals

import os
import getpass
import json
import re
import time
from distutils.util import strtobool
from ssl import create_default_context
from urllib3.poolmanager import PoolManager
from xml.etree import ElementTree

import requests
from requests.compat import urljoin
from requests.adapters import HTTPAdapter

from helpers import split_quoted_string, unquote_string, deep_get
from logging_io import setup_logger

log = setup_logger(__name__)


def create_session(server_url, proxies=None):
    session = requests.Session()
    if proxies:
        session.proxies = proxies
    adapter = SSLContextAdapter()
    session.mount(server_url, adapter)
    return session


def generate_token(server_url, username=None, password=None, expiration=15, ags_instance=None, session=None):
    username, password = prompt_for_credentials(username, password, ags_instance)
    log.info('Generating token (URL: {}, user: {})'.format(server_url, username))
    url = urljoin(server_url, '/arcgis/admin/generateToken')
    try:
        r = session.post(
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


def get_site_mode(server_url, token, session=None):
    log.debug('Getting site mode (URL: {})'.format(server_url))
    url = urljoin(server_url, 'arcgis/admin/mode')
    try:
        r = session.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        site_mode = data.get('siteMode')
        log.debug(
            'Site mode info (URL {}): {}'
            .format(r.url, json.dumps(data, indent=4))
        )
        return site_mode
    except StandardError:
        log.exception('An error occurred while getting site mode (URL: {})'.format(server_url))
        raise


def set_site_mode(server_url, token, site_mode, session=None):
    log.debug('Setting site mode to {} (URL: {})'.format(site_mode, server_url))
    url = urljoin(server_url, 'arcgis/admin/mode/update')
    try:
        r = session.post(url, params={'f': 'json', 'siteMode': site_mode, 'runAsync': False}, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        status = data.get('status')
        if status == 'error':
            raise RuntimeError(data.get('messages'))
        if status != 'success':
            raise RuntimeError(data)
        log.debug(
            'Site mode update result (URL {}): {}'
            .format(r.url, status)
        )
        return status
    except StandardError:
        log.exception('An error occurred while setting site mode to {} (URL: {})'.format(site_mode, server_url))
        raise


def list_data_stores(server_url, token, session=None):
    log.debug('Listing data stores (URL: {})'.format(server_url))
    url = urljoin(server_url, '/arcgis/admin/data/items')
    try:
        r = session.post(
            url,
            params={
                'f': 'json',
            },
            data={
                'token': token,
            }
        )
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        root_items = data.get('rootItems', tuple())
        data_stores = []
        for root_item in root_items:
            url = urljoin(server_url, '/arcgis/admin/data/findItems')
            r = session.post(
                url,
                params={
                    'f': 'json',
                    'ancestorPath': root_item,
                },
                data={
                    'token': token,
                }
            )
            assert (r.status_code == 200)
            data = r.json()
            if data.get('status') == 'error':
                raise RuntimeError(data.get('messages'))
            for item in data.get('items', tuple()):
                item_path = item.get('path', 'n/a')
                item_type = item.get('type', 'n/a')
                file_path = deep_get(item, 'info.path', 'n/a')
                conn_props = parse_connection_string(deep_get(item, 'info.connectionString', dict()))
                user = conn_props.get('USER', 'n/a')
                version = conn_props.get('VERSION', 'n/a')
                database = get_database_from_connection_properties(conn_props)
                data_stores.append(dict(
                    item_path=item_path,
                    item_type=item_type,
                    file_path=file_path,
                    user=user,
                    version=version,
                    database=database,
                ))
        log.debug(
            'Data stores (URL {}): {}'
            .format(r.url, json.dumps(data_stores, indent=4))
        )
        return data_stores
    except StandardError:
        log.exception('An error occurred while listing data stores (URL: {})'.format(server_url))
        raise


def list_service_folders(server_url, token, session=None):
    log.debug('Listing service folders (URL: {})'.format(server_url))
    url = urljoin(server_url, '/arcgis/admin/services')
    try:
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def list_services(server_url, token, service_folder=None, session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def list_service_workspaces(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, data={'token': token})
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.text
        datasets = parse_datasets_from_service_manifest(data)

        for dataset in datasets:
            dataset_name = dataset['dataset_name']
            dataset_type = dataset['dataset_type']
            dataset_path = dataset['dataset_path']
            by_reference = dataset['by_reference']
            conn_props = dataset['conn_props']
            yield dict(
                user=conn_props.get('USER', 'n/a'),
                database=get_database_from_connection_properties(conn_props),
                version=conn_props.get('VERSION', 'n/a'),
                dataset_name=dataset_name,
                dataset_type=dataset_type,
                dataset_path=dataset_path,
                by_reference=by_reference,
            )
    except StandardError:
        log.exception(
            'An error occurred while listing workspaces for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def delete_service(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def get_service_info(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def get_service_item_info(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
    log.debug('Getting item info for service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'iteminfo'
                ) if part
            )
        )
    )
    try:
        r = session.post(url, params={'f': 'json'}, data={'token': token})
        log.debug('Request URL: {}'. format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        if data.get('error'):
            raise RuntimeError(data.get('error').get('message'))
        log.debug(
            'Service {} item info (URL {}, Folder: {}): {}'
            .format(service_name, server_url, service_folder, json.dumps(data, indent=4))
        )
        return data
    except StandardError:
        log.exception(
            'An error occurred while getting item info for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def set_service_item_info(server_url, token, item_info, service_name, service_folder=None, service_type='MapServer', session=None):
    log.debug('Setting item info for service {} (URL {}, Folder: {})'.format(service_name, server_url, service_folder))
    url = urljoin(
        server_url,
        '/'.join(
            (
                part for part in (
                    '/arcgis/admin/services',
                    service_folder,
                    '{}.{}'.format(service_name, service_type),
                    'iteminfo/edit'
                ) if part
            )
        )
    )
    try:
        r = session.post(
            url,
            params={
                'f': 'json'
            },
            data={'token': token},
            files=[
                (
                    'serviceItemInfo',
                    (
                        None,
                        json.dumps(item_info)
                    )
                ),
                (
                    'thumbnail',
                    (
                        '',
                        None,
                        'application/octet-stream'
                    )
                )
            ]
        )
        log.debug('Request URL: {}'.format(r.url))
        assert (r.status_code == 200)
        data = r.json()
        if data.get('status') == 'error':
            raise RuntimeError(data.get('messages'))
        if data.get('error'):
            raise RuntimeError(data.get('error').get('message'))
        log.debug(
            'Updated service {} item info (URL {}, Folder: {}): {}'
            .format(service_name, server_url, service_folder, json.dumps(data, indent=4))
        )
        return data
    except StandardError:
        log.exception(
            'An error occurred while setting item info for service {}/{}'
            .format(service_folder, service_name)
        )
        raise


def get_service_manifest(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def get_service_status(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def test_service(server_url, token, service_name, service_folder=None, service_type='MapServer', warn_on_errors=False, session=None):
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
        r = session.post(url, params=params, data={'token': token})
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
        service_status = get_service_status(server_url, token, service_name, service_folder, service_type, session=session)
        configured_state = service_status.get('configuredState')
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
            service_info = get_service_info(server_url, token, service_name, service_folder, service_type, session=session)
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
            service_info = get_service_info(server_url, token, service_name, service_folder, service_type, session=session)
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


def stop_service(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def start_service(server_url, token, service_name, service_folder=None, service_type='MapServer', session=None):
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
        r = session.post(url, params={'f': 'json'}, data={'token': token})
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


def restart_service(
    server_url,
    token,
    service_name,
    service_folder=None,
    service_type='MapServer',
    delay=30,
    max_retries=3,
    test_after_restart=True,
    session=None
):
    succeeded = False
    configured_state = None
    realtime_state = None
    error_message = None
    retry_count = 0
    while retry_count < max_retries:
        retry_count += 1
        log.info(
            'Restarting service {} (URL {}, Folder: {}, attempt #{} of {})'
            .format(service_name, server_url, service_folder, retry_count, max_retries)
        )
        stop_service(server_url, token, service_name, service_folder, service_type, session=session)
        log.debug(
            'Waiting {} seconds before restarting service {} (URL {}, Folder: {})'
            .format(delay, service_name, server_url, service_folder)
        )
        time.sleep(delay)
        start_service(server_url, token, service_name, service_folder, service_type, session=session)
        log.debug(
            'Waiting {} seconds before checking status of service {} (URL {}, Folder: {})'
            .format(delay, service_name, server_url, service_folder)
        )
        time.sleep(delay)
        service_status = get_service_status(server_url, token, service_name, service_folder, service_type, session=session)
        configured_state = service_status.get('configuredState')
        realtime_state = service_status.get('realTimeState')
        if realtime_state == 'STARTED':
            if test_after_restart:
                test_data = test_service(server_url, token, service_name, service_folder, service_type, warn_on_errors=True, session=session)
                configured_state = test_data.get('configured_state')
                realtime_state = test_data.get('realtime_state')
                error_message = test_data.get('error_message')
                if realtime_state == 'STARTED' and not error_message:
                    succeeded = True
            else:
                succeeded = True
            if succeeded:
                break

    if succeeded:
        log.info(
            '{} service {}/{} successfully restarted after {} attempts (configured state: {}, realtime state: {})'
            .format(service_type, service_folder, service_name, retry_count, configured_state, realtime_state)
        )
    else:
        raise RuntimeError(
            '{} service {}/{} was not successfully restarted after {} attempts! (configured state: {}, realtime state: {}, error message: {})'
            .format(service_type, service_folder, service_name, retry_count, configured_state, realtime_state, error_message)
        )


def parse_datasets_from_service_manifest(data):
    tree = ElementTree.fromstring(data)
    databases_xpath = './Databases/SVCDatabase'
    datasets_xpath = './Datasets/SVCDataset'
    database_elements = tree.findall(databases_xpath)
    for database_element in database_elements:
        by_reference = bool(strtobool(database_element.find('ByReference').text))
        conn_props = parse_connection_properties_from_service_manifest(database_element, by_reference)
        for dataset_element in database_element.findall(datasets_xpath):
            dataset_path = dataset_element.find('OnPremisePath').text
            dataset_name = os.path.basename(dataset_path)
            dataset_type = dataset_element.find('DatasetType').text
            yield dict(
                dataset_name=dataset_name,
                dataset_type=dataset_type,
                dataset_path=dataset_path,
                by_reference=by_reference,
                conn_props=conn_props,
            )


def parse_connection_properties_from_service_manifest(database_element, by_reference):
    for conn_string_xpath in (
        'OnServerConnectionString',
        'OnPremiseConnectionString',
    ):
        # Prefer on-premise connection string if data is copied to server
        if not by_reference and conn_string_xpath == 'OnServerConnectionString':
            continue
        conn_string_element = database_element.find(conn_string_xpath)
        if conn_string_element is not None:
            conn_string = conn_string_element.text
            if conn_string is not None:
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


def parse_database_from_service_string(database):
    if isinstance(database, basestring) and database != 'n/a':
        pattern = re.compile(r'^(?:sde:\w+\$)?(?:sde:\w+:)(?:[\\/];\w+=)?([^;:\$]+)[;:\$]?.*$', re.IGNORECASE)
        match = re.match(pattern, database)
        if match:
            database = match.group(1)
    return database


def get_database_from_connection_properties(conn_props):
    if not conn_props:
        log.warn('No connection properties specified!')
        return 'n/a'
    database = 'n/a'
    if 'DATABASE' in conn_props:
        database = conn_props.get('DATABASE', 'n/a')
    if database == 'n/a' and 'DB_CONNECTION_PROPERTIES' in conn_props:
        database = conn_props.get('DB_CONNECTION_PROPERTIES', 'n/a')
        if database == '/' or database == '\\':
            database = 'n/a'
    if database == 'n/a' and 'INSTANCE' in conn_props:
        database = conn_props.get('INSTANCE', 'n/a')
    database = parse_database_from_service_string(database)
    if database == 'n/a':
        log.warn('No database found in connection properties: {}'.format(conn_props))
    return database


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
            prompt=str('Password{}: ')
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


# Adapted from https://stackoverflow.com/a/50215614
class SSLContextAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_default_context()
        kwargs['ssl_context'] = context
        context.load_default_certs() # this loads the OS defaults on Windows
        self.poolmanager = PoolManager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        context = create_default_context()
        kwargs['ssl_context'] = context
        context.load_default_certs() # this loads the OS defaults on Windows
        return super(SSLContextAdapter, self).proxy_manager_for(*args, **kwargs)
