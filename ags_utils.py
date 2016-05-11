import getpass
import logging

import requests
from requests.compat import urljoin

from config_io import get_config, default_config_dir

log = logging.getLogger(__name__)


def generate_token(ags_instance, username=None, password=None, expiration='15', config_dir=default_config_dir):
    if not username:
        username = raw_input('User name: ')
    if not password:
        password = getpass.getpass()
    log.info('Generating token for ArcGIS Server instance: {}, user: {}'.format(ags_instance, username))
    user_config = get_config('userconfig', config_dir)
    ags_props = user_config['ags_instances'][ags_instance]
    baseurl = ags_props['url']
    r = requests.post(baseurl + '/arcgis/admin/generateToken',
                      {'username': username, 'password': password, 'client': 'requestip', 'expiration': expiration,
                       'f': 'json'})
    assert r.status_code == 200
    data = r.json()
    if data.get('status') == 'error':
        raise RuntimeError(data.get('messages'))
    log.info('Successfully generated token for ArcGIS Server instance: {}, user: {}, expires: {}'.format(ags_instance,
                                                                                                         username, data[
                                                                                                             'expires']))
    return data['token']


def list_services(ags_instance, service_folder=None, config_dir=default_config_dir):
    log.info('Listing services on ArcGIS Server instance {}, Folder: {}'.format(ags_instance, service_folder))
    user_config = get_config('userconfig', config_dir)
    ags_props = user_config['ags_instances'][ags_instance]
    baseurl = ags_props['url']
    token = ags_props['token']
    url = urljoin(baseurl, '/'.join(('/arcgis/admin/services', service_folder)))
    r = requests.get(url, {'token': token, 'f': 'json'})
    assert (r.status_code == 200)
    data = r.json()
    if data.get('status') == 'error':
        raise RuntimeError(data.get('messages'))
    services = data['services']
    return services


def delete_service(ags_instance, service_name, service_folder=None, service_type='MapServer',
                   config_dir=default_config_dir):
    log.info('Deleting service {} on ArcGIS Server instance {}, Folder: {}'.format(service_name, ags_instance,
                                                                                   service_folder))
    user_config = get_config('userconfig', config_dir)
    ags_props = user_config['ags_instances'][ags_instance]
    baseurl = ags_props['url']
    token = ags_props['token']
    url = urljoin(baseurl, '/'.join((part for part in (
        '/arcgis/admin/services', service_folder, '{}.{}'.format(service_name, service_type), 'delete') if part)))
    log.info(url)
    r = requests.post(url, {'token': token, 'f': 'json'})
    assert (r.status_code == 200)
    data = r.json()
    if data.get('status') == 'error':
        raise RuntimeError(data.get('messages'))
    log.info(
        'Service {} successfully deleted from ArcGIS Server instance {}, Folder: {}'.format(service_name, ags_instance,
                                                                                            service_folder))
