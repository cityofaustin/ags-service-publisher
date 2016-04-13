import os
import collections
import logging
import datetime
from shutil import copyfile

import arcpy

from datasources import update_data_sources
from extrafilters import superfilter
from config_io import read_config_from_file
from sddraft_io import modify_sddraft
from helpers import snake_case_to_camel_case, asterisk_tuple, empty_tuple

arcpy.env.overwriteOutput = True

default_config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config'))

log = logging.getLogger(__name__)


def get_config(config_name, config_dir=default_config_dir):
    log.debug('Getting config \'{}\' in directory: {}'.format(config_name, config_dir))
    return read_config_from_file(os.path.abspath(os.path.join(config_dir, config_name + '.yml')))


def get_configs(config_names=asterisk_tuple, config_dir=default_config_dir):
    log.debug('Getting configs \'{}\' in directory: {}'.format(config_names, config_dir))
    if len(config_names) == 1 and config_names[0] == '*':
        config_names = (os.path.splitext(os.path.basename(config_file))[0] for config_file in superfilter(
            os.listdir(config_dir), inclusion_patterns=('*.yml',), exclusion_patterns=('userconfig.yml',)))
    return collections.OrderedDict(((config_name, get_config(config_name, config_dir)) for config_name in config_names))


def publish_config(config, config_dir, included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                   included_instances=asterisk_tuple, excluded_instances=empty_tuple, included_services=asterisk_tuple,
                   excluded_services=empty_tuple):
    env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No publishable environments specified!')

    log.info('Publishing environments: {}'.format(', '.join(env_names)))
    user_config = get_config('userconfig', config_dir)
    for env_name in env_names:
        publish_env(config, env_name, user_config, included_instances, excluded_instances, included_services,
                    excluded_services)


def publish_config_name(config_name, config_dir=default_config_dir, included_envs=asterisk_tuple,
                        excluded_envs=empty_tuple,
                        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                        included_services=asterisk_tuple,
                        excluded_services=empty_tuple):
    config = get_config(config_name, config_dir)
    log.info('Publishing config \'{}\''.format(config_name))
    publish_config(config, config_dir, included_envs, excluded_envs, included_instances, excluded_instances,
                   included_services, excluded_services)


def publish_env(config, env_name, user_config, included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                included_services=asterisk_tuple, excluded_services=empty_tuple):
    env = config['environments'][env_name]
    mxd_dir = env['mxd_dir']
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    services = superfilter(config['services'], included_services, excluded_services)
    service_folder = config.get('service_folder', os.path.basename(mxd_dir))
    default_service_properties = config.get('default_service_properties')
    data_source_mappings = env.get('data_source_mappings', {})
    mxd_dir_to_copy_from = env.get('mxd_dir_to_copy_from')

    if len(ags_instances) == 0:
        raise RuntimeError('No publishable instances specified!')

    if len(services) == 0:
        raise RuntimeError('No publishable services specified!')

    log.info(
        'Publishing environment: {}, service folder: {}, ArcGIS Server instances: {}'
            .format(env_name, service_folder, ', '.join(ags_instances))
    )

    for service_name, service_properties in services.iteritems() if hasattr(services, 'iteritems') \
            else ((service, default_service_properties) for service in services):
        mxd_path = os.path.abspath(os.path.join(mxd_dir, service_name) + '.mxd')
        if mxd_dir_to_copy_from:
            mxd_to_copy_from = os.path.abspath(os.path.join(mxd_dir_to_copy_from, service_name) + '.mxd')
            log.info('Copying {} to {}'.format(mxd_to_copy_from, mxd_path))
            copyfile(mxd_to_copy_from, mxd_path)
        mxd = arcpy.mapping.MapDocument(mxd_path)
        try:
            if data_source_mappings:
                update_data_sources(mxd, data_source_mappings)
                mxd.save()
            for ags_instance in ags_instances:
                ags_connection = user_config['ags_connections'][ags_instance]
                publish_service(mxd, ags_instance, ags_connection, service_folder, service_properties)
            mxd.save()
        finally:
            del mxd


def publish_service(mxd, ags_instance, ags_connection, service_folder='', service_properties=None):
    mxd_path = mxd.filePath
    service_name = os.path.splitext(os.path.basename(mxd_path))[0]

    log.info(
        'Publishing MXD {} to ArcGIS Server instance {}, Connection File: {}, Service: {}, Folder: {}'
            .format(mxd_path, ags_instance, ags_connection, service_name, service_folder)
    )

    xpath_pairs = None

    if service_properties:
        xpath_pairs = {}
        for key, value in service_properties.iteritems():
            xpath_spec = "./Configurations/SVCConfiguration/Definition/Props/PropertyArray/PropertySetProperty[Key='{}']/Value"
            xpath_pairs[xpath_spec.format(snake_case_to_camel_case(key))] = str(value)

    sddraft = os.path.abspath(os.path.join(os.path.dirname(mxd_path), service_name + '.sddraft'))
    sd = os.path.abspath(os.path.join(os.path.dirname(mxd_path), service_name + '.sd'))
    arcpy.mapping.CreateMapSDDraft(mxd, sddraft, service_name, 'FROM_CONNECTION_FILE', ags_connection, False,
                                   service_folder)
    modify_sddraft(sddraft, xpath_pairs)
    analysis = arcpy.mapping.AnalyzeForSD(sddraft)
    for key, log_method in (('messages', log.info), ('warnings', log.warn), ('errors', log.error)):
        log.info('----' + key.upper() + '---')
        items = analysis[key]
        for ((message, code), layerlist) in items.iteritems():
            log_method('    {} (CODE {:05d})'.format(message, code))
            log_method('       applies to:')
            for layer in layerlist:
                log_method('           {}'.format(layer.name))
            log_method('')

    if analysis['errors'] == {}:
        arcpy.StageService_server(sddraft, sd)
        arcpy.UploadServiceDefinition_server(sd, ags_connection)
        log.info('Service {}/{} successfully published to {} at {:%#m/%#d/%y %#I:%M:%S %p}'
                 .format(service_folder, service_name, ags_instance, datetime.datetime.now()))
    else:
        error_message = 'Analysis failed for service {}/{} at {:%#m/%#d/%y %#I:%M:%S %p}' \
            .format(service_folder, service_name, datetime.datetime.now())
        log.error(error_message)
        raise RuntimeError(error_message, analysis['errors'])
