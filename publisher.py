import datetime
import os
from shutil import copyfile

import arcpy

from ags_utils import list_services, delete_service, list_service_folders, list_service_workspaces
from config_io import get_config, default_config_dir
from datasources import update_data_sources
from extrafilters import superfilter
from helpers import snake_case_to_camel_case, asterisk_tuple, empty_tuple
from logging_io import setup_logger
from sddraft_io import modify_sddraft

arcpy.env.overwriteOutput = True

log = setup_logger(__name__)

def publish_config(config, config_dir,
                   included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                   included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                   included_services=asterisk_tuple, excluded_services=empty_tuple,
                   cleanup_services=False,
                   service_prefix='',
                   service_suffix=''):
    env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No publishable environments specified!')

    log.info('Publishing environments: {}'.format(', '.join(env_names)))
    user_config = get_config('userconfig', config_dir)
    for env_name in env_names:
        publish_env(config, env_name, user_config, included_instances, excluded_instances, included_services,
                    excluded_services, cleanup_services, service_prefix, service_suffix)


def publish_config_name(config_name, config_dir=default_config_dir,
                        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                        included_services=asterisk_tuple, excluded_services=empty_tuple,
                        cleanup_services=False,
                        service_prefix='',
                        service_suffix=''):
    config = get_config(config_name, config_dir)
    log.info('Publishing config \'{}\''.format(config_name))
    publish_config(config, config_dir, included_envs, excluded_envs, included_instances, excluded_instances,
                   included_services, excluded_services, cleanup_services, service_prefix, service_suffix)


def publish_env(config, env_name, user_config,
                included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                included_services=asterisk_tuple, excluded_services=empty_tuple,
                cleanup_services=False,
                service_prefix='',
                service_suffix=''):
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
                ags_props = user_config['ags_instances'][ags_instance]
                ags_connection = ags_props['ags_connection']
                publish_service(mxd, ags_instance, ags_connection, service_folder, service_properties, service_prefix,
                                service_suffix)
            mxd.save()
        finally:
            del mxd

    if cleanup_services:
        for ags_instance in ags_instances:
            cleanup_instance(ags_instance, config)


def publish_service(mxd, ags_instance, ags_connection, service_folder=None, service_properties=None, service_prefix='',
                    service_suffix=''):
    mxd_path = mxd.filePath
    service_name = '{}{}{}'.format(service_prefix, os.path.splitext(os.path.basename(mxd_path))[0], service_suffix)

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


def cleanup_config(config,
                   included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                   included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                   config_dir=default_config_dir):
    env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No cleanable environments specified!')

    log.info('Cleaning environments: {}'.format(', '.join(env_names)))
    for env_name in env_names:
        cleanup_env(config, env_name, included_instances, excluded_instances, config_dir)


def cleanup_env(config, env_name, included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                config_dir=default_config_dir):
    env = config['environments'][env_name]
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    if len(ags_instances) == 0:
        raise RuntimeError('No cleanable instances specified!')
    for ags_instance in ags_instances:
        cleanup_instance(ags_instance, config, config_dir)


def cleanup_instance(ags_instance, config, config_dir=default_config_dir):
    configured_services = config['services']
    service_folder = config.get('service_folder')
    log.info('Cleaning up unused services on ArcGIS Server instance {}, service folder {}'
             .format(ags_instance, service_folder))
    userconfig = get_config('userconfig', config_dir)
    ags_instance_props = userconfig['ags_instances'][ags_instance]
    server_url = ags_instance_props['url']
    token = ags_instance_props['token']
    existing_services = list_services(server_url, token, service_folder)
    services_to_remove = [service for service in existing_services if service['serviceName'] not in configured_services]
    log.info('Removing {} services: {}'
             .format(len(services_to_remove), ', '.join((service['serviceName'] for service in services_to_remove))))
    for service in services_to_remove:
        delete_service(server_url, token, service['serviceName'], service_folder, service['type'])


def find_dataset_usages(included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
                        included_services=asterisk_tuple, excluded_services=empty_tuple,
                        included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
                        included_instances=asterisk_tuple, excluded_instances=empty_tuple,
                        config_dir=default_config_dir):
    userconfig = get_config('userconfig', config_dir)
    ags_instances = superfilter(userconfig['ags_instances'].keys(), included_instances, excluded_instances)
    log.info('Finding dataset usages on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
    for ags_instance in ags_instances:
        ags_instance_props = userconfig['ags_instances'][ags_instance]
        server_url = ags_instance_props['url']
        token = ags_instance_props['token']
        service_folders = list_service_folders(server_url, token)
        for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
            for service in list_services(server_url, token, service_folder):
                service_name = service['serviceName']
                service_type = service['type']
                if superfilter((service_name,), included_services, excluded_services):
                    for dataset_path in list_service_workspaces(server_url, token, service_name, service_folder, service_type):
                        dataset_name = os.path.basename(dataset_path)
                        if superfilter((dataset_name,), included_datasets, excluded_datasets):
                            yield (ags_instance, service_folder, service_name, service_type, dataset_name, dataset_path)
