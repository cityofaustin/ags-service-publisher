import collections
import datetime
import multiprocessing
import os
import tempfile
from copy import deepcopy
from shutil import copyfile, rmtree

from ags_utils import list_services, delete_service, list_service_folders, list_service_workspaces
from config_io import get_config, default_config_dir
from datasources import update_data_sources
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger
from mplog import open_queue, logged_call
from sddraft_io import modify_sddraft

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
        env = config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
        if len(ags_instances) > 0:
            publish_env(config, env_name, user_config, included_instances, excluded_instances, included_services,
                        excluded_services, cleanup_services, service_prefix, service_suffix)
        else:
            log.warn('No publishable instances specified for environment {}'.format(env_name))


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

    if not default_service_properties:
        log.debug('No default service properties specified')

    if len(ags_instances) == 0:
        raise RuntimeError('No publishable instances specified!')

    if len(services) == 0:
        raise RuntimeError('No publishable services specified!')

    log.info(
        'Publishing environment: {}, service folder: {}, ArcGIS Server instances: {}'
        .format(env_name, service_folder, ', '.join(ags_instances))
    )
    with open_queue() as log_queue:
        source_mxd_info = {}
        errors = []
        if mxd_dir_to_copy_from:
            for service in services:
                service_name = service.keys()[0] if isinstance(service, collections.Mapping) else service
                source_mxd_info[service_name] = []
                if isinstance(mxd_dir_to_copy_from, list):
                    # If multiple MXD source folders are provided, look for the MXD in each source folder
                    source_dirs = mxd_dir_to_copy_from
                else:
                    source_dirs = [mxd_dir_to_copy_from]
                for source_dir in source_dirs:
                    source_mxd = os.path.abspath(os.path.join(source_dir, service_name) + '.mxd')
                    if os.path.isfile(source_mxd):
                        source_mxd_info[service_name].append(source_mxd)
            for service_name, source_mxd_paths in source_mxd_info.iteritems():
                if len(source_mxd_paths) == 0:
                    errors.append('- No source MXD found for service {}'.format(service_name))
                elif len(source_mxd_paths) > 1:
                    errors.append('- More than one source MXD found for service {}: \n{}'
                                  .format(
                                        service_name,
                                        '\n'.join('  - {}'
                                                  .format(source_mxd_path) for source_mxd_path in source_mxd_paths)))
        else:
            for service in services:
                service_name = service.keys()[0] if isinstance(service, collections.Mapping) else service
                mxd_path = os.path.abspath(os.path.join(mxd_dir, service_name) + '.mxd')
                if not os.path.exists(mxd_path):
                    errors.append('- MXD {} does not exist!'.format(mxd_path))
        if len(errors) > 0:
            raise RuntimeError(
                'One or more errors occurred while validating the {} environment for service folder {}:\n{}'
                .format(env_name, service_folder, '\n'.join(errors)))
        for service in services:
            is_mapping = isinstance(service, collections.Mapping)
            service_name = service.keys()[0] if is_mapping else service
            merged_service_properties = deepcopy(default_service_properties) if default_service_properties else {}
            if is_mapping:
                service_properties = service.items()[0][1]
                if service_properties:
                    log.debug('Overriding default service properties for service {}'.format(service_name))
                    merged_service_properties.update(service_properties)
                else:
                    log.warn(
                        'No service-level properties specified for service {} even though it was specified as a mapping'
                        .format(service_name)
                    )
            else:
                log.debug('No service-level properties specified for service {}'.format(service_name))
            mxd_path = os.path.abspath(os.path.join(mxd_dir, service_name) + '.mxd')
            if mxd_dir_to_copy_from:
                mxd_to_copy_from = source_mxd_info[service_name][0]
                log.info('Copying {} to {}'.format(mxd_to_copy_from, mxd_path))
                if not os.path.isdir(mxd_dir):
                    os.makedirs(mxd_dir)
                copyfile(mxd_to_copy_from, mxd_path)
            if not os.path.exists(mxd_path):
                raise RuntimeError('MXD {} does not exist!'.format(mxd_path))
            if data_source_mappings:
                proc = multiprocessing.Process(
                    target=logged_call,
                    args=(log_queue, update_data_sources, mxd_path, data_source_mappings)
                )
                proc.start()
                proc.join()
                if proc.exitcode != 0:
                    raise RuntimeError('An error occurred in subprocess {} (pid {}) while updating data sources for MXD {}'
                                       .format(proc.name, proc.pid, mxd_path))
                del proc
            procs = list()
            for ags_instance in ags_instances:
                ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
                ags_connection = ags_instance_props['ags_connection']
                proc = (multiprocessing.Process(target=logged_call, args=(log_queue, publish_service, mxd_path,
                                                                          ags_instance, ags_connection, service_folder,
                                                                          merged_service_properties, service_prefix,
                                                                          service_suffix)), ags_instance)
                proc[0].start()
                procs.append(proc)
            for proc, ags_instance in procs:
                proc.join()

            errors = list()
            for proc, ags_instance in procs:
                if proc.exitcode != 0:
                    errors.append('An error occurred in subprocess {} (pid {}, exitcode {}) while publishing service {}/{} to AGS instance {}'
                                  .format(proc.name, proc.pid, proc.exitcode, service_folder, service_name, ags_instance))
            if len(errors) > 0:
                log.error('One or more errors occurred while publishing service {}/{}, aborting.'
                          .format(service_folder, service_name))
                raise RuntimeError(errors)

    if cleanup_services:
        for ags_instance in ags_instances:
            cleanup_instance(ags_instance, env_name, config, user_config)


def publish_service(mxd_path, ags_instance, ags_connection, service_folder=None, service_properties=None,
                    service_prefix='', service_suffix=''):
    import arcpy
    arcpy.env.overwriteOutput = True

    service_name = '{}{}{}'.format(service_prefix, os.path.splitext(os.path.basename(mxd_path))[0], service_suffix)

    log.info(
        'Publishing MXD {} to ArcGIS Server instance {}, Connection File: {}, Service: {}, Folder: {}'
        .format(mxd_path, ags_instance, ags_connection, service_name, service_folder)
    )

    tempdir = tempfile.mkdtemp()
    log.debug('Temporary directory created: {}'.format(tempdir))
    try:
        sddraft = os.path.join(tempdir, service_name + '.sddraft')
        sd = os.path.join(tempdir, service_name + '.sd')
        mxd = arcpy.mapping.MapDocument(mxd_path)
        log.debug('Creating SDDraft file: {}'.format(sddraft))
        arcpy.mapping.CreateMapSDDraft(mxd, sddraft, service_name, 'FROM_CONNECTION_FILE', ags_connection, False,
                                       service_folder)
        modify_sddraft(sddraft, service_properties)
        log.debug('Analyzing SDDraft file: {}'.format(sddraft))
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
            log.debug('Staging SDDraft file: {} to SD file: {}'.format(sddraft, sd))
            arcpy.StageService_server(sddraft, sd)
            log.debug('Uploading SD file: {} to AGS connection file: {}'.format(sd, ags_connection))
            arcpy.UploadServiceDefinition_server(sd, ags_connection)
            log.info('Service {}/{} successfully published to {} at {:%#m/%#d/%y %#I:%M:%S %p}'
                     .format(service_folder, service_name, ags_instance, datetime.datetime.now()))
        else:
            error_message = 'Analysis failed for service {}/{} at {:%#m/%#d/%y %#I:%M:%S %p}' \
                .format(service_folder, service_name, datetime.datetime.now())
            log.error(error_message)
            raise RuntimeError(error_message, analysis['errors'])
    except:
        log.exception('An error occurred while publishing service {}/{} to ArcGIS Server instance {}'
                      .format(service_folder, service_name, ags_instance))
        raise
    finally:
        log.debug('Cleaning up temporary directory: {}'.format(tempdir))
        rmtree(tempdir, ignore_errors=True)


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
    user_config = get_config('userconfig', config_dir)
    for ags_instance in ags_instances:
        cleanup_instance(ags_instance, env_name, config, user_config)


def cleanup_instance(ags_instance, env_name, config, user_config):
    configured_services = config['services']
    service_folder = config.get('service_folder')
    log.info('Cleaning up unused services on environment {}, ArcGIS Server instance {}, service folder {}'
             .format(env_name, ags_instance, service_folder))
    ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
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
                        included_envs=asterisk_tuple, excluded_envs=empty_tuple,
                        config_dir=default_config_dir):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Finding dataset usages on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
        for ags_instance in ags_instances:
            ags_instance_props = env['ags_instances'][ags_instance]
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            service_folders = list_service_folders(server_url, token)
            for service_folder in superfilter(service_folders, included_service_folders, excluded_service_folders):
                for service in list_services(server_url, token, service_folder):
                    service_name = service['serviceName']
                    service_type = service['type']
                    if superfilter((service_name,), included_services, excluded_services):
                        for dataset_path in list_service_workspaces(server_url, token, service_name, service_folder,
                                                                    service_type):
                            dataset_name = os.path.basename(dataset_path)
                            if superfilter((dataset_name,), included_datasets, excluded_datasets):
                                yield (ags_instance, service_folder, service_name, service_type, dataset_name,
                                       dataset_path)
