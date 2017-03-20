import collections
import datetime
import multiprocessing
import os
import tempfile
from copy import deepcopy
from shutil import copyfile, rmtree

from ags_utils import list_services, delete_service, list_service_folders, list_service_workspaces, restart_service, get_service_status, test_service
from config_io import get_config, get_configs, default_config_dir
from datasources import update_data_sources, get_data_sources
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger
from mplog import open_queue, logged_call
from sddraft_io import modify_sddraft

log = setup_logger(__name__)


def publish_config(
    config,
    config_dir,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    copy_source_files_from_staging_folder=True,
    cleanup_services=False,
    service_prefix='',
    service_suffix='',
    warn_on_validation_errors=False
):
    env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No publishable environments specified!')

    log.info('Publishing environments: {}'.format(', '.join(env_names)))
    user_config = get_config('userconfig', config_dir)
    for env_name in env_names:
        env = config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
        if len(ags_instances) > 0:
            publish_env(
                config,
                env_name,
                user_config,
                included_instances, excluded_instances,
                included_services, excluded_services,
                copy_source_files_from_staging_folder,
                cleanup_services,
                service_prefix,
                service_suffix,
                warn_on_validation_errors
            )
        else:
            log.warn('No publishable instances specified for environment {}'.format(env_name))


def publish_config_name(
    config_name,
    config_dir=default_config_dir,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    copy_source_files_from_staging_folder=True,
    cleanup_services=False,
    service_prefix='',
    service_suffix='',
    warn_on_validation_errors=False
):
    config = get_config(config_name, config_dir)
    log.info('Publishing config \'{}\''.format(config_name))
    publish_config(
        config,
        config_dir,
        included_envs, excluded_envs,
        included_instances, excluded_instances,
        included_services, excluded_services,
        copy_source_files_from_staging_folder,
        cleanup_services,
        service_prefix,
        service_suffix,
        warn_on_validation_errors
    )


def publish_env(
    config,
    env_name,
    user_config,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    copy_source_files_from_staging_folder=True,
    cleanup_services=False,
    service_prefix='',
    service_suffix='',
    warn_on_validation_errors=False
):
    env = config['environments'][env_name]
    source_dir = env['source_dir']
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    services = superfilter(config['services'], included_services, excluded_services)
    service_folder = config.get('service_folder', os.path.basename(source_dir))
    default_service_properties = config.get('default_service_properties')
    data_source_mappings = env.get('data_source_mappings', {})
    staging_dir = env.get('staging_dir')

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
        source_info, errors = get_source_info(services, source_dir, staging_dir, default_service_properties)
        if len(errors) > 0:
            message = 'One or more errors occurred while validating the {} environment for service folder {}:\n{}' \
                .format(env_name, service_folder, '\n'.join(errors))
            if warn_on_validation_errors:
                log.warn(message)
            else:
                raise RuntimeError(message)
        for service_name, service_type, service_properties in normalize_services(services, default_service_properties):
            if copy_source_files_from_staging_folder:
                service_info = source_info[service_name]
                if service_type == 'MapServer':
                    source_mxd_path = service_info['source_file']
                    if not source_mxd_path:
                        source_mxd_path = os.path.join(source_dir, service_name + '.mxd')
                    if staging_dir:
                        staging_mxd_path = service_info['staging_files'][0]
                        log.info('Copying staging MXD {} to {}'.format(staging_mxd_path, source_mxd_path))
                        if not os.path.isdir(source_dir):
                            log.warn('Creating source directory {}'.format(source_dir))
                            os.makedirs(source_dir)
                        copyfile(staging_mxd_path, source_mxd_path)
                    if not os.path.isfile(source_mxd_path):
                        raise RuntimeError('Source MXD {} does not exist!'.format(source_mxd_path))
                    if data_source_mappings:
                        proc = multiprocessing.Process(
                            target=logged_call,
                            args=(
                                log_queue,
                                update_data_sources,
                                source_mxd_path,
                                data_source_mappings
                            )
                        )
                        proc.start()
                        proc.join()
                        if proc.exitcode != 0:
                            raise RuntimeError(
                                'An error occurred in subprocess {} (pid {}) while updating data sources for MXD {}'
                                .format(proc.name, proc.pid, source_mxd_path)
                            )
                        del proc
                if service_type == 'GeocodeServer':
                    source_locator_path = service_info['source_file']
                    if staging_dir:
                        staging_locator_path = service_info['staging_files'][0]
                        log.info('Copying staging locator file {} to {}'.format(staging_locator_path, source_locator_path))
                        if not os.path.isdir(source_dir):
                            log.warn('Creating source directory {}'.format(source_dir))
                            os.makedirs(source_dir)
                        copyfile(staging_locator_path, source_locator_path)
                        copyfile(staging_locator_path + '.xml', source_locator_path + '.xml')
                        staging_locator_lox_path = os.path.splitext(staging_locator_path)[0] + '.lox'
                        if os.path.isfile(staging_locator_lox_path):
                            copyfile(staging_locator_lox_path, os.path.splitext(source_locator_path)[0] + '.lox')
                    if not os.path.isfile(source_locator_path):
                        raise RuntimeError('Source locator file {} does not exist!'.format(source_locator_path))
                    if data_source_mappings:
                        log.warn(
                            'Data source mappings specified but are not supported with GeocodeServer services, skipping '
                            'service {}.'
                            .format(service_name)
                        )
            else:
                log.debug('Will skip copying source files from staging folder.')
            procs = list()
            for ags_instance in ags_instances:
                ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
                ags_connection = ags_instance_props['ags_connection']
                proc = (
                    multiprocessing.Process(
                        target=logged_call,
                        args=(
                            log_queue,
                            publish_service,
                            service_name,
                            service_type,
                            source_dir,
                            ags_instance,
                            ags_connection,
                            service_folder,
                            service_properties,
                            service_prefix,
                            service_suffix
                        )
                    ),
                    ags_instance
                )
                proc[0].start()
                procs.append(proc)
            for proc, ags_instance in procs:
                proc.join()

            errors = list()
            for proc, ags_instance in procs:
                if proc.exitcode != 0:
                    errors.append(
                        'An error occurred in subprocess {} (pid {}, exitcode {}) '
                        'while publishing service {}/{} to AGS instance {}'
                        .format(
                            proc.name,
                            proc.pid,
                            proc.exitcode,
                            service_folder,
                            service_name,
                            ags_instance
                        )
                    )
            if len(errors) > 0:
                log.error(
                    'One or more errors occurred while publishing service {}/{}, aborting.'
                    .format(service_folder, service_name)
                )
                raise RuntimeError(errors)

    if cleanup_services:
        for ags_instance in ags_instances:
            cleanup_instance(ags_instance, env_name, config, user_config)


def publish_service(
    service_name,
    service_type,
    source_dir,
    ags_instance,
    ags_connection,
    service_folder=None,
    service_properties=None,
    service_prefix='',
    service_suffix=''
):
    import arcpy
    arcpy.env.overwriteOutput = True

    original_service_name = service_name
    service_name = '{}{}{}'.format(service_prefix, service_name, service_suffix)

    log.info(
        'Publishing {} service {} to ArcGIS Server instance {}, Connection File: {}, Service Folder: {}'
        .format(service_type, service_name, ags_instance, ags_connection, service_folder)
    )

    tempdir = tempfile.mkdtemp()
    log.debug('Temporary directory created: {}'.format(tempdir))
    try:
        sddraft = os.path.join(tempdir, service_name + '.sddraft')
        sd = os.path.join(tempdir, service_name + '.sd')
        log.debug('Creating SDDraft file: {}'.format(sddraft))

        if service_type == 'MapServer':
            mxd_path = os.path.join(source_dir, original_service_name + '.mxd')
            mxd = arcpy.mapping.MapDocument(mxd_path)
            arcpy.mapping.CreateMapSDDraft(
                mxd,
                sddraft,
                service_name,
                'FROM_CONNECTION_FILE',
                ags_connection,
                False,
                service_folder
            )
            modify_sddraft(sddraft, service_properties)
            log.debug('Analyzing SDDraft file: {}'.format(sddraft))
            analysis = arcpy.mapping.AnalyzeForSD(sddraft)

        elif service_type == 'GeocodeServer':
            locator_path = os.path.join(source_dir, original_service_name)
            if service_properties.get('rebuild_locators'):
                log.info('Rebuilding locator {}'.format(locator_path))
                arcpy.RebuildAddressLocator_geocoding(locator_path)
            analysis = arcpy.CreateGeocodeSDDraft(
                locator_path,
                sddraft,
                service_name,
                'FROM_CONNECTION_FILE',
                ags_connection,
                False,
                service_folder
            )
            modify_sddraft(sddraft, service_properties)

        else:
            raise RuntimeError('Unsupported service type {}!'.format(service_type))

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
            log.info(
                'Service {}/{} successfully published to {} at {:%#m/%#d/%y %#I:%M:%S %p}'
                .format(service_folder, service_name, ags_instance, datetime.datetime.now())
            )
        else:
            error_message = 'Analysis failed for service {}/{} at {:%#m/%#d/%y %#I:%M:%S %p}' \
                .format(service_folder, service_name, datetime.datetime.now())
            log.error(error_message)
            raise RuntimeError(error_message, analysis['errors'])
    except:
        log.exception(
            'An error occurred while publishing service {}/{} to ArcGIS Server instance {}'
            .format(service_folder, service_name, ags_instance)
        )
        raise
    finally:
        log.debug('Cleaning up temporary directory: {}'.format(tempdir))
        rmtree(tempdir, ignore_errors=True)


def cleanup_config(
    config,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    config_dir=default_config_dir
):
    env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No cleanable environments specified!')

    log.info('Cleaning environments: {}'.format(', '.join(env_names)))
    for env_name in env_names:
        cleanup_env(config, env_name, included_instances, excluded_instances, config_dir)


def cleanup_env(
    config,
    env_name,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    config_dir=default_config_dir
):
    env = config['environments'][env_name]
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    if len(ags_instances) == 0:
        raise RuntimeError('No cleanable instances specified!')
    user_config = get_config('userconfig', config_dir)
    for ags_instance in ags_instances:
        cleanup_instance(ags_instance, env_name, config, user_config)


def cleanup_instance(
    ags_instance,
    env_name,
    config,
    user_config
):
    configured_services = config['services']
    service_folder = config.get('service_folder')
    log.info(
        'Cleaning up unused services on environment {}, ArcGIS Server instance {}, service folder {}'
        .format(env_name, ags_instance, service_folder)
    )
    ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
    server_url = ags_instance_props['url']
    token = ags_instance_props['token']
    existing_services = list_services(server_url, token, service_folder)
    services_to_remove = [service for service in existing_services if service['serviceName'] not in configured_services]
    log.info(
        'Removing {} services: {}'
        .format(
            len(services_to_remove),
            ', '.join((service['serviceName'] for service in services_to_remove))
        )
    )
    for service in services_to_remove:
        delete_service(server_url, token, service['serviceName'], service_folder, service['type'])


def find_dataset_usages(
    included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
    included_users=asterisk_tuple, excluded_users=empty_tuple,
    included_databases=asterisk_tuple, excluded_databases=empty_tuple,
    included_versions=asterisk_tuple, excluded_versions=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    config_dir=default_config_dir
):
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
                        for (
                            dataset_path,
                            user,
                            database,
                            version
                        ) in list_service_workspaces(
                            server_url,
                            token,
                            service_name,
                            service_folder,
                            service_type
                        ):
                            dataset_name = os.path.basename(dataset_path)
                            if (
                                superfilter((dataset_name,), included_datasets, excluded_datasets) and
                                superfilter((user,), included_users, excluded_users) and
                                superfilter((database,), included_databases, excluded_databases) and
                                superfilter((version,), included_versions, excluded_versions)
                            ):
                                yield (
                                    ags_instance,
                                    service_folder,
                                    service_name,
                                    service_type,
                                    dataset_name,
                                    user,
                                    database,
                                    version,
                                    dataset_path
                                )


def restart_services(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    include_running_services=True,
    config_dir=default_config_dir
):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Restarting services on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
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
                        if not include_running_services:
                            status = get_service_status(server_url, token, service_name, service_folder, service_type)
                            if status.get('configuredState') == 'STARTED':
                                pass
                            restart_service(server_url, token, service_name, service_folder, service_type)
                        restart_service(server_url, token, service_name, service_folder, service_type)


def test_services(
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_service_folders=asterisk_tuple, excluded_service_folders=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    warn_on_errors=False,
    config_dir=default_config_dir
):
    user_config = get_config('userconfig', config_dir)
    env_names = superfilter(user_config['environments'].keys(), included_envs, excluded_envs)
    if len(env_names) == 0:
        raise RuntimeError('No environments specified!')
    for env_name in env_names:
        env = user_config['environments'][env_name]
        ags_instances = superfilter(env['ags_instances'].keys(), included_instances, excluded_instances)
        log.info('Testing services on ArcGIS Server instances {}'.format(', '.join(ags_instances)))
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
                        try:
                            test_service(server_url, token, service_name, service_folder, service_type)
                        except StandardError as e:
                            if not warn_on_errors:
                                raise
                            log.warn(e.message)


def find_mxd_data_sources(
    included_configs=asterisk_tuple, excluded_configs=empty_tuple,
    included_users=asterisk_tuple, excluded_users=empty_tuple,
    included_databases=asterisk_tuple, excluded_databases=empty_tuple,
    included_versions=asterisk_tuple, excluded_versions=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_datasets=asterisk_tuple, excluded_datasets=empty_tuple,
    include_staging_mxds=True,
    warn_on_validation_errors=False,
    config_dir=default_config_dir
):
    for config_name, config in get_configs(included_configs, excluded_configs, config_dir).iteritems():
        env_names = superfilter(config['environments'].keys(), included_envs, excluded_envs)
        services = superfilter(config['services'], included_services, excluded_services)
        default_service_properties = config.get('default_service_properties')

        if not default_service_properties:
            log.debug('No default service properties specified')

        for env_name in env_names:
            log.debug('Finding MXD data sources for config {}, environment {}'.format(config_name, env_name))
            env = config['environments'][env_name]
            staging_dir = env.get('staging_dir')
            source_dir = env['source_dir']
            source_info, errors = get_source_info(services, source_dir, staging_dir, default_service_properties)
            if len(errors) > 0:
                message = 'One or more errors occurred while validating the {} environment for config name {}:\n{}' \
                          .format(env_name, config_name, '\n'.join(errors))
                if warn_on_validation_errors:
                    log.warn(message)
                else:
                    raise RuntimeError(message)
            for service_name, service_type, service_properties in normalize_services(services, default_service_properties):
                if service_type == 'MapServer':
                    def generate_mxd_data_sources_report_rows(mxd_path, mxd_type):
                        for (
                            layer_name,
                            dataset_name,
                            workspace_path,
                            user,
                            database,
                            version,
                            definition_query
                        ) in get_data_sources(mxd_path):
                            if (
                                superfilter((dataset_name,), included_datasets, excluded_datasets) and
                                superfilter((user,), included_users, excluded_users) and
                                superfilter((database,), included_databases, excluded_databases) and
                                superfilter((version,), included_versions, excluded_versions)
                            ):
                                yield (
                                    config_name,
                                    env_name,
                                    service_name,
                                    mxd_path,
                                    mxd_type,
                                    layer_name,
                                    dataset_name,
                                    user,
                                    database,
                                    version,
                                    workspace_path,
                                    definition_query
                                )

                    if include_staging_mxds:
                        for staging_mxd_path in source_info[service_name]['staging_files']:
                            for row in generate_mxd_data_sources_report_rows(staging_mxd_path, 'staging'):
                                yield row
                    source_mxd_path = source_info[service_name]['source_file']
                    if source_mxd_path:
                        for row in generate_mxd_data_sources_report_rows(source_mxd_path, 'source'):
                            yield row
                    else:
                        log.warn('No source MXD found for service {}/{} in the {} environment!'.format(config_name, service_name, env_name))
                else:
                    log.debug(
                        'Unsupported service type {} of service {} will be skipped'
                        .format(service_type, service_name)
                    )


def get_source_info(services, source_dir, staging_dir, default_service_properties):
    log.debug(
        'Getting source info for services {}, source directory: {}, staging directory {}'
        .format(services, source_dir, staging_dir)
    )

    source_info = {}
    errors = []

    for service_name, service_type, service_properties in normalize_services(services, default_service_properties):
        service_info = source_info[service_name] = {
            'source_file': None,
            'staging_files': []
        }
        if staging_dir:
            staging_files = service_info['staging_files']
            # If multiple staging folders are provided, look for the source item in each staging folder
            staging_dirs = (staging_dir,) if isinstance(staging_dir, basestring) else staging_dir
            for _staging_dir in staging_dirs:
                log.debug('Finding staging items in directory: {}'.format(_staging_dir))
                if service_type == 'MapServer':
                    staging_file = os.path.abspath(os.path.join(_staging_dir, service_name + '.mxd'))
                elif service_type == 'GeocodeServer':
                    staging_file = os.path.abspath(os.path.join(_staging_dir, service_name + '.loc'))
                else:
                    log.debug('Unsupported service type {} of service {} will be skipped'.format(service_type, service_name))

                if os.path.isfile(staging_file):
                    log.debug('Staging file found: {}'.format(staging_file))
                    staging_files.append(staging_file)
                else:
                    log.debug('Staging file missing: {}'.format(staging_file))

            if len(staging_files) == 0:
                errors.append('- No staging file found for service {}'.format(service_name))
            elif len(staging_files) > 1:
                errors.append(
                    '- More than one staging file found for service {}: \n{}'
                    .format(
                        service_name,
                        '\n'.join('  - {}'.format(staging_file) for staging_file in staging_files)
                    )
                )

        if source_dir:
            log.debug('Finding source files in directory: {}'.format(source_dir))
            if service_type == 'MapServer':
                source_file = os.path.abspath(os.path.join(source_dir, service_name + '.mxd'))
            elif service_type == 'GeocodeServer':
                source_file = os.path.abspath(os.path.join(source_dir, service_name + '.loc'))
            else:
                log.debug('Unsupported service type {} of service {} will be skipped'.format(service_type, service_name))
            if os.path.isfile(source_file):
                log.debug('Source file found: {}'.format(source_file))
                service_info['source_file'] = source_file
            else:
                log.debug('Source file missing: {}'.format(source_file))
                errors.append('- Source file {} for service {} does not exist!'.format(source_file, service_name))

    return source_info, errors


def normalize_services(services, default_service_properties):
    for service in services:
        yield normalize_service(service, default_service_properties)


def normalize_service(service, default_service_properties):
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
                'No service-level properties specified for service {} '
                'even though it was specified as a mapping'
                    .format(service_name)
            )
    else:
        log.debug('No service-level properties specified for service {}'.format(service_name))
    service_type = merged_service_properties.get('service_type', 'MapServer')
    return service_name, service_type, merged_service_properties
