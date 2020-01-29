from __future__ import unicode_literals

import datetime
import getpass
import multiprocessing
import os
import tempfile
from shutil import copyfile, rmtree

from ags_utils import list_services, delete_service, get_site_mode, set_site_mode, create_session, get_service_item_info, set_service_item_info
from config_io import get_config, default_config_dir
from datasources import update_data_sources, open_mxd
from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger
from mplog import open_queue, logged_call
from sddraft_io import modify_sddraft
from services import normalize_services, get_source_info

log = setup_logger(__name__)


def publish_config(
    config,
    config_dir=default_config_dir,
    included_envs=asterisk_tuple, excluded_envs=empty_tuple,
    included_instances=asterisk_tuple, excluded_instances=empty_tuple,
    included_services=asterisk_tuple, excluded_services=empty_tuple,
    copy_source_files_from_staging_folder=True,
    cleanup_services=False,
    service_prefix='',
    service_suffix='',
    warn_on_publishing_errors=False,
    warn_on_validation_errors=False,
    create_backups=True,
    update_timestamps=True
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
            for result in publish_env(
                config,
                env_name,
                user_config,
                included_instances, excluded_instances,
                included_services, excluded_services,
                copy_source_files_from_staging_folder,
                cleanup_services,
                service_prefix,
                service_suffix,
                warn_on_publishing_errors,
                warn_on_validation_errors,
                create_backups,
                update_timestamps
            ):
                yield result
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
    warn_on_publishing_errors=False,
    warn_on_validation_errors=False,
    create_backups=True,
    update_timestamps=True
):
    config = get_config(config_name, config_dir)
    log.info('Publishing config \'{}\''.format(config_name))
    for result in publish_config(
        config,
        config_dir,
        included_envs, excluded_envs,
        included_instances, excluded_instances,
        included_services, excluded_services,
        copy_source_files_from_staging_folder,
        cleanup_services,
        service_prefix,
        service_suffix,
        warn_on_publishing_errors,
        warn_on_validation_errors,
        create_backups,
        update_timestamps
    ):
        result['config_name'] = config_name
        yield result


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
    warn_on_publishing_errors=False,
    warn_on_validation_errors=False,
    create_backups=True,
    update_timestamps=True
):
    env = config['environments'][env_name]
    source_dir = env['source_dir']
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    services = superfilter(config['services'], included_services, excluded_services)
    service_folder = config.get('service_folder', os.path.basename(source_dir))
    default_service_properties = config.get('default_service_properties')
    env_service_properties = env.get('service_properties', {})
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

    source_info, errors = get_source_info(
        services,
        source_dir,
        staging_dir,
        default_service_properties,
        env_service_properties
    )
    if len(errors) > 0:
        message = 'One or more errors occurred while validating the {} environment for service folder {}:\n{}' \
            .format(env_name, service_folder, '\n'.join(errors))
        if warn_on_validation_errors:
            log.warn(message)
        else:
            raise RuntimeError(message)

    initial_site_modes = get_site_modes(ags_instances, env_name, user_config)
    make_sites_editable(ags_instances, env_name, user_config, initial_site_modes)

    try:
        for result in publish_services(
            services,
            user_config,
            ags_instances,
            env_name,
            default_service_properties,
            env_service_properties,
            source_info,
            source_dir,
            staging_dir,
            data_source_mappings,
            service_folder,
            copy_source_files_from_staging_folder,
            service_prefix,
            service_suffix,
            warn_on_publishing_errors,
            create_backups,
            update_timestamps
        ):
            yield result
    finally:
        restore_site_modes(ags_instances, env_name, user_config, initial_site_modes)

    if cleanup_services:
        for ags_instance in ags_instances:
            cleanup_instance(ags_instance, env_name, config, user_config)


def get_site_modes(ags_instances, env_name, user_config):
    result = {}
    for ags_instance in ags_instances:
        ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
        site_mode = ags_instance_props.get('site_mode')
        if site_mode:
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                current_site_mode = get_site_mode(server_url, token, session=session)
                result[ags_instance] = current_site_mode
    return result


def make_sites_editable(ags_instances, env_name, user_config, initial_site_modes):
    for ags_instance in ags_instances:
        ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
        site_mode = ags_instance_props.get('site_mode')
        if site_mode:
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            if initial_site_modes[ags_instance] != 'EDITABLE':
                with create_session(server_url, proxies=proxies) as session:
                    set_site_mode(server_url, token, 'EDITABLE', session=session)


def restore_site_modes(ags_instances, env_name, user_config, initial_site_modes):
    for ags_instance in ags_instances:
        ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
        site_mode = ags_instance_props.get('site_mode')
        if site_mode:
            server_url = ags_instance_props['url']
            token = ags_instance_props['token']
            proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
            with create_session(server_url, proxies=proxies) as session:
                current_site_mode = get_site_mode(server_url, token, session=session)
                if site_mode.upper() == 'INITIAL':
                    if current_site_mode != initial_site_modes[ags_instance]:
                        set_site_mode(server_url, token, initial_site_modes[ags_instance], session=session)
                elif site_mode.upper() == 'READ_ONLY':
                    if current_site_mode != 'READ_ONLY':
                        set_site_mode(server_url, token, 'READ_ONLY', session=session)
                elif site_mode.upper() == 'EDITABLE':
                    if current_site_mode != 'EDITABLE':
                        set_site_mode(server_url, token, 'EDITABLE', session=session)
                else:
                    log.warn('Unrecognized site mode {}'.format(site_mode))


def publish_services(
    services,
    user_config,
    ags_instances,
    env_name,
    default_service_properties,
    env_service_properties,
    source_info,
    source_dir,
    staging_dir,
    data_source_mappings,
    service_folder,
    copy_source_files_from_staging_folder=True,
    service_prefix='',
    service_suffix='',
    warn_on_publishing_errors=False,
    create_backups=True,
    update_timestamps=True
):
    for (
        service_name,
        service_type,
        service_properties
    ) in normalize_services(
        services,
        default_service_properties,
        env_service_properties
    ):
        service_info = source_info[service_name]
        file_path = service_info['source_file']
        with open_queue() as log_queue:
            if create_backups:
                backup_dir = os.path.join(source_dir, 'Backup')
                if not os.path.isdir(backup_dir):
                    log.warn('Creating backup directory {}'.format(backup_dir))
                    os.makedirs(backup_dir)
                if service_type == 'MapServer':
                    source_mxd_path = file_path
                    if not source_mxd_path:
                        file_path = source_mxd_path = os.path.join(source_dir, service_name + '.mxd')
                    backup_file_name = '{}_{:%Y%m%d_%H%M%S}.mxd'.format(service_name, datetime.datetime.now())
                    backup_file_path = os.path.join(backup_dir, backup_file_name)
                    log.info('Backing up source MXD {} to {}'.format(source_mxd_path, backup_file_path))
                    copyfile(source_mxd_path, backup_file_path)
                if service_type == 'GeocodeServer':
                    source_locator_path = file_path
                    backup_file_name = '{}_{:%Y%m%d_%H%M%S}.loc'.format(service_name, datetime.datetime.now())
                    backup_file_path = os.path.join(backup_dir, backup_file_name)
                    log.info('Backing up source locator file {} to {}'.format(source_locator_path, backup_file_path))
                    copyfile(source_locator_path, backup_file_path)
                    copyfile(source_locator_path + '.xml', backup_file_path + '.xml')
                    source_locator_lox_path = os.path.splitext(source_locator_path)[0] + '.lox'
                    if os.path.isfile(source_locator_lox_path):
                        copyfile(source_locator_lox_path, os.path.splitext(backup_file_path)[0] + '.lox')
            if copy_source_files_from_staging_folder:
                if service_type == 'MapServer':
                    source_mxd_path = file_path
                    if not source_mxd_path:
                        file_path = source_mxd_path = os.path.join(source_dir, service_name + '.mxd')
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
                    source_locator_path = file_path
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
                error_message = None
                timestamp = datetime.datetime.now()
                if proc.exitcode != 0:
                    succeeded = False
                    error_message = 'An error occurred in subprocess {} (pid {}, exitcode {}) ' \
                        'while publishing service {}/{} to AGS instance {}' \
                        .format(
                            proc.name,
                            proc.pid,
                            proc.exitcode,
                            service_folder,
                            service_name,
                            ags_instance
                        )
                    errors.append(error_message)
                else:
                    succeeded = True
                    if update_timestamps:
                        set_publishing_summary(
                            user_config,
                            env_name,
                            ags_instance,
                            service_name,
                            service_folder,
                            service_type,
                            timestamp
                        )
                yield dict(
                    env_name=env_name,
                    ags_instance=ags_instance,
                    service_folder=service_folder,
                    service_name=service_name,
                    service_type=service_type,
                    file_path=file_path,
                    succeeded=succeeded,
                    error=error_message,
                    timestamp=timestamp
                )
            if len(errors) > 0 and not warn_on_publishing_errors:
                log.error(
                    'One or more errors occurred while publishing service {}/{}, aborting.'
                    .format(service_folder, service_name)
                )
                raise RuntimeError(errors)


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
            mxd = open_mxd(mxd_path)
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
                    log_method('           {}'.format(layer.longName if hasattr(layer, 'longName') else layer.name))
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
    except StandardError:
        log.exception(
            'An error occurred while publishing service {}/{} to ArcGIS Server instance {}'
            .format(service_folder, service_name, ags_instance)
        )
        raise
    finally:
        log.debug('Cleaning up temporary directory: {}'.format(tempdir))
        rmtree(tempdir, ignore_errors=True)


def set_publishing_summary(
    user_config,
    env_name,
    ags_instance,
    service_name,
    service_folder,
    service_type,
    timestamp
):
    try:
        ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
        server_url = ags_instance_props['url']
        token = ags_instance_props['token']
        proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
        with create_session(server_url, proxies=proxies) as session:
            item_info = get_service_item_info(
                server_url,
                token,
                service_name,
                service_folder,
                service_type,
                session=session
            )
            item_info['summary'] = 'Last published by {} on {:%#m/%#d/%y at %#I:%M:%S %p}'.format(
                getpass.getuser(),
                timestamp
            )
            set_service_item_info(
                server_url,
                token,
                item_info,
                service_name,
                service_folder,
                service_type,
                session=session
            )
    except StandardError:
        log.warning(
            'An error occurred while updating timestamp for service {}/{} to ArcGIS Server instance {}'
            .format(
                service_folder,
                service_name,
                ags_instance
            ),
            exc_info=True
        )


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
    proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
    with create_session(server_url, proxies=proxies) as session:
        existing_services = list_services(server_url, token, service_folder, session=session)
        services_to_remove = [service for service in existing_services if service['serviceName'] not in configured_services]
        log.info(
            'Removing {} services: {}'
            .format(
                len(services_to_remove),
                ', '.join((service['serviceName'] for service in services_to_remove))
            )
        )
        for service in services_to_remove:
            delete_service(server_url, token, service['serviceName'], service_folder, service['type'], session=session)
