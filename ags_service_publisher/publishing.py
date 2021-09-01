import datetime
import getpass
import multiprocessing
import tempfile
from pathlib import Path
from shutil import copyfile, rmtree

from .ags_utils import (
    analyze_staging_result,
    create_session,
    delete_service,
    get_site_mode,
    list_services,
    set_site_mode,
    get_service_item_info,
    set_service_item_info
)
from .config_io import get_config, default_config_dir
from .datasources import get_layer_properties, update_data_sources, convert_mxd_to_aprx, open_aprx
from .extrafilters import superfilter
from .helpers import asterisk_tuple, deep_get, empty_tuple
from .logging_io import setup_logger
from .mplog import open_queue, logged_call
from .sddraft_io import modify_sddraft
from .services import normalize_services, get_source_info

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

    log.info(f'Publishing environments: {", ".join(env_names)}')
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
            log.warn(f'No publishable instances specified for environment {env_name}')


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
    log.info(f'Publishing config {config_name}')
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
    source_dir = Path(env.get('source_dir')) if env.get('source_dir') else None
    ags_instances = superfilter(env['ags_instances'], included_instances, excluded_instances)
    services = superfilter(config['services'], included_services, excluded_services)
    service_folder = config.get('service_folder', source_dir.name if source_dir else None)
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
        f'Publishing environment: {env_name}, service folder: {service_folder}, '
        f'ArcGIS Server instances: {", ".join(ags_instances)}'
    )

    source_info, errors = get_source_info(
        services,
        source_dir,
        staging_dir,
        default_service_properties,
        env_service_properties
    )
    if len(errors) > 0:
        message = (
            f'One or more errors occurred while validating the {env_name} environment '
            f'for service folder {service_folder}:\n'
            f'{chr(10).join(errors)}'
        )
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
                    log.warn(f'Unrecognized site mode {site_mode}')


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
    source_dir = Path(source_dir) if source_dir else None
    for (
        service_name,
        service_type,
        service_properties
    ) in normalize_services(
        services,
        default_service_properties,
        env_service_properties
    ):
        log.debug(f'Publishing {service_type} service {service_name} to environment {env_name}')
        service_info = source_info[service_name]
        file_path = service_info['source_file']
        with open_queue() as log_queue:
            if create_backups and source_dir:
                backup_dir = source_dir / 'Backup'
                if not backup_dir.is_dir():
                    log.warn(f'Creating backup directory {backup_dir}')
                    backup_dir.mkdir(parents=True)
                timestamp = datetime.datetime.now()
                if service_type in ('MapServer', 'ImageServer'):
                    source_file_path = Path(file_path)
                    backup_file_path = backup_dir / f'{service_name}_{timestamp:%Y%m%d_%H%M%S}{source_file_path.suffix}'
                    log.info(f'Backing up source file {source_file_path} to {backup_file_path}')
                    copyfile(source_file_path, backup_file_path)
                if service_type == 'GeocodeServer':
                    source_locator_path = Path(file_path)
                    backup_file_path = backup_dir / f'{service_name}_{timestamp:%Y%m%d_%H%M%S}.loc'
                    log.info(f'Backing up source locator file {source_locator_path} to {backup_file_path}')
                    copyfile(source_locator_path, backup_file_path)
                    source_locator_xml_path = source_locator_path.parent / f'{source_locator_path}.xml'
                    if source_locator_xml_path.is_file():
                        copyfile(source_locator_xml_path, f'{backup_file_path}.xml')
                    source_locator_lox_path = source_locator_path.parent / f'{source_locator_path.stem}.lox'
                    if source_locator_lox_path.is_file():
                        copyfile(source_locator_lox_path, backup_file_path.parent / f'{backup_file_path.stem}.lox')
                    source_locator_loz_path = source_locator_path.parent / f'{source_locator_path.stem}.loz'
                    if source_locator_loz_path.is_file():
                        copyfile(source_locator_loz_path, backup_file_path.parent / f'{backup_file_path.stem}.loz')
            if copy_source_files_from_staging_folder:
                if service_type in ('MapServer', 'ImageServer'):
                    source_file_path = Path(file_path)
                    if source_file_path.suffix.lower() == '.mxd':
                        source_file_path = source_file_path.parent / f'{source_file_path.stem}.aprx'
                    if staging_dir:
                        staging_file_path = Path(service_info['staging_files'][0])
                        log.info(f'Copying staging file {staging_file_path} to {source_file_path}')
                        if source_dir and not source_dir.is_dir():
                            log.warn(f'Creating source directory {source_dir}')
                            source_dir.mkdir(parents=True)
                        if staging_file_path.suffix.lower() == '.mxd':
                            source_file_path = source_file_path.parent / f'{source_file_path.stem}.aprx'
                            convert_mxd_to_aprx(staging_file_path, source_file_path)
                        else:
                            copyfile(staging_file_path, source_file_path)
                    if not source_file_path.is_file():
                        raise RuntimeError(f'Source file {source_file_path} does not exist!')
                    if data_source_mappings:
                        proc = multiprocessing.Process(
                            target=logged_call,
                            args=(
                                log_queue,
                                update_data_sources,
                                source_file_path,
                                data_source_mappings
                            )
                        )
                        proc.start()
                        proc.join()
                        if proc.exitcode != 0:
                            raise RuntimeError(
                                f'An error occurred in subprocess {proc.name} (pid {proc.pid}) '
                                f'while updating data sources for file {source_file_path}'
                            )
                        del proc
                if service_type == 'GeocodeServer':
                    source_locator_path = Path(file_path)
                    if staging_dir:
                        staging_locator_path = Path(service_info['staging_files'][0])
                        log.info(f'Copying staging locator file {staging_locator_path} to {source_locator_path}')
                        if source_dir and not source_dir.is_dir():
                            log.warn(f'Creating source directory {source_dir}')
                            source_dir.mkdir(parents=True)
                        copyfile(staging_locator_path, source_locator_path)
                        staging_locator_xml_path = staging_locator_path.parent / f'{staging_locator_path}.xml'
                        if staging_locator_xml_path.is_file():
                            copyfile(staging_locator_xml_path, f'{source_locator_path}.xml')
                        staging_locator_lox_path = staging_locator_path.parent / f'{staging_locator_path.stem}.lox'
                        if staging_locator_lox_path.is_file():
                            copyfile(staging_locator_lox_path, source_locator_path.parent / f'{source_locator_path.stem}.lox')
                        staging_locator_loz_path = staging_locator_path.parent / f'{staging_locator_path.stem}.loz'
                        if staging_locator_loz_path.is_file():
                            copyfile(staging_locator_loz_path, source_locator_path.parent / f'{source_locator_path.stem}.loz')
                    if not source_locator_path.is_file():
                        raise RuntimeError(f'Source locator file {source_locator_path} does not exist!')
                    if data_source_mappings:
                        log.warn(
                            f'Data source mappings specified but are not supported with GeocodeServer services, skipping '
                            f'service {service_name}.'
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
                    error_message = (
                        f'An error occurred in subprocess {proc.name} (pid {proc.pid}, exitcode {proc.exitcode}) '
                        f'while publishing service {service_folder}/{service_name} to AGS instance {ags_instance}'
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
                    f'One or more errors occurred while publishing service {service_folder}/{service_name}, aborting.'
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
    log.debug('Importing arcpy...')
    try:
        import arcpy
    except Exception:
        log.exception('An error occurred importing arcpy')
        raise
    log.debug('Successfully imported arcpy')

    arcpy.env.overwriteOutput = True

    original_service_name = service_name
    service_name = f'{service_prefix}{service_name}{service_suffix}'

    log.info(
        f'Publishing {service_type} service {service_name} to ArcGIS Server instance {ags_instance}, '
        f'Connection File: {ags_connection}, Service Folder: {service_folder}'
    )

    tempdir = Path(tempfile.mkdtemp())
    log.debug(f'Temporary directory created: {tempdir}')
    try:
        sddraft = tempdir / f'{service_name}.sddraft'
        sd = tempdir / f'{service_name}.sd'
        if service_type in ('MapServer', 'ImageServer'):
            file_path = Path(source_dir) / f'{original_service_name}.aprx'
            if not file_path.exists():
                file_path = Path(source_dir) / f'{original_service_name}.mxd'
                if not file_path.exists():
                    raise RuntimeError(f'No MXD or ArcGIS Pro project file found for service {service_name} in {source_dir}')
            if file_path.suffix.lower() == '.aprx':
                aprx = open_aprx(file_path)
            elif file_path.suffix.lower() == '.mxd':
                temp_aprx_path = tempdir / f'{service_name}.aprx'
                convert_mxd_to_aprx(file_path, temp_aprx_path)
                aprx = open_aprx(temp_aprx_path)
            else:
                raise RuntimeError(f'Unrecognized file type for {file_path}')
            map_ = aprx.listMaps()[0]
            if service_type == 'MapServer':
                map_service_draft = arcpy.sharing.CreateSharingDraft(
                    server_type='STANDALONE_SERVER',
                    service_type='MAP_SERVICE',
                    service_name=service_name,
                    draft_value=map_
                )
                map_service_draft.targetServer = ags_connection
                map_service_draft.serverFolder = service_folder
                log.debug(f'Creating SDDraft file: {sddraft}')
                map_service_draft.exportToSDDraft(str(sddraft))
                modify_sddraft(sddraft, service_properties)
                log.debug(f'Staging SDDraft file: {sddraft} to SD file: {sd}')
                result = arcpy.StageService_server(str(sddraft), str(sd))
                analysis = analyze_staging_result(result)
            elif service_type == 'ImageServer':
                dataset_path = None
                for layer in map_.listLayers():
                    desc = arcpy.Describe(layer)
                    if desc.dataType in ('MosaicLayer', 'RasterLayer'):
                        dataset_path = desc.catalogPath
                        layer_props = get_layer_properties(layer)
                        layer_name = layer_props.get('layer_name')
                        dataset_name = layer_props.get('dataset_name')
                        current_database = layer_props.get('database')
                        log.debug(f'Using layer {layer_name}, dataset name: {dataset_name}, database: {current_database}, dataset path: {dataset_path}) as image service data source')
                        break
                else:
                    raise RuntimeError(f'No supported mosaic or raster layers found in source document {file_path}!')
                log.debug(f'Creating SDDraft file: {sddraft}')
                analysis = arcpy.CreateImageSDDraft(
                    str(dataset_path),
                    str(sddraft),
                    service_name,
                    folder_name=service_folder,
                )
                modify_sddraft(sddraft, service_properties)
        elif service_type == 'GeocodeServer':
            locator_path = source_dir / original_service_name
            if service_properties.get('rebuild_locators'):
                log.info(f'Rebuilding locator {locator_path}')
                arcpy.RebuildAddressLocator_geocoding(f'{locator_path}.loc')
            analysis = arcpy.CreateGeocodeSDDraft(
                str(locator_path),
                str(sddraft),
                service_name,
                folder_name=service_folder,
            )
            modify_sddraft(sddraft, service_properties)
        else:
            raise RuntimeError(f'Unsupported service type {service_type}!')
        for key, log_method in (('warnings', log.warn), ('errors', log.error)):
            log.info('----' + key.upper() + '---')
            items = analysis[key]
            for ((message, code), layerlist) in items.items():
                log_method(f'    {message} (CODE {code:05d})')
                if layerlist:
                    log_method('       applies to:')
                    for layer in layerlist:
                        if layer:
                            log_method(f'           {layer if isinstance(layer, str) else deep_get(layer, "longName", layer.name)}')
        if analysis['errors'] == {}:
            if not sd.is_file():
                log.debug(f'Staging SDDraft file: {sddraft} to SD file: {sd}')
                arcpy.StageService_server(str(sddraft), str(sd))
            log.debug(f'Uploading SD file: {sd} to AGS connection: {ags_connection}')
            arcpy.UploadServiceDefinition_server(str(sd), ags_connection)
            log.info(
                f'Service {service_folder}/{service_name} successfully published to '
                f'{ags_instance} at {datetime.datetime.now():%#m/%#d/%y %#I:%M:%S %p}'
            )
        else:
            error_message = (
                f'Analysis failed for service {service_folder}/{service_name} '
                f'at {datetime.datetime.now():%#m/%#d/%y %#I:%M:%S %p}'
            )
            log.error(error_message)
            raise RuntimeError(error_message, analysis['errors'])
    except Exception:
        log.exception(
            f'An error occurred while publishing service {service_folder}/{service_name} '
            f'to ArcGIS Server instance {ags_instance}'
        )
        raise
    finally:
        log.debug(f'Cleaning up temporary directory: {tempdir}')
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
    except Exception:
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

    log.info(f'Cleaning environments: {", ".join(env_names)}')
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
        f'Cleaning up unused services on environment {env_name}, ArcGIS Server instance {ags_instance}, service folder {service_folder}'
    )
    ags_instance_props = user_config['environments'][env_name]['ags_instances'][ags_instance]
    server_url = ags_instance_props['url']
    token = ags_instance_props['token']
    proxies = ags_instance_props.get('proxies') or user_config.get('proxies')
    with create_session(server_url, proxies=proxies) as session:
        existing_services = list_services(server_url, token, service_folder, session=session)
        services_to_remove = [service for service in existing_services if service['serviceName'] not in configured_services]
        log.info(
            f'Removing {len(services_to_remove)} services: '
            f'{", ".join((service["serviceName"] for service in services_to_remove))}'
        )
        for service in services_to_remove:
            delete_service(server_url, token, service['serviceName'], service_folder, service['type'], session=session)
