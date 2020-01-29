from __future__ import unicode_literals

import os
from collections import OrderedDict

import yaml  # PyYAML: http://pyyaml.org/

from extrafilters import superfilter
from helpers import asterisk_tuple, empty_tuple
from logging_io import setup_logger

log = setup_logger(__name__)

default_config_dir = os.getenv(
    'AGS_SERVICE_PUBLISHER_CONFIG_DIR',
    os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs'))
)


def get_config(config_name, config_dir=default_config_dir):
    log.debug('Getting config \'{}\' in directory: {}'.format(config_name, config_dir))
    return read_config_from_file(get_config_file_path(config_name, config_dir))


def get_configs(
    included_configs=asterisk_tuple, excluded_configs=empty_tuple,
    config_dir=default_config_dir
):
    if len(included_configs) == 1 and included_configs[0] == '*':
        log.debug('No config names specified, reading all configs in directory: {}'.format(config_dir))
        config_names = [
            os.path.splitext(os.path.basename(config_file))[0] for
            config_file in
            superfilter(os.listdir(config_dir), inclusion_patterns=('*.yml',), exclusion_patterns=('userconfig.yml',))
        ]
    else:
        config_names = included_configs
    config_names = superfilter(config_names, included_configs, excluded_configs)
    log.debug('Getting configs \'{}\' in directory: {}'.format(', '.join(config_names), config_dir))
    return OrderedDict(((config_name, get_config(config_name, config_dir)) for config_name in config_names))


def set_config(config, config_name, config_dir=default_config_dir):
    log.debug('Setting config \'{}\' in directory: {}'.format(config_name, config_dir))
    return write_config_to_file(config, get_config_file_path(config_name, config_dir))


# Adapted from http://stackoverflow.com/a/21912744
def ordered_load(stream, Loader=yaml.SafeLoader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


# Adapted from http://stackoverflow.com/a/21912744
def ordered_dump(data, stream=None, Dumper=yaml.SafeDumper, **kwds):
    class OrderedDumper(Dumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_dict(data.iteritems())

    OrderedDumper.add_representer(OrderedDict, _dict_representer)

    return yaml.dump(data, stream, OrderedDumper, **kwds)


def get_config_file_path(config_name, config_dir=default_config_dir):
    return os.path.abspath(os.path.join(config_dir, config_name + '.yml'))


def read_config_from_file(file_path):
    log.debug('Reading config from file: {}'.format(file_path))
    with open(file_path) as f:
        config = ordered_load(f)
        return config


def write_config_to_file(config, file_path):
    log.debug('Writing config to file: {}'.format(file_path))
    with open(file_path, 'wb') as f:
        ordered_dump(config, f, default_flow_style=False, width=float('inf'))
