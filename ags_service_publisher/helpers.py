from __future__ import unicode_literals

import collections
import contextlib
import gc
import inspect
import os
import sys


class NoDefaultProvided(object):
    pass


def deep_get(obj, attr, default=NoDefaultProvided):
    try:
        return reduce(
            lambda d, k: d.get(k, default) if isinstance(d, dict) else getattr(d, k, default),
            attr.split('.'),
            obj
        )
    except (KeyError, NameError, AttributeError):
        if default != NoDefaultProvided:
            return default
        raise


def snake_case_to_camel_case(input_string):
    return ''.join((word if i == 0 else word.capitalize() for i, word in enumerate(input_string.split('_'))))


def snake_case_to_sentence_case(input_string):
    return ' '.join(input_string.split('_')).capitalize()


def sentence_case_to_snake_case(input_string, capitalize=False):
    return '_'.join((word.capitalize() if capitalize else word for word in input_string.split(' ')))


def snake_case_to_pascal_case(input_string):
    return ''.join((word.capitalize() for word in input_string.split('_')))


def split_quoted_string(input_string, delimiter):
    parts = []
    quoted = False
    part = ''
    length = len(input_string)
    for i, char in enumerate(input_string):
        if i == length - 1:
            part += char
            parts.append(part)
            break
        if not quoted and char == delimiter:
            parts.append(part)
            part = ''
        elif not quoted and char == '"':
            quoted = True
            part += char
        elif quoted and char == '"':
            quoted = False
            part += char
        else:
            part += char
    return parts


def unquote_string(input_string):
    return input_string if not (input_string.startswith('"') and input_string.endswith('"')) else input_string[1:-1]


def format_arguments(args):
    return ', '.join([snake_case_to_sentence_case(str(key)) + ': ' + str(value) for key, value in args.iteritems()])


def list_files_in_dir(directory, ext=''):
    return map(lambda x: os.path.abspath(os.path.join(directory, x)),
               filter(lambda x: x.endswith(ext), os.listdir(directory)))


# Adapted from http://stackoverflow.com/a/4506081
def get_func_from_frame(frame):
    code = frame.f_code
    globs = frame.f_globals
    functype = type(lambda: 0)
    funcs = []
    for func in gc.get_referrers(code):
        if type(func) is functype:
            if getattr(func, "func_code", None) is code:
                if getattr(func, "func_globals", None) is globs:
                    funcs.append(func)
                    if len(funcs) > 1:
                        return None
    return funcs[0] if funcs else None


def dump_func():
    frame = inspect.currentframe(1)
    func = get_func_from_frame(frame)
    argspec = inspect.getargspec(func)
    return func.func_name, collections.OrderedDict((arg, frame.f_locals[arg]) for arg in argspec.args)


# Adapted from http://stackoverflow.com/a/9836725
@contextlib.contextmanager
def file_or_stdout(file_name, mode='w'):
    if file_name is None:
        yield sys.stdout
    else:
        with open(file_name, mode) as out_file:
            yield out_file


empty_tuple = ()
asterisk_tuple = ('*',)
