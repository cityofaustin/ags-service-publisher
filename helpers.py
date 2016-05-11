import collections
import gc
import inspect


def snake_case_to_camel_case(input):
    return ''.join(word.capitalize() for word in input.split('_'))


def snake_case_to_sentence_case(input):
    return ' '.join(input.split('_')).capitalize()


def format_arguments(args):
    return ', '.join([snake_case_to_sentence_case(str(key)) + ': ' + str(value) for key, value in args.iteritems()])


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


empty_tuple = ()
asterisk_tuple = ('*',)
