import collections
import fnmatch


def superfilter(names, inclusion_patterns=(), exclusion_patterns=()):
    """Enhanced version of fnmatch.filter() that accepts multiple inclusion and exclusion patterns.
    If only inclusion_patterns is specified, only the names which match one or more patterns are returned.
    If only exclusion_patterns is specified, only the names which do not match any pattern are returned.
    If both are specified, the exclusion patterns take precedence.
    If neither is specified, the input is returned as-is.
    names can either be a sequence type (e.g. list, tuple), or mapping type (e.g. dict). In the case of a mapping type,
    the key/value pairs are filtered by key. Mapping types nested within sequence types are also supported.
    Order is preserved for sequence types and OrderedDicts.
    Returned value type is same as names, unless names is a MappingView, in which case it is returned as a list."""
    is_mapping = isinstance(names, collections.abc.Mapping)
    is_view = isinstance(names, collections.abc.MappingView)
    keys = names.keys() if is_mapping else names
    included = multifilter(keys, inclusion_patterns) if inclusion_patterns else keys
    excluded = multifilter(keys, exclusion_patterns) if exclusion_patterns else ()
    filtered = set(included) - set(excluded)
    if is_mapping:
        return type(names)(
            ((key, value) for key, value in names.items() if key_is_in_collection(key, filtered))
        )
    elif is_view:
        return [key for key in keys if key_is_in_collection(key, filtered)]
    else:
        return type(names)((key for key in keys if key_is_in_collection(key, filtered)))


def key_is_in_collection(key, collection):
    """Determines whether key is in collection. If collection is a mapping type, recursively checks for key inclusion"""
    if isinstance(key, collections.abc.Mapping):
        for subkey in key.keys():
            if key_is_in_collection(subkey, collection):
                return True
        return False
    else:
        return key in collection


def multifilter(names, patterns):
    """Generator function which yields the names that match one or more of the patterns."""
    for name in names:
        if isinstance(name, collections.abc.Mapping):
            for key in name.keys():
                for pattern in patterns:
                    if fnmatch.fnmatch(key, pattern):
                        yield key
                        break
        else:
            for pattern in patterns:
                if fnmatch.fnmatch(name, pattern):
                    yield name
                    break


if __name__ == "__main__":
    names = ['a', 'b', 'c']
    assert superfilter(names, ('a',)) == ['a', ]
    names = ('a', 'b', 'c')
    assert superfilter(names, ('a',)) == ('a',)
    names = {
        'a': 1,
        'b': 2,
        'c': 3
    }
    assert superfilter(names, ('a',)) == {'a': 1}
    names = collections.OrderedDict((
        ('a', 1),
        ('b', 2),
        ('c', 3)
    ))
    assert superfilter(names, ('a',)) == collections.OrderedDict((
        ('a', 1),
    ))
    assert superfilter(names.keys(), ('a',)) == [
        'a',
    ]
    names = [
        collections.OrderedDict((
            ('a', 1),
        )),
        'b'
    ]
    assert superfilter(names, ('a',)) == [collections.OrderedDict((
        ('a', 1),
    ))]
