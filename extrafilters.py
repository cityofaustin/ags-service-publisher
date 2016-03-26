import fnmatch
import collections


def superfilter(names, inclusion_patterns=(), exclusion_patterns=()):
    """Enhanced version of fnmatch.filter() that accepts multiple inclusion and exclusion patterns.
    If only inclusion_patterns is specified, only the names which match one or more patterns are returned.
    If only exclusion_patterns is specified, only the names which do not match any pattern are returned.
    If both are specified, the exclusion patterns take precedence.
    If neither is specified, the input is returned as-is.
    names can either be a sequence type (e.g. list, tuple), or mapping type (e.g. dict). In the case of a mapping type, the
    key/value pairs are filtered by key.
    Order is preserved for sequence types and OrderedDicts.
    Returned value type is same as names."""
    is_mapping = isinstance(names, collections.Mapping)
    keys = names.iterkeys() if is_mapping else names
    included = multifilter(keys, inclusion_patterns) if inclusion_patterns else keys
    excluded = multifilter(keys, exclusion_patterns) if exclusion_patterns else ()
    filtered = set(included) - set(excluded)
    if is_mapping:
        return names.__class__(((key, value) for key, value in names.iteritems() if key in filtered))
    else:
        return names.__class__((key for key in keys if key in filtered))


def multifilter(names, patterns):
    """Generator function which yields the names that match one or more of the patterns."""
    for name in names:
        for pattern in patterns:
            if fnmatch.fnmatch(name, pattern):
                yield name

if __name__ == "__main__":
    names = ['a', 'b', 'c']
    assert superfilter(names, ('a',)) == ['a',]
    names = ('a', 'b', 'c')
    assert superfilter(names, ('a',)) == ('a',)
    names = {
        'a': 1,
        'b': 2,
        'c': 3
    }
    assert superfilter(names, ('a',)) == { 'a': 1}
    names = collections.OrderedDict((
        ('a', 1),
        ('b', 2),
        ('c', 3)
    ))
    assert superfilter(names, ('a',)) == collections.OrderedDict((
        ('a', 1),
    ))
