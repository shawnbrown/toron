"""Simplified, Python-native object serialization using strings."""

from ast import literal_eval as _literal_eval


_primitive_types = (str, int, float, bool, type(None), bytes, complex)


def _is_primitive(obj):
    """Return True if *obj* is a supported, non-container value."""
    for type_ in _primitive_types:
        if obj.__class__ is type_:
            try:
                return obj == _literal_eval(repr(obj))
            except Exception:
                return False
    return False


def _serialize_list_or_tuple(obj):
    """Serialize a list or tuple of primitive items as a string."""
    for item in obj:
        if not _is_primitive(item):
            msg = f'cannot serialize item of type {item.__class__}'
            raise TypeError(msg)

    return repr(obj)


def _serialize_set(obj):
    member_reprs = []
    for item in obj:
        if not _is_primitive(item):
            msg = f'cannot serialize member of type {item.__class__}'
            raise TypeError(msg)
        member_reprs.append(repr(item))

    return f'{{{", ".join(sorted(member_reprs))}}}'


def _serialize_dict(obj):
    """Serialize a dictionary of basic types to a Python-literal
    formatted string. Keys and values must be instances of one of
    the supported types. Dictionary items do not preserve their
    original order but are serialized in alphabetical order by key.

    Supported types: str, bytes, int, float, bool, complex, NoneType
    """
    item_reprs = []
    for key, value in obj.items():
        if not _is_primitive(key):
            msg = f'cannot serialize key of type {key.__class__}'
            raise TypeError(msg)

        if not _is_primitive(value):
            msg = f'cannot serialize value of type {value.__class__}'
            raise TypeError(msg)

        item_reprs.append(f'{key!r}: {value!r}')

    return f'{{{", ".join(sorted(item_reprs))}}}'


def dumps(obj):
    """Return a string representing the serialized content of *obj*."""
    if _is_primitive(obj):
        return repr(obj)

    if (obj.__class__ is list) or (obj.__class__ is tuple):
        return _serialize_list_or_tuple(obj)

    if obj.__class__ is set:
        return _serialize_set(obj)

    if obj.__class__ is dict:
        return _serialize_dict(obj)

    msg = f'cannot serialize object of type {obj.__class__}'
    raise TypeError(msg)

