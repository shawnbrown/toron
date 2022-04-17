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


def dumps(obj):
    """Return a string representing the serialized content of *obj*."""
    if _is_primitive(obj):
        return repr(obj)

    if (obj.__class__ is list) or (obj.__class__ is tuple):
        return _serialize_list_or_tuple(obj)

    msg = f'cannot serialize object of type {obj.__class__}'
    raise TypeError(msg)

