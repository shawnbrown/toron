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

