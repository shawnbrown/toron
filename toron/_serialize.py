"""Simplified, Python-native object serialization using strings."""

_primitive_types = (str, int, float, bool, type(None), bytes, complex)


def _is_primitive(obj):
    """Returns True if *obj* is a supported non-container type."""
    for type_ in _primitive_types:
        if obj.__class__ is type_:
            return True
    return False

