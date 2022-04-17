"""Simplified, Python-native object serialization using strings.

Differences from JSON:

* Supports ``tuple`` and ``set`` object (JSON does not support these types).
* Restricts serialization to instances of exact type matches to prevent
  data loss. E.g., OrderedDict would raise an exception because they cannot
  be serialized using literal object notation.
* Collections cannot be nested--containers can only contain non-container
  types (JSON allows nested containers).
"""

from ast import literal_eval as _literal_eval


_primitive_types = (str, int, float, bool, type(None), bytes, complex)


def get_primitive_repr(obj):
    """Return repr string for supported, non-container values."""
    for type_ in _primitive_types:
        if obj.__class__ is type_:
            obj_repr = repr(obj)
            try:
                if obj == _literal_eval(obj_repr):
                    return obj_repr
            except Exception:
                return None
    return None


def _serialize_list_or_tuple(obj):
    """Serialize a list or tuple of primitive items as a string."""
    for item in obj:
        if get_primitive_repr(item) is None:
            msg = f'cannot serialize item of type {item.__class__}'
            raise TypeError(msg)

    return repr(obj)


def _serialize_set(obj):
    """Serialize a set of primitive items as a string."""
    member_reprs = []
    for item in obj:
        item_repr = get_primitive_repr(item)
        if item_repr is None:
            msg = f'cannot serialize member of type {item.__class__}'
            raise TypeError(msg)
        member_reprs.append(item_repr)

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
        key_repr = get_primitive_repr(key)
        if key_repr is None:
            msg = f'cannot serialize key of type {key.__class__}'
            raise TypeError(msg)

        value_repr = get_primitive_repr(value)
        if value_repr is None:
            msg = f'cannot serialize value of type {value.__class__}'
            raise TypeError(msg)

        item_reprs.append(f'{key_repr}: {value_repr}')

    return f'{{{", ".join(sorted(item_reprs))}}}'


def dumps(obj):
    """Return a string representing the serialized content of *obj*."""
    obj_repr = get_primitive_repr(obj)
    if obj_repr:
        return obj_repr

    if (obj.__class__ is list) or (obj.__class__ is tuple):
        return _serialize_list_or_tuple(obj)

    if obj.__class__ is set:
        return _serialize_set(obj)

    if obj.__class__ is dict:
        return _serialize_dict(obj)

    msg = f'cannot serialize object of type {obj.__class__}'
    raise TypeError(msg)

