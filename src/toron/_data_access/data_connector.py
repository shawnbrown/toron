"""DataConnector and related objects using SQLite."""

import atexit
import os
import tempfile

from toron._typing import (
    Callable,
    List,
    Optional,
    Set,
)

from .base_classes import BaseDataConnector


_tempfiles_to_remove_at_exit: Set[str] = set()


@atexit.register  # <- Register with `atexit` module.
def _cleanup_leftover_temp_files():
    """Remove temporary files left-over from `cache_to_drive` usage.

    The DataConnector class cleans-up files when __del__() is called
    but the Python documentation states:

        It is not guaranteed that __del__() methods are called
        for objects that still exist when the interpreter exits.

    For more details see:

        https://docs.python.org/3/reference/datamodel.html#object.__del__

    This function is intended to be registered with the `atexit` module
    and executed only once when the interpreter exits.
    """
    while _tempfiles_to_remove_at_exit:
        path = _tempfiles_to_remove_at_exit.pop()
        try:
            os.unlink(path)
        except Exception as e:
            import warnings
            msg = f'cannot remove temporary file {path!r}, {e.__class__.__name__}'
            warnings.warn(msg, RuntimeWarning)


class DataConnector(BaseDataConnector):
    # Absolute path of class instance's database (None if file in memory).
    _current_working_path: Optional[str] = None
    _cleanup_funcs: List[Callable]

    def __init__(self, cache_to_drive: bool = False) -> None:
        """Initialize a new node instance."""
        self._cleanup_funcs = []

        if cache_to_drive:
            temp_f = tempfile.NamedTemporaryFile(suffix='.toron', delete=False)
            temp_f.close()
            database_path = os.path.abspath(temp_f.name)
            self._current_working_path = database_path

            _tempfiles_to_remove_at_exit.add(database_path)
            self._cleanup_funcs.extend([
                lambda: _tempfiles_to_remove_at_exit.discard(database_path),
                lambda: os.unlink(database_path),
            ])
        else:
            database_path = ':memory:'
            self._current_working_path = None

    def __del__(self):
        while self._cleanup_funcs:
            func = self._cleanup_funcs.pop()
            func()
