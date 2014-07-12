# -*- coding: utf-8 -*-
from gpn.connector import _Connector


class Partition(object):
    def __init__(self, path=None, mode=0):
        """Get existing Partition or create a new one."""
        self._connect = _Connector(path, mode=mode)
