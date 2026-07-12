"""compatibility layer for `unittest` (Python standard library)"""
__unittest = True

import sys
from unittest import *
from unittest import mock


# Save original TestCase reference.
_TestCaseOrig = TestCase

# Subclass which will be monkey-patched as needed.
class TestCase(_TestCaseOrig):
    pass


if not hasattr(TestCase, 'assertNoLogs'):  # New in 3.10
    # The following code is adapted from the Python 3.10 Standard Library.
    import collections
    import logging
    from unittest.case import _BaseTestCaseContext

    _LoggingWatcher = collections.namedtuple("_LoggingWatcher",
                                             ["records", "output"])

    class _CapturingHandler(logging.Handler):
        def __init__(self):
            logging.Handler.__init__(self)
            self.watcher = _LoggingWatcher([], [])

        def flush(self):
            pass

        def emit(self, record):
            self.watcher.records.append(record)
            msg = self.format(record)
            self.watcher.output.append(msg)

    class _AssertLogsContext(_BaseTestCaseContext):
        LOGGING_FORMAT = "%(levelname)s:%(name)s:%(message)s"

        def __init__(self, test_case, logger_name, level, no_logs):
            _BaseTestCaseContext.__init__(self, test_case)
            self.logger_name = logger_name
            if level:
                self.level = logging._nameToLevel.get(level, level)
            else:
                self.level = logging.INFO
            self.msg = None
            self.no_logs = no_logs

        def __enter__(self):
            if isinstance(self.logger_name, logging.Logger):
                logger = self.logger = self.logger_name
            else:
                logger = self.logger = logging.getLogger(self.logger_name)
            formatter = logging.Formatter(self.LOGGING_FORMAT)
            handler = _CapturingHandler()
            handler.setLevel(self.level)
            handler.setFormatter(formatter)
            self.watcher = handler.watcher
            self.old_handlers = logger.handlers[:]
            self.old_level = logger.level
            self.old_propagate = logger.propagate
            logger.handlers = [handler]
            logger.setLevel(self.level)
            logger.propagate = False
            if self.no_logs:
                return
            return handler.watcher

        def __exit__(self, exc_type, exc_value, tb):
            self.logger.handlers = self.old_handlers
            self.logger.propagate = self.old_propagate
            self.logger.setLevel(self.old_level)

            if exc_type is not None:
                return False

            if self.no_logs:
                if len(self.watcher.records) > 0:
                    self._raiseFailure(
                        "Unexpected logs found: {!r}".format(
                            self.watcher.output
                        )
                    )
            else:
                if len(self.watcher.records) == 0:
                    self._raiseFailure(
                        "no logs of level {} or higher triggered on {}"
                        .format(logging.getLevelName(self.level), self.logger.name))

    def _assertNoLogs(self, logger=None, level=None):
        return _AssertLogsContext(self, logger, level, no_logs=True)

    TestCase.assertNoLogs = _assertNoLogs


if not all([
    hasattr(TestCase, 'enterContext'),  # New in 3.11
    hasattr(TestCase, 'enterClassContext'),  # New in 3.11
]):
    # The following code is adapted from the Python 3.11 Standard Library.
    def _enter_context(cm, addcleanup):
        cls = type(cm)
        try:
            enter = cls.__enter__
            exit = cls.__exit__
        except AttributeError:
            raise TypeError(f"'{cls.__module__}.{cls.__qualname__}' object does "
                            f"not support the context manager protocol") from None
        result = enter(cm)
        addcleanup(exit, cm, None, None, None)
        return result

    @classmethod
    def _enterClassContext(cls, cm):
        return _enter_context(cm, cls.addClassCleanup)

    def _enterContext(self, cm):
        return _enter_context(cm, self.addCleanup)

    TestCase.enterClassContext = _enterClassContext
    TestCase.enterContext = _enterContext


if not hasattr(TestCase, 'assertIsSubclass'):  # New in 3.14
    # The following code is adapted from the Python 3.14 Standard Library.
    def _assertIsSubclass(self, cls, superclass, msg=None):
        try:
            if issubclass(cls, superclass):
                return
        except TypeError:
            if not isinstance(cls, type):
                self.fail(self._formatMessage(msg, f'{cls!r} is not a class'))
            raise
        if isinstance(superclass, tuple):
            standardMsg = f'{cls!r} is not a subclass of any of {superclass!r}'
        else:
            standardMsg = f'{cls!r} is not a subclass of {superclass!r}'
        self.fail(self._formatMessage(msg, standardMsg))

    TestCase.assertIsSubclass = _assertIsSubclass
