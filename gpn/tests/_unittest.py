"""unittest compatibility layer."""
from unittest import *

try:
    TestCase.assertIs  # New in 3.1
    TestCase.assertIsNot
    TestCase.assertIsNone
    TestCase.assertIsNotNone
    TestCase.assertIsInstance
    TestCase.assertNotIsInstance
    TestCase.assertSetEqual
    TestCase.assertIn
    TestCase.assertNotIn
except AttributeError:
    class _TestCase(TestCase):
        # The following code was adapted from the Python 3.1 and 3.2
        # standard library (unittest.py).
        longMessage = False

        def _formatMessage(self, msg, standardMsg):
            if not self.longMessage:
                return msg or standardMsg
            if msg is None:
                return standardMsg
            return standardMsg + ' : ' + msg

        def assertIs(self, expr1, expr2, msg=None):
            """Just like self.assertTrue(a is b), but with a nicer default message."""
            if expr1 is not expr2:
                standardMsg = '%r is not %r' % (expr1, expr2)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsNot(self, expr1, expr2, msg=None):
            """Just like self.assertTrue(a is not b), but with a nicer default message."""
            if expr1 is expr2:
                standardMsg = 'unexpectedly identical: %r' % (expr1,)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsNone(self, obj, msg=None):
            """Same as self.assertTrue(obj is None), with a nicer default message."""
            if obj is not None:
                standardMsg = '%r is not None' % obj
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsNotNone(self, obj, msg=None):
            """Included for symmetry with assertIsNone."""
            if obj is None:
                standardMsg = 'unexpectedly None'
                self.fail(self._formatMessage(msg, standardMsg))

        def assertIsInstance(self, obj, cls, msg=None):
            """Same as self.assertTrue(isinstance(obj, cls)), with a nicer
            default message."""
            if not isinstance(obj, cls):
                standardMsg = '%s is not an instance of %r' % (safe_repr(obj), cls)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertNotIsInstance(self, obj, cls, msg=None):
            """Included for symmetry with assertIsInstance."""
            if isinstance(obj, cls):
                standardMsg = '%s is an instance of %r' % (safe_repr(obj), cls)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertSetEqual(self, set1, set2, msg=None):
            """A set-specific equality assertion."""
            try:
                difference1 = set1.difference(set2)
            except TypeError as e:
                self.fail('invalid type when attempting set difference: %s' % e)
            except AttributeError as e:
                self.fail('first argument does not support set difference: %s' % e)
            try:
                difference2 = set2.difference(set1)
            except TypeError as e:
                self.fail('invalid type when attempting set difference: %s' % e)
            except AttributeError as e:
                self.fail('second argument does not support set difference: %s' % e)
            if not (difference1 or difference2):
                return
            lines = []
            if difference1:
                lines.append('Items in the first set but not the second:')
                for item in difference1:
                    lines.append(repr(item))
            if difference2:
                lines.append('Items in the second set but not the first:')
                for item in difference2:
                    lines.append(repr(item))
            standardMsg = '\n'.join(lines)
            self.fail(self._formatMessage(msg, standardMsg))

        def assertIn(self, member, container, msg=None):
            """Just like self.assertTrue(a in b), but with a nicer default message."""
            if member not in container:
                standardMsg = '%r not found in %r' % (member, container)
                self.fail(self._formatMessage(msg, standardMsg))

        def assertNotIn(self, member, container, msg=None):
            """Just like self.assertTrue(a not in b), but with a nicer default message."""
            if member in container:
                standardMsg = '%r unexpectedly found in %r' % (member, container)
                self.fail(self._formatMessage(msg, standardMsg))

    TestCase = _TestCase


try:
    TestCase.assertRaisesRegex  # Renamed in 3.2 (previously assertRaisesRegexp)
except AttributeError:
    try:
        TestCase.assertRaisesRegex = TestCase.assertRaisesRegexp  # New in 2.7
    except AttributeError:
        import re
        class _TestCase(TestCase):
            # The following method was adapted from unittest2 source
            # code <http://pypi.python.org/pypi/unittest2>.
            # Copyright 2010 Michael Foord, released under the BSD License.
            def assertRaisesRegex(self, expected_exception, expected_regexp,
                                   callable_obj=None, *args, **kwargs):
                """Asserts that the message in a raised exception matches a regexp."""
                if callable_obj is None:
                    return unittest._AssertRaisesContext(expected_exception, self, expected_regexp)
                try:
                    callable_obj(*args, **kwargs)
                except expected_exception as exc_value:
                    #if isinstance(expected_regexp, basestring):
                    if isinstance(expected_regexp, str):
                        expected_regexp = re.compile(expected_regexp)
                    if not expected_regexp.search(str(exc_value)):
                        raise self.failureException('"%s" does not match "%s"' %
                                 (expected_regexp.pattern, str(exc_value)))
                else:
                    if hasattr(expected_exception, '__name__'):
                        excName = expected_exception.__name__
                    else:
                        excName = str(expected_exception)
                    raise self.failureException("%s not raised" % excName)

        TestCase = _TestCase


try:
    skip  # New in 3.1
    skipIf
    skipUnless
except NameError:
    # The following code was adapted from the Python 3.1 and 3.2
    # standard library (unittest.py).
    import functools

    def _id(obj):
        return obj

    def skip(reason):
        """Unconditionally skip a test."""
        def decorator(test_item):
            if isinstance(test_item, type) and issubclass(test_item, TestCase):
                test_item.__unittest_skip__ = True
                test_item.__unittest_skip_why__ = reason
                return test_item
            @functools.wraps(test_item)
            def skip_wrapper(*args, **kwargs):
                #raise SkipTest(reason)  # Older versions of unittest
                pass                     # consider SkipTest an error.
            return skip_wrapper
        return decorator

    def skipIf(condition, reason):
        """Skip a test if the condition is true."""
        if condition:
            return skip(reason)
        return _id

    def skipUnless(condition, reason):
        """Skip a test unless the condition is true."""
        if not condition:
            return skip(reason)
        return _id
