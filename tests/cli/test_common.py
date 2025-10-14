"""Tests for toron/cli/common.py module."""
import logging
from io import BytesIO, TextIOWrapper
from .. import _unittest as unittest
from ..common import (  # <- tests/common.py (not cli/common.py)
    StreamTestMixin,
    DummyStream,
    DummyRedirectedStream,
)

from toron.cli.common import (
    csv_stdout_writer,
    ansi_codes,
    TerminalStyle,
    get_stream_styles,
    get_formatter_class,
)


class TestCsvStdoutWriter(unittest.TestCase, StreamTestMixin):
    def test_line_endings(self):
        """Should use consistent for newlines regardless of system."""
        dummy_stdout = TextIOWrapper(BytesIO(), newline='\r\n')

        with csv_stdout_writer(dummy_stdout) as writer:
            writer.writerow(['foo', 'bar', 'baz'])
            writer.writerow(['qux', 'quux', 'corge'])

        self.assertStream(dummy_stdout, 'foo,bar,baz\nqux,quux,corge\n')

    def test_utf8_encoding(self):
        """Should encode as UTF-8 regardless of original stream."""
        dummy_stdout = TextIOWrapper(BytesIO(), encoding='latin-1')

        with csv_stdout_writer(dummy_stdout) as writer:
            writer.writerow(['\u0192\u00f3\u00f3', '\u0253\u00e0\u0155', '\u0184\u0251\u017e'])
        self.assertStream(dummy_stdout, 'ƒóó,ɓàŕ,Ƅɑž\n')


class TestGetStreamStyles(unittest.TestCase):
    def setUp(self):
        self.ansi_styles = TerminalStyle(**ansi_codes)
        self.no_styles = TerminalStyle()

    def test_ansi_styles(self):
        """Interactive streams should get styled output."""
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.ansi_styles)
        self.assertEqual(stderr_style, self.ansi_styles)

    def test_environ_no_color(self):
        """Should disable color if "NO_COLOR" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'NO_COLOR': 1},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_styles)
        self.assertEqual(stderr_style, self.no_styles)

    def test_environ_dumb_terminal(self):
        """Should disable color if "TERM=dumb" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'TERM': 'dumb'},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_styles)
        self.assertEqual(stderr_style, self.no_styles)

    def test_stream_redirection(self):
        """Should disable color for streams that are redirected."""
        # Redirected stdout.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyRedirectedStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_styles)
        self.assertEqual(stderr_style, self.ansi_styles)

        # Redirected stderr.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyStream(),
            stderr=DummyRedirectedStream(),
        )
        self.assertEqual(stdout_style, self.ansi_styles)
        self.assertEqual(stderr_style, self.no_styles)

    def test_default_behavior(self):
        """Without arguments, should use system environ and streams."""
        stdout_style, stderr_style = get_stream_styles()  # <- No args given.
        self.assertIsInstance(stdout_style, TerminalStyle)
        self.assertIsInstance(stderr_style, TerminalStyle)


class TestGetFormatterClass(unittest.TestCase):
    def test_no_styles_class(self):
        """When no styles are set, should return built-in Formatter."""
        no_styles = TerminalStyle()
        formatter_class = get_formatter_class(no_styles)
        self.assertIs(formatter_class, logging.Formatter)

    def test_ansi_styles_class(self):
        """When styles are set, should return subclassed Formatter."""
        ansi_styles = TerminalStyle(**ansi_codes)
        formatter_class = get_formatter_class(ansi_styles)
        self.assertIsNot(formatter_class, logging.Formatter)
        self.assertIsSubclass(formatter_class, logging.Formatter)

    def test_format_method(self):
        """Test custom formatter's ``format()`` method."""
        formatter_class = get_formatter_class(TerminalStyle(
            info='[START]',
            reset='[STOP]',
        ))
        formatter = formatter_class()

        # Check `format()` method's styled output.
        value = formatter.format(logging.LogRecord(
            name='dummy_logger',
            level=logging.INFO,
            pathname='/path/to/file.py',
            lineno=42,
            msg='hello world',
            args=(),
            exc_info=None,
        ))
        self.assertEqual(value, '[START]hello world[STOP]')

        # Check `format()` handling of unknown level number.
        value = formatter.format(logging.LogRecord(
            name='dummy_logger',
            level=999,  # <- Level not not match a styled formatter.
            pathname='/path/to/file.py',
            lineno=42,
            msg='hello world',
            args=(),
            exc_info=None,
        ))
        self.assertEqual(value, 'hello world', msg='output should be unstyled')
