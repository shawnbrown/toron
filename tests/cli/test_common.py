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
    StyleCodes,
    get_stream_styles,
    get_formatter_class,
)


class TestCsvStdoutWriter(unittest.TestCase, StreamTestMixin):
    def test_line_endings(self):
        """Should use consistent newlines regardless of system."""
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
        self.ansi_style = StyleCodes(**ansi_codes)
        self.no_style = StyleCodes()

    def test_ansi_style(self):
        """Interactive streams should get styled output."""
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.ansi_style)
        self.assertEqual(stderr_style, self.ansi_style)

    def test_environ_no_color(self):
        """Should disable color if "NO_COLOR" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'NO_COLOR': 1},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.no_style)

    def test_environ_dumb_terminal(self):
        """Should disable color if "TERM=dumb" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'TERM': 'dumb'},
            stdout=DummyStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.no_style)

    def test_stream_redirection(self):
        """Should disable color for streams that are redirected."""
        # Redirected stdout.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyRedirectedStream(),
            stderr=DummyStream(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.ansi_style)

        # Redirected stderr.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyStream(),
            stderr=DummyRedirectedStream(),
        )
        self.assertEqual(stdout_style, self.ansi_style)
        self.assertEqual(stderr_style, self.no_style)

    def test_default_behavior(self):
        """Without arguments, should use system environ and streams."""
        stdout_style, stderr_style = get_stream_styles()  # <- No args given.
        self.assertIsInstance(stdout_style, StyleCodes)
        self.assertIsInstance(stderr_style, StyleCodes)


class TestGetFormatterClass(unittest.TestCase):
    def test_no_style_class(self):
        """When styles are not given, should return built-in Formatter."""
        no_style = StyleCodes()
        formatter_class = get_formatter_class(no_style)
        self.assertIs(formatter_class, logging.Formatter)

    def test_ansi_style_class(self):
        """When styles are given, should return subclassed Formatter."""
        ansi_style = StyleCodes(**ansi_codes)
        formatter_class = get_formatter_class(ansi_style)
        self.assertIsNot(formatter_class, logging.Formatter)
        self.assertIsSubclass(formatter_class, logging.Formatter)

    def test_format_method(self):
        """Test custom formatter's ``format()`` method."""
        irc_control_codes = StyleCodes(error='\x03C4', reset='\x03O')
        formatter_class = get_formatter_class(irc_control_codes)
        formatter = formatter_class()

        # Log record to test formatting.
        log_record = logging.LogRecord(
            name='dummy_logger',
            level=logging.ERROR,
            pathname='/path/to/file.py',
            lineno=42,
            msg='hello world',
            args=(),
            exc_info=None,
        )

        # Check for expected style codes in output.
        value = formatter.format(log_record)
        self.assertEqual(value, '\x03C4hello world\x03O')

        # Check handling of unknown level number.
        log_record.levelno = 999  # <- Level does not match a styled formatter.
        value = formatter.format(log_record)
        self.assertEqual(value, 'hello world', msg='output should be unstyled')
