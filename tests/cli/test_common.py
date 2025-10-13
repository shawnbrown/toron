"""Tests for toron/cli/common.py module."""
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


class TestGetStreamStyles(unittest.TestCase, StreamTestMixin):
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
