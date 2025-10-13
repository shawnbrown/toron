"""Tests for toron/cli/common.py module."""
from io import BytesIO, TextIOWrapper
from .. import _unittest as unittest
from ..common import StreamTestMixin

from toron.cli.common import (
    csv_stdout_writer,
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
