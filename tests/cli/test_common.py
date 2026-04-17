"""Tests for toron/cli/common.py module."""
import logging
import uuid
from io import BytesIO, TextIOWrapper
from .. import _unittest as unittest
from ..common import (  # <- tests/common.py (not cli/common.py)
    StreamWrapperTestCase,
    DummyTTY,
    DummyRedirection,
)

from toron.cli.common import (
    csv_stdout_writer,
    ansi_codes,
    StyleCodes,
    get_stream_styles,
    get_formatter_class,
    index_id_to_code,
    index_code_to_id,
    is_index_code,
    get_index_code_position,
    remap_index_codes_to_index_ids,
    make_index_code_header,
)


class TestCsvStdoutWriter(StreamWrapperTestCase):
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
            stdout=DummyTTY(),
            stderr=DummyTTY(),
        )
        self.assertEqual(stdout_style, self.ansi_style)
        self.assertEqual(stderr_style, self.ansi_style)

    def test_environ_no_color(self):
        """Should disable color if "NO_COLOR" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'NO_COLOR': 1},
            stdout=DummyTTY(),
            stderr=DummyTTY(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.no_style)

    def test_environ_dumb_terminal(self):
        """Should disable color if "TERM=dumb" is set in environment."""
        stdout_style, stderr_style = get_stream_styles(
            environ={'TERM': 'dumb'},
            stdout=DummyTTY(),
            stderr=DummyTTY(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.no_style)

    def test_stream_redirection(self):
        """Should disable color for streams that are redirected."""
        # Redirected stdout.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyRedirection(),
            stderr=DummyTTY(),
        )
        self.assertEqual(stdout_style, self.no_style)
        self.assertEqual(stderr_style, self.ansi_style)

        # Redirected stderr.
        stdout_style, stderr_style = get_stream_styles(
            environ={},
            stdout=DummyTTY(),
            stderr=DummyRedirection(),
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


class TestIndexCodeHandling(unittest.TestCase):
    def setUp(self):
        self.node_id1 = uuid.UUID('11111111-1111-1111-1111-111111111111')
        self.node_id2 = uuid.UUID('22222222-2222-2222-2222-222222222222')

    def test_index_id_to_code(self):
        """Check ``index_id_to_code()`` function (e.g., 999 -> 999X04C3FB2E)."""
        values = [
            # Check without zero-padding.
            (0, self.node_id1, 0, '0X27B3B62D'),
            (0, self.node_id2, 0, '0X7054347B'),
            (999, self.node_id1, 0, '999X24CE8BE2'),
            (999, self.node_id2, 0, '999X732909B4'),

            # Should zero-pad index_id values to 4 chars.
            (0, self.node_id1, 4, '0000X27B3B62D'),
            (0, self.node_id2, 4, '0000X7054347B'),
            (999, self.node_id1, 4, '0999X24CE8BE2'),
            (999, self.node_id2, 4, '0999X732909B4'),
        ]

        for index_id, unique_id, pad_len, expected_code in values:
            with self.subTest(index_id=index_id, unique_id=unique_id, pad_len=pad_len):
                index_code = index_id_to_code(index_id, unique_id.bytes, pad_len)  # <- Function under test.
                self.assertEqual(index_code, expected_code)

    def test_index_code_to_id(self):
        """Check ``index_code_to_id()`` function (e.g., 999X04C3FB2E -> 999)."""
        values = [
            # Check index codes without zero-padding.
            ('0X27B3B62D', self.node_id1, 0),
            ('0X7054347B', self.node_id2, 0),
            ('999X24CE8BE2', self.node_id1, 999),
            ('999X732909B4', self.node_id2, 999),

            # Check index codes with zero-padding.
            ('0000X27B3B62D', self.node_id1, 0),
            ('0000X7054347B', self.node_id2, 0),
            ('0999X24CE8BE2', self.node_id1, 999),
            ('0999X732909B4', self.node_id2, 999),
        ]

        for index_code, unique_id, expected_id in values:
            with self.subTest(index_code=index_code, unique_id=unique_id):
                index_id = index_code_to_id(index_code, unique_id.bytes)  # <- Function under test.
                self.assertEqual(index_id, expected_id)

        regex = r'checksum mismatch for index code: 123XDBE54EF9'
        with self.assertRaisesRegex(ValueError, regex):
            index_code_to_id('123XDBE54EF9', self.node_id1.bytes)

        regex = r'badly formatted index code: 123_D6577782'
        with self.assertRaisesRegex(ValueError, regex):
            index_code_to_id('123_D6577782', self.node_id1.bytes)

        regex = (r"'NoneType' object has no attribute 'partition'; "
                 r"index_code must be a str, got None")
        with self.assertRaisesRegex(AttributeError, regex):
            index_code_to_id(None, self.node_id1.bytes)

    def test_is_index_code(self):
        self.assertTrue(is_index_code('0999X24CE8BE2', self.node_id1.bytes))
        self.assertFalse(is_index_code('0999X24CE8BE2', self.node_id2.bytes))

        self.assertTrue(is_index_code('0999X732909B4', self.node_id2.bytes))
        self.assertFalse(is_index_code('0999X732909B4', self.node_id1.bytes))

        self.assertFalse(is_index_code('<BADLY FORMED>', self.node_id1.bytes))
        self.assertFalse(is_index_code('', self.node_id1.bytes))
        self.assertFalse(is_index_code(123, self.node_id1.bytes))

    def test_get_index_code_position(self):
        sample_rows = [
            ['index_code1', 'weight', 'index_code2'],
            ['1XA0157D6E', '150', '5X84FAD8F7'],
            ['2XF38F26EA', '120', '8X96447BE5'],
            ['3X7429EDA9', '180', '4X035C13B4'],
        ]

        position = get_index_code_position(sample_rows, self.node_id1.bytes)
        self.assertEqual(position, 0)

        position = get_index_code_position(sample_rows, self.node_id2.bytes)
        self.assertEqual(position, 2)

        # Raise an error if no column contains matching index codes.
        sample_rows = [
            ['index_code3', 'weight', 'index_code4'],
            ['21X9239A237', '110', '32XA468A4BC'],
            ['22XB2ABDE7C', '170', '35X06B329DB'],
            ['23X350D153F', '140', '36XF21DC557'],
        ]
        regex = r'no column found with matching index codes'
        with self.assertRaisesRegex(RuntimeError, regex):
            get_index_code_position(sample_rows, self.node_id1.bytes)

        # Raise an error if two or more columns contain matching index codes.
        sample_rows = [
            ['index_code1', 'weight', 'index_code2'],
            ['1XA0157D6E', '150', '5X84FAD8F7'],
            ['2XF38F26EA', '125', '8XC1A3F9B3'],  # <- Code '8XC1A3F9B3' matches index_code1.
            ['3X7429EDA9', '180', '4X035C13B4'],
        ]
        regex = r'found multiple columns with matching index codes at positions: 0 and 2'
        with self.assertRaisesRegex(RuntimeError, regex):
            get_index_code_position(sample_rows, self.node_id1.bytes)

    def test_remap_index_codes_to_index_ids(self):
        index_rows = [
            ['index_code1', 'geo1', 'geo2', 'weight'],
            ['1XA0157D6E', 'A', 'X', '150'],
            ['2XF38F26EA', 'A', 'Y', '120'],
            ['3X7429EDA9', 'B', '2', '180'],
        ]
        remapped = remap_index_codes_to_index_ids(index_rows, self.node_id1.bytes, 0)
        expected = [
            ['index_id', 'geo1', 'geo2', 'weight'],
            [1, 'A', 'X', '150'],
            [2, 'A', 'Y', '120'],
            [3, 'B', '2', '180'],
        ]
        self.assertEqual(list(remapped), expected)

        crosswalk_rows = [
            ['index_code1', 'weight', 'index_code2'],
            ['1XA0157D6E', '150', '5X84FAD8F7'],
            ['2XF38F26EA', '120', '8X96447BE5'],
            ['3X7429EDA9', '180', '4X035C13B4'],
        ]
        remapped1 = remap_index_codes_to_index_ids(crosswalk_rows, self.node_id1.bytes, position=0)
        remapped2 = remap_index_codes_to_index_ids(remapped1, self.node_id2.bytes, position=2)
        expected = [
            ['index_id', 'weight', 'index_id'],
            [1, '150', 5],
            [2, '120', 8],
            [3, '180', 4],
        ]
        self.assertEqual(list(remapped2), expected)


class TestMakeIndexCodeHeader(unittest.TestCase):
    def test_str_input(self):
        values = [
            ('foo',      'foo_index_code'),
            ('   foo\t', 'foo_index_code'),
            ('foo bar',  'foo_bar_index_code'),
            ('',         'index_code'),
        ]
        for domain, expected in values:
            with self.subTest(domain=domain):
                self.assertEqual(make_index_code_header(domain), expected)

    def test_dict_input(self):
        # NOTE: When `domain` is changed to str, this test will be unneeded.
        values = [
            ({'domain': 'foo'},                'foo_index_code'),
            ({'domain': '   foo\t'},           'foo_index_code'),
            ({'domain': 'foo bar'},            'foo_bar_index_code'),
            ({'domain': ''},                   'index_code'),
            ({'bbb': 'bar baz', 'aaa': 'foo'}, 'foo_bar_baz_index_code'),
            ({},                               'index_code'),
        ]
        for legacy_domain, expected in values:
            with self.subTest(domain=legacy_domain):
                self.assertEqual(make_index_code_header(legacy_domain), expected)
