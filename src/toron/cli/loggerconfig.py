"""Resources to configure CLI application logger."""

import logging
import logging.config
import sys
from .._typing import (
    Any,
    Dict,
    Literal,
    Optional,
)
from .colorconfig import stderr_styles


if sys.version_info < (3, 8):
    # Prior to Python 3.8, logging.Formatter did not support the *validate* or
    # *defaults* arguments.
    class _Formatter(logging.Formatter):
        def __init__(
            self,
            fmt: Optional[str] = None,
            datefmt: Optional[str] = None,
            style: Literal['%', '{', '$'] = '%',
            validate: bool = True,
            *,
            defaults: Optional[Dict[str, Any]] = None,
        ) -> None:
            """Initialize, discarding *validate* and *defaults* arguments."""
            super().__init__(fmt, datefmt, style)

elif sys.version_info < (3, 10):
    # Prior to Python 3.10, logging.Formatter did not support the *defaults*
    # argument.
    class _Formatter(logging.Formatter):
        def __init__(
            self,
            fmt: Optional[str] = None,
            datefmt: Optional[str] = None,
            style: Literal['%', '{', '$'] = '%',
            validate: bool = True,
            *,
            defaults: Optional[Dict[str, Any]] = None,
        ) -> None:
            """Initialize, discarding *defaults* argument."""
            super().__init__(fmt, datefmt, style, validate)

else:
    # For Python 3.10 and newer.
    _Formatter = logging.Formatter


class ColorFormatter(_Formatter):
    """Formatter to convert LogRecord into ANSI color terminal text."""
    def __init__(
        self,
        fmt: str,
        datefmt: Optional[str] = None,
        style: Literal['%', '{', '$'] = '%',
        validate: bool = True,
        *,
        defaults: Optional[Dict[str, Any]] = None,
    ) -> None:
        common_args = (datefmt, style, validate)

        # Initialize self as un-colored formatter (for fall-back).
        super().__init__(fmt, *common_args, defaults=defaults)

        # Define short alias for styles dataclass (used in f-strings).
        s = stderr_styles

        # Instantiate color formatters.
        self.color_formatters = {
            logging.INFO: _Formatter(
                f'{s.info}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.WARNING: _Formatter(
                f'{s.warning}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.ERROR: _Formatter(
                f'{s.error}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
            logging.CRITICAL: _Formatter(
                f'{s.critical}{fmt}{s.reset}', *common_args, defaults=defaults
            ),
        }

    def format(self, record: logging.LogRecord) -> str:
        formatter = self.color_formatters.get(record.levelno)
        if formatter:
            return formatter.format(record)
        return super().format(record)  # <- Use un-colored fall-back.


def configure_applogger(applogger: logging.Logger) -> None:
    """Configure handler and formatter for given *applogger*."""
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'cli_formatter': {
                '()': ColorFormatter,
                'fmt': '%(levelname)s: %(message)s',
            },
        },
        'handlers': {
            'cli_handler': {
                'class': 'logging.StreamHandler',
                'formatter': 'cli_formatter',
                'stream': 'ext://sys.stderr',
            },
        },
        'loggers': {
            applogger.name: {
                'handlers': ['cli_handler'],
                'propagate': False,
            },
        },
    })
