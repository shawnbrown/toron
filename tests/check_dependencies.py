"""Verify package dependencies.

To prevent the addition of transitive dependencies from going unnoticed,
this script checks packages that get installed when testing via `tox`.
"""

import os
import sys
from platform import python_implementation
try:
    from importlib.metadata import distributions  # New in Python 3.8
except ModuleNotFoundError:
    sys.stderr.write('Cancelled: Requires Python 3.8 or newer.\n')
    exit(1)  #  <- EXIT with error code!


# Define the package name as a string.
PACKAGE_NAME = 'toron'

# Define a list of expected dependency names.
DEPENDENCY_NAMES = ['lark']
if sys.version_info < (3, 11):
    DEPENDENCY_NAMES.append('typing_extensions')
if sys.platform == 'win32':
    DEPENDENCY_NAMES.append('colorama')

# Define list of names to ignore.
NAMES_TO_IGNORE = ['pip']
if sys.version_info < (3, 12):
    NAMES_TO_IGNORE.append('setuptools')  # Ignore extra packages installed
    NAMES_TO_IGNORE.append('wheel')       # before CPython supported PEP 517.
if python_implementation() == 'PyPy':
    NAMES_TO_IGNORE.append('cffi')        # Ignore packages that are tightly
    NAMES_TO_IGNORE.append('greenlet')    # integrated with PyPy and are
    NAMES_TO_IGNORE.append('hpy')         # always installed with it.


def main():
    if not os.getenv('TOX_ENV_NAME'):
        sys.stderr.write(
            'Cancelled: This script should be run inside a tox environment.\n'
        )
        return 1  # EXIT with error code.

    installed_set = set(dist.metadata['Name'] for dist in distributions())
    installed_set = installed_set - set(NAMES_TO_IGNORE)

    try:
        installed_set.remove(PACKAGE_NAME)
    except KeyError:
        sys.stderr.write(f'Cancelled: The package {PACKAGE_NAME!r} not found.')
        return 1  # EXIT with error code.

    required_set = set(DEPENDENCY_NAMES)

    if installed_set == required_set:
        result_message = 'Dependency check passed!'
        output_stream = sys.stdout
        return_code = 0
    else:
        result_message = 'Dependency check failed.'
        output_stream = sys.stderr
        return_code = 1

    # Build dependency group lists.
    expected_deps = sorted(installed_set & required_set)
    missing_deps = sorted(required_set - installed_set)
    unexpected_deps = sorted(installed_set - required_set)

    # Get output symbols and assure encoding compatibility.
    symbols = ('\u2714', '\u26A0', '\u2718')  # ✔, ⚠, and ✘
    try:
        for x in symbols:
            x.encode(output_stream.encoding)
    except UnicodeEncodeError:
        symbols = ('OK', '!!', 'XX')
    good, warn, bad = symbols

    # Write script output to stream.
    output_stream.write(f'{result_message}\n')
    if expected_deps:
        output_stream.write('\n  Satisfied dependencies:\n')
        for dep in expected_deps:
            output_stream.write(f'  {good} {dep}\n')

    if missing_deps:
        output_stream.write('\n  Missing dependencies:\n')
        for dep in missing_deps:
            output_stream.write(f'  {warn} {dep}\n')

    if unexpected_deps:
        output_stream.write('\n  Unexpected packages:\n')
        for dep in unexpected_deps:
            output_stream.write(f'  {bad} {dep}\n')

    output_stream.write('\n')
    return return_code


if __name__ == '__main__':
    sys.exit(main())
