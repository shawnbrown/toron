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


# Package name.
PACKAGE_NAME = 'toron'

# List of package dependencies.
DEPENDENCY_NAMES = ['lark']
if sys.version_info < (3, 11):
    DEPENDENCY_NAMES.extend(['typing_extensions'])
if sys.platform == 'win32':
    DEPENDENCY_NAMES.extend(['colorama'])

# List of names to ignore (packages included with test environment).
NAMES_TO_IGNORE = ['pip']
if sys.version_info < (3, 12):
    NAMES_TO_IGNORE.extend(['setuptools', 'wheel'])
if python_implementation() == 'PyPy':
    NAMES_TO_IGNORE.extend(['cffi', 'greenlet', 'hpy'])


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
        messages = ['Dependency check passed!']
        output_stream = sys.stdout
        return_code = 0
    else:
        messages = ['Dependency check failed.']
        output_stream = sys.stderr
        return_code = 1

    # Build dependency group lists.
    expected_deps = sorted(installed_set & required_set)
    missing_deps = sorted(required_set - installed_set)
    unexpected_deps = sorted(installed_set - required_set)

    # Get output symbols and assure encoding compatibility.
    good, warn, bad = ('\u2714', '\u26A0', '\u2718')  # ✔, ⚠, and ✘
    try:
        f'{good}{warn}{bad}'.encode(output_stream.encoding, errors='strict')
    except UnicodeEncodeError:
        good, warn, bad = ('OK', '!!', 'XX')

    # Add messages for dependency groups.
    if expected_deps:
        messages.append('\n  Satisfied dependencies:')
        messages.extend(f'  {good} {dep}' for dep in expected_deps)

    if missing_deps:
        messages.append('\n  Missing dependencies:')
        messages.extend(f'  {warn} {dep}' for dep in missing_deps)

    if unexpected_deps:
        messages.append('\n  Unexpected packages:')
        messages.extend(f'  {bad} {dep}' for dep in unexpected_deps)

    # Write messages to output stream.
    for msg in messages:
        output_stream.write(f'{msg}\n')
    output_stream.write('\n')

    return return_code


if __name__ == '__main__':
    sys.exit(main())
