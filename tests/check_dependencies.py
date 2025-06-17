#!/usr/bin/env python3
"""Verify Package Dependencies

This script helps prevent the addition of transitive dependencies from
going unnoticed. It is intended to be run via 'tox' inside an isolated
build environment with no additional dependencies.

1. Configure Script Variables

   Update the `PACKAGE_NAME`, `DEPENDENCY_NAMES`, and `NAMES_TO_IGNORE`
   variables in this script to match your project:

     PACKAGE_NAME = 'mypackage'

     DEPENDENCY_NAMES = ['dependency1', 'dependency2']
     if sys.platform == 'win32':
         DEPENDENCY_NAMES.extend(['dependency3'])

     NAMES_TO_IGNORE = ['pip']
     if sys.version_info < (3, 12):
         NAMES_TO_IGNORE.extend(['setuptools', 'wheel'])

   Note: Complete environment isolation is not always practical.
   Some packages may come pre-installed with a Python build or in
   virtual environments. You can add these to NAMES_TO_IGNORE to
   exclude them from the check.

2. Define a Tox Environment

   Add a "check_deps" environment to your tox configuration:

     [testenv:check_deps]
     description = Check for transitive dependencies.
     isolated_build = true
     commands = python tests/check_dependencies.py

   Important: Do not specify a `deps = ...` setting. This environment
   should only install the dependencies declared by your package.

3. Add to Tox Environment List

   Include the new environment in your `env_list` to ensure it runs
   with other checks:

    [tox]
    env_list = check_deps, lint, 3.1{3,2,1}
    ...
"""

import os
import sys
from platform import python_implementation
try:
    from importlib.metadata import distributions  # New in Python 3.8
except ModuleNotFoundError:
    sys.stderr.write('Cancelled: Requires Python 3.8 or newer.\n')
    sys.exit(1)  # QUIT with failure status!


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


def main(package_name, dependency_names, names_to_ignore, require_tox=True):
    if require_tox and not os.getenv('TOX_ENV_NAME'):
        sys.stderr.write(
            'Cancelled: This script should be run inside a tox environment.\n'
        )
        return 1  # EXIT with failure status.

    installed_set = set(dist.metadata['Name'] for dist in distributions())
    installed_set = installed_set - set(names_to_ignore)

    try:
        installed_set.remove(package_name)
    except KeyError:
        sys.stderr.write(f'Cancelled: The package {package_name!r} not found.\n')
        return 1  # EXIT with failure status.

    required_set = set(dependency_names)

    if installed_set == required_set:
        messages = ['Dependency check passed!']
        output_stream = sys.stdout
        exit_status = 0  # Success status.
    else:
        messages = ['Dependency check failed.']
        output_stream = sys.stderr
        exit_status = 1  # Failure status.

    # Build dependency group lists.
    expected_deps = sorted(installed_set & required_set)
    missing_deps = sorted(required_set - installed_set)
    unexpected_deps = sorted(installed_set - required_set)

    # Define output symbols and assure encoding compatibility.
    try:
        good, warn, bad = ('\u2714', '\u26A0', '\u2718')  # ✔, ⚠, and ✘
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

    return exit_status


if __name__ == '__main__':
    sys.exit(main(PACKAGE_NAME, DEPENDENCY_NAMES, NAMES_TO_IGNORE))
