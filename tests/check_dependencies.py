"""Verify package dependencies.

To prevent the addition of transitive dependencies from going unnoticed,
we check the packages that get installed when testing via `tox`.
"""

import os
import subprocess
import sys


EXITCODE_OK = 0
EXITCODE_ERR = 1


def main(package_dependencies):
    if not os.getenv('TOX_ENV_NAME'):
        sys.stderr.write(
            'This script should be run inside a tox environment.\n'
        )
        return EXITCODE_ERR  # <- EXIT!

    if not sys.executable:
        sys.stderr.write(
            'Cannot determine executable binary for current '
            'Python interpreter, `sys.executable` is empty.\n'
        )
        return EXITCODE_ERR  # <- EXIT!

    result = subprocess.run(
        args=[
            sys.executable,        # Execute current Python interpreter.
            '-m', 'pip',           # Run pip using module-as-script interface.
            'freeze',              # List installed packages.
            '--local',             # Omit globally-installed packages.
            '--exclude', 'toron',  # Exclude Toron itself.
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    result_text = result.stdout.strip()
    result_list = result_text.split('\n')
    installed_packages = {x.partition('==')[0] for x in result_list}

    if installed_packages == package_dependencies:
        package_text = '\n'.join(f'* {x}' for x in sorted(installed_packages))
        sys.stdout.write(
            f'Dependency check passed!\n'
            f'Installed dependencies include:\n'
            f'{package_text}\n'
        )
        return EXITCODE_OK  # <- EXIT!

    sys.stderr.write('Dependency check failed.\n')

    missing = package_dependencies - installed_packages
    extra = installed_packages - package_dependencies
    if missing:
        missing_text = '\n'.join(f'* {x}' for x in sorted(missing))
        sys.stderr.write(
            f'\nMissing expected packages:\n'
            f'{missing_text}\n'
        )
    if extra:
        extra_text = '\n'.join(f'* {x}' for x in sorted(extra))
        sys.stderr.write(
            f"\nFound extra unexpected packages:\n"
            f'{extra_text}\n'
        )

    sys.stderr.write('\n')
    return EXITCODE_ERR


if __name__ == '__main__':
    package_dependencies = {
        'lark',
    }
    if sys.version_info < (3, 11):
        package_dependencies.add('typing_extensions')

    if sys.platform == 'win32':
        package_dependencies.add('colorama')

    exitcode = main(package_dependencies)
    sys.exit(exitcode)
