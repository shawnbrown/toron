"""Tests for installed package dependencies.

To prevent the addition of transitive dependencies from going unnoticed,
we check the packages that get installed when testing via `tox`.
"""

import os
import subprocess
import sys
import unittest


@unittest.skipUnless(os.getenv('TOX_ENV_NAME'), 'requires tox environment')
class TestPackageDependencies(unittest.TestCase):
    def get_installed_packages(self):
        """Return installed package names as a set of strings."""
        if not sys.executable:
            self.fail(
                'cannot determine executable binary for current '
                'Python interpreter, `sys.executable` is empty'
            )

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
        package_names = [x.partition('==')[0] for x in result_list]
        return set(package_names)

    def test_package_dependencies(self):
        """Check for expected package dependencies."""
        package_dependencies = {
            'lark',
        }
        if sys.version_info < (3, 11):
            package_dependencies.add('typing_extensions')

        self.assertEqual(self.get_installed_packages(), package_dependencies)
