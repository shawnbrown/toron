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
    @staticmethod
    def get_installed_packages():
        """Return a set of installed package names (minus some commonly
        pre-installed ones).
        """
        args = [
            # List installed packages.
            'python', '-m', 'pip', 'freeze',

            # Exclude Toron itself.
            '--exclude', 'toron',

            # Exclude packages that are pre-installed in some Python builds.
            '--exclude', 'cffi',
            '--exclude', 'greenlet',
            '--exclude', 'hpy',
            '--exclude', 'readline',
        ]
        result = subprocess.run(args, stdout=subprocess.PIPE, text=True)
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
