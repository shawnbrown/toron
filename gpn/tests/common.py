# -*- coding: utf-8 -*-
import glob
import os
import tempfile

from gpn.tests import _unittest as unittest


class MkdtempTestCase(unittest.TestCase):
    # TestCase changes cwd to temporary location.  After testing,
    # removes files and restores original cwd.
    @classmethod
    def setUpClass(cls):
        cls._orig_dir = os.getcwd()
        cls._temp_dir = tempfile.mkdtemp()  # Requires mkdtemp--cannot
        os.chdir(cls._temp_dir)             # use TemporaryDirectory.

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls._orig_dir)
        os.rmdir(cls._temp_dir)

    def setUp(self):
        self._no_class_fixtures = not hasattr(self, '_temp_dir')
        if self._no_class_fixtures:
            self.setUpClass.__func__(self)

    def tearDown(self):
        self._remove_tempfiles()
        if self._no_class_fixtures:
            self.tearDownClass.__func__(self)

    def _remove_tempfiles(self):
        for path in glob.glob(os.path.join(self._temp_dir, '*')):
            os.remove(path)
