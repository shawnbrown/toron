"""csv compatibility layer."""
from csv import *

try:
    DictWriter.writeheader  # Added in 3.2
except AttributeError:

    def _writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        self.writerow(header)

    DictWriter.writeheader = _writeheader
