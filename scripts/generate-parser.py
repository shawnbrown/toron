#!/usr/bin/env python3
"""Generate a stand-alone LALR(1) parser for attribute selectors.

If the grammar and parser files have the same last-modified time,
then the rebuild is skipped--the existing parser file is left
as-is. If the last-modified dates are different, the stand-alone
parser is regenerated and its last modified date is changed to
match that of the grammar file.

You can generate a stand-alone parser from the command line by
starting in the project's root directory and running the following
commands:

  $ cd toron/
  $ python -m lark.tools.standalone _selectors_grammar.lark > _selectors_parser.py

"""
import os
import subprocess
import sys
import time


# File paths relative to project root.
GRAMMAR_FILE = 'toron/_selectors_grammar.lark'
OUTPUT_FILE = 'toron/_selectors_parser.py'
PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))

time_format = '%Y-%m-%d %I:%M:%S %p'

# Change working directory to the project root.
os.chdir(PROJECT_ROOT)

# Get the last-modified time of grammar and parser files.
grammar_mtime = os.path.getmtime(GRAMMAR_FILE)
try:
    parser_mtime = os.path.getmtime(OUTPUT_FILE)
except OSError:
    parser_mtime = None

# If last-modified times are the same, then exit early.
if grammar_mtime == parser_mtime:
    grammar_time_string = time.strftime(time_format, time.localtime(grammar_mtime))
    print(f'Lark files in-sync, modified {grammar_time_string} (skipping build).')
    print(f'  grammar - {GRAMMAR_FILE}')
    print(f'   parser - {OUTPUT_FILE}')
    sys.exit(0)  # Exit status `0` for success.

# Regenerate parser.
args = [
    sys.executable or 'python',  # Python interpreter to call.
    '-B',  # Don't write .pyc files on import.
    '-m', 'lark.tools.standalone', # Run Lark's standalone generator.
    GRAMMAR_FILE,  # Specify grammar file.
]
print(f'Lark grammar updated, generating stand-alone parser...')
print(f'  grammar - {GRAMMAR_FILE}')
print(f'   parser - {OUTPUT_FILE}', end=' ', flush=True)
with open(OUTPUT_FILE, 'w') as f:
    f.write('# type: ignore\n\n')
    f.flush()
    exit_status = subprocess.call(args, stdout=f)
print(f'(rebuilt)')

# Set parser's last-modified time to match grammar's.
os.utime(OUTPUT_FILE, (grammar_mtime, grammar_mtime))

sys.exit(exit_status)

