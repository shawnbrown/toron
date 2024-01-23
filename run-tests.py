#!/usr/bin/env python3
import os
import subprocess
import sys


args = [
    sys.executable or 'python',  # Get Python interpreter to call for testing.
    '-B',                        # Don't write .pyc files on import.
    '-O',                        # Remove assert and __debug__-dependent statements.
    '-W', 'default',             # Enable default handling for all warnings.
    '-m', 'unittest',            # Run the unittest module as a script.
    'discover',                  # Use test discovery.
    '-s', 'tests',               # Start discovery in 'tests' directory.
    '-t', '.',                   # Set top level of project to working directory.
]
args.extend(sys.argv[1:])  # Append any arguments passed to script (-v, etc.).

cwd = os.path.dirname(__file__) or '.'  # Get working directory for subprocess.

env = os.environ.copy()
env['PYTHONPATH'] = 'src'  # Make package importable from 'src' directory.

sys.exit(subprocess.call(args=args, cwd=cwd, env=env))
