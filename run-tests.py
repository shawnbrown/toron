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
]

# Append arguments passed to script or configure test discovery.
args.extend(sys.argv[1:] or ['discover', '-s', 'tests', '-t', '.'])

cwd = os.path.dirname(__file__) or '.'  # Get working directory for subprocess.

env = os.environ.copy()
env['PYTHONPATH'] = 'src'  # Make package importable from 'src' directory.

sys.exit(subprocess.call(args=args, cwd=cwd, env=env))
