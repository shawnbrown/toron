#!/usr/bin/env python3
import os
import subprocess
import sys


# Prepare subprocess arguments for testing.
args = [
    sys.executable or 'python',  # Get Python interpreter to call for testing.
    '-B',                        # Don't write .pyc files on import.
    '-O',                        # Remove assert and __debug__-dependent statements.
    '-W', 'default',             # Enable default handling for all warnings.
    #'-W', 'error',               # Convert all warnings to errors.
    '-m', 'unittest',            # Run the unittest module as a script.
]

# Include arguments passed to script or configure test discovery.
args.extend(sys.argv[1:] or ['discover', '-s', 'tests', '-t', '.'])

# Get working directory for subprocess.
cwd = os.path.dirname(__file__) or '.'

# Make package importable from 'src' directory.
env = os.environ.copy()
env['PYTHONPATH'] = 'src'

# Run tests in subprocess.
sys.exit(subprocess.call(args=args, cwd=cwd, env=env))
