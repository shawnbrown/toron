#!/usr/bin/env python3
import os
import subprocess
import sys


args = [
    sys.executable or 'python',  # Python interpreter to call for testing.
    '-B',  # Don't write .pyc files on import.
    '-m', 'unittest', # Run the unittest module as a script.
    'discover',  # Use test discovery.
    '-s', 'tests',  # Start discovery in 'tests' directory.
    '-t', os.path.dirname(__file__),  # Set top-level of project.
]
args.extend(sys.argv[1:])  # Append any arguments passed to script (-v, etc.).
sys.exit(subprocess.call(args))

