#!/usr/bin/env python3
"""Run the Arrow lock guard before delegating to cibuildwheel."""

import pathlib
import subprocess
import sys


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]


def main():
    subprocess.check_call([
        sys.executable,
        str(PROJECT_ROOT / 'c-questdb-client' / 'ci' /
            'check_arrow_ffi_lock.py'),
    ])
    subprocess.check_call([
        sys.executable,
        '-m',
        'cibuildwheel',
        *sys.argv[1:],
    ], cwd=PROJECT_ROOT)


if __name__ == '__main__':
    main()
