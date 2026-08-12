"""Run a Python test script with CI hang diagnostics enabled."""

import faulthandler
import functools
import os
import platform
from pathlib import Path
import runpy
import subprocess
import sys
import unittest


DEFAULT_WATCHDOG_SECONDS = 15 * 60
WATCHDOG_SECONDS_ENV = 'QDB_TEST_WATCHDOG_SECONDS'


def watchdog_seconds():
    value = os.environ.get(
        WATCHDOG_SECONDS_ENV,
        str(DEFAULT_WATCHDOG_SECONDS))
    try:
        seconds = int(value)
    except ValueError:
        raise SystemExit(
            f'{WATCHDOG_SECONDS_ENV} must be a positive integer, got {value!r}')
    if seconds <= 0:
        raise SystemExit(
            f'{WATCHDOG_SECONDS_ENV} must be a positive integer, got {value!r}')
    return seconds


def print_environment():
    print('=== test environment ===', file=sys.stderr, flush=True)
    print(f'executable: {sys.executable}', file=sys.stderr, flush=True)
    print(f'python: {sys.version}', file=sys.stderr, flush=True)
    print(f'platform: {platform.platform()}', file=sys.stderr, flush=True)
    print('packages (pip freeze --all):', file=sys.stderr, flush=True)
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pip', 'freeze', '--all'],
            stdout=sys.stderr,
            stderr=sys.stderr,
            check=False)
    except OSError as error:
        print(
            f'warning: unable to run pip freeze: {error}',
            file=sys.stderr,
            flush=True)
    else:
        if result.returncode != 0:
            print(
                f'warning: pip freeze exited with status {result.returncode}',
                file=sys.stderr,
                flush=True)
    print('=== end test environment ===', file=sys.stderr, flush=True)


def arm_watchdog(seconds):
    faulthandler.cancel_dump_traceback_later()
    faulthandler.dump_traceback_later(
        seconds,
        repeat=False,
        exit=True)


def install_unittest_watchdog(seconds):
    original_start_test = unittest.TestResult.startTest

    @functools.wraps(original_start_test)
    def start_test(result, test):
        arm_watchdog(seconds)
        return original_start_test(result, test)

    unittest.TestResult.startTest = start_test
    return original_start_test


def run_test_script(test_script, test_args):
    original_argv = sys.argv
    original_path = sys.path[:]
    sys.argv = [str(test_script), *test_args]
    sys.path.insert(0, str(test_script.parent))
    try:
        runpy.run_path(str(test_script), run_name='__main__')
    finally:
        sys.argv = original_argv
        sys.path[:] = original_path


def main():
    if len(sys.argv) < 2:
        print(
            f'usage: {Path(sys.argv[0]).name} TEST_SCRIPT [TEST_ARG ...]',
            file=sys.stderr)
        return 2

    test_script = Path(sys.argv[1]).resolve()
    if not test_script.is_file():
        print(f'test script does not exist: {test_script}', file=sys.stderr)
        return 2

    seconds = watchdog_seconds()
    print_environment()
    faulthandler.enable(all_threads=True)
    original_start_test = install_unittest_watchdog(seconds)
    print(
        f'Test watchdog armed for {seconds} seconds without test progress.',
        file=sys.stderr,
        flush=True)
    arm_watchdog(seconds)
    try:
        run_test_script(test_script, sys.argv[2:])
    finally:
        faulthandler.cancel_dump_traceback_later()
        unittest.TestResult.startTest = original_start_test
    return 0


if __name__ == '__main__':
    sys.exit(main())
