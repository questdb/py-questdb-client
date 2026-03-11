import sys
import subprocess
import shlex
import textwrap
import platform
import argparse

arg_parser = argparse.ArgumentParser(
    prog='pip_install_deps.py',
    description='installs dependencies'
)

arg_parser.add_argument('--pandas-version')


class UnsupportedDependency(Exception):
    pass


def pip_install(package, version=None):
    args = [
        sys.executable,
        '-m', 'pip', 'install',
        '--upgrade',
        '--only-binary', ':all:',
        package if version is None else f'{package}=={version}'
    ]
    args_s = ' '.join(shlex.quote(arg) for arg in args)
    sys.stderr.write(args_s + '\n')
    res = subprocess.run(
        args,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE)
    if res.returncode == 0:
        return
    output = res.stdout.decode('utf-8')
    is_unsupported = (
            ('Could not find a version that satisfies the requirement' in output) or
            ('The conflict is caused by' in output))
    if is_unsupported:
        raise UnsupportedDependency(output)
    else:
        sys.stderr.write(output + '\n')
        sys.exit(res.returncode)


def try_pip_install(package, version=None):
    try:
        pip_install(package, version)
    except UnsupportedDependency as e:
        msg = textwrap.indent(str(e), ' ' * 8)
        sys.stderr.write(f'    Ignored unsatisfiable dependency:\n{msg}\n')


def ensure_timezone():
    try:
        import zoneinfo
        if platform.system() == 'Windows':
            pip_install('tzdata')  # for zoneinfo
    except ImportError:
        pip_install('pytz')


def install_pandas2_and_numpy(pandas_version=None):
    if pandas_version is not None:
        try_pip_install('pandas', pandas_version)
    else:
        try_pip_install('pandas>=2,<3')
    try_pip_install('numpy<2')


def install_pandas3_and_numpy():
    try_pip_install('pandas>=3')
    try_pip_install('numpy>=2')


def should_use_pandas3(py_version=None):
    if py_version is None:
        py_version = sys.version_info[:2]
    return py_version >= (3, 11)


def install_default_pandas_and_numpy():
    # Pandas 3 currently requires Python 3.11+, so keep 3.10 wheel tests on
    # the pandas 2 / numpy 1.x-compatible path unless explicitly overridden.
    if should_use_pandas3():
        install_pandas3_and_numpy()
    else:
        install_pandas2_and_numpy()


def main(args):
    ensure_timezone()
    pip_install('pip')
    pip_install('setuptools')
    pip_install('packaging')
    if args.pandas_version is not None and args.pandas_version != '':
        install_pandas2_and_numpy(args.pandas_version)
    else:
        install_default_pandas_and_numpy()

    try_pip_install('fastparquet>=2023.10.1')
    try_pip_install('pyarrow')

    on_linux_is_glibc = (
            (not platform.system() == 'Linux') or
            (platform.libc_ver()[0] == 'glibc'))
    is_64bits = sys.maxsize > 2 ** 32
    is_cpython = platform.python_implementation() == 'CPython'
    is_final = sys.version_info.releaselevel == 'final'
    py_version = (sys.version_info.major, sys.version_info.minor)
    if on_linux_is_glibc and is_64bits and is_cpython and is_final:
        # Ensure that we've managed to install the expected dependencies.
        import pandas
        import numpy
        import pyarrow

        # Temporarily don't require fastparquet on 3.14
        # Compat will still be tested on older releases.
        if py_version < (3, 14):
            import fastparquet


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
