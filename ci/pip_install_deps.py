import sys
import subprocess
import shlex
import textwrap
import platform


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


def main(*args, **kwargs):
    ensure_timezone()
    pip_install('pip')
    pip_install('setuptools')
    try_pip_install('fastparquet>=2023.10.1')
    if 'pandasVersion' in kwargs and kwargs['pandasVersion'] != '':
        try_pip_install('pandas', kwargs["pandasVersion"])
    else:
        try_pip_install('pandas')
    try_pip_install('numpy')
    try_pip_install('pyarrow')


    on_linux_is_glibc = (
        (not platform.system() == 'Linux') or
        (platform.libc_ver()[0] == 'glibc'))
    is_64bits = sys.maxsize > 2**32
    is_cpython = platform.python_implementation() == 'CPython'
    is_windows_py3_12 = (
        # https://github.com/dask/fastparquet/issues/892
        platform.system() == 'Windows' and
        sys.version_info >= (3, 12))
    if on_linux_is_glibc and is_64bits and is_cpython:
        # Ensure that we've managed to install the expected dependencies.
        import pandas
        import numpy
        import pyarrow
        if (sys.version_info >= (3, 8)) and (not is_windows_py3_12):
            import fastparquet


if __name__ == "__main__":
    main()
