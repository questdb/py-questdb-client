import sys
import subprocess
import shlex
import textwrap
import platform


class UnsupportedDependency(Exception):
    pass


def pip_install(package):
    args = [
        sys.executable,
        '-m', 'pip', 'install',
        '--upgrade',
        '--only-binary', ':all:',
        package]
    args_s = ' '.join(shlex.quote(arg) for arg in args)
    sys.stderr.write(args_s + '\n')
    res = subprocess.run(
        args,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE)
    if res.returncode == 0:
        return
    output = res.stdout.decode('utf-8')
    if 'Could not find a version that satisfies the requirement' in output:
        raise UnsupportedDependency(output)
    else:
        sys.stderr.write(output + '\n')
        sys.exit(res.returncode)


def try_pip_install(package):
    try:
        pip_install(package)
    except UnsupportedDependency as e:
        msg = textwrap.indent(str(e), ' ' * 8)
        sys.stderr.write(f'    Ignored unsatisfiable dependency:\n{msg}\n')


def ensure_zoneinfo():
    try:
        import zoneinfo
    except ImportError:
        pip_install('backports.zoneinfo')
        from backports import zoneinfo


def main():
    ensure_zoneinfo()
    if platform.system() == 'Windows':
        pip_install('tzdata')  # for zoneinfo

    try_pip_install('pandas')
    try_pip_install('numpy')
    try_pip_install('pyarrow')

    if platform.python_implementation() == 'CPython':
        # We import the dependencies we expect to have correctly installed.
        import pandas
        import numpy
        import pyarrow


if __name__ == "__main__":
    main()
