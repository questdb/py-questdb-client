import sys
import subprocess
import shlex
import textwrap
import platform
import sysconfig
import argparse
import importlib.metadata

arg_parser = argparse.ArgumentParser(
    prog='pip_install_deps.py',
    description='installs dependencies'
)

arg_parser.add_argument('--pandas-version')
arg_parser.add_argument(
    '--min-versions',
    action='store_true',
    help='install the oldest dependency versions that the docs promise')
arg_parser.add_argument(
    '--with-duckdb',
    action='store_true',
    help='also install duckdb, which only the system tests use')

# The oldest versions that the `dataframe`, `polars` and `duckdb` extras in
# `pyproject.toml` and `docs/installation.rst` promise. Keep them in sync. NumPy stays on 1.x
# here; the NumPy floor (1.21) has its own CI jobs.
MIN_VERSIONS = {
    'numpy': '1.26.4',
    'pandas': '2.2.0',
    'pyarrow': '14.0.1',
    'polars': '1.32.0',
    'fastparquet': '2023.10.1',
}
MIN_DUCKDB_VERSION = '1.2.0'
arg_parser.add_argument('--pyarrow-version')
arg_parser.add_argument('--polars-version')


class UnsupportedDependency(Exception):
    pass


def pip_install(package, version=None):
    """Install `package` (a requirement string) or a list of requirement
    strings in one pip call."""
    if isinstance(package, str):
        requirements = [
            package if version is None else f'{package}=={version}']
    else:
        requirements = list(package)
    args = [
        sys.executable,
        '-m', 'pip', 'install',
        '--upgrade',
        '--only-binary', ':all:',
        *requirements
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


def pip_install_required(package, version):
    try:
        pip_install(package, version)
    except UnsupportedDependency as e:
        raise SystemExit(
            f'Required dependency {package}=={version} is not installable:\n'
            f'{e}') from None
    installed = importlib.metadata.version(package)
    if installed != version:
        raise SystemExit(
            f'Required dependency {package}=={version} was requested, but '
            f'version {installed} is installed')


def ensure_timezone():
    try:
        import zoneinfo
        if platform.system() == 'Windows':
            pip_install('tzdata')  # for zoneinfo
    except ImportError:
        pip_install('pytz')


def install_pandas2_dependencies(pandas_version=None):
    if pandas_version is not None:
        pip_install_required('pandas', pandas_version)
    else:
        try_pip_install('pandas>=2.2,<3')
    try_pip_install('numpy<2')
    # PyArrow 26 requires NumPy 2. Keep this lane testing NumPy 1.x.
    try_pip_install('pyarrow<26')


def install_pandas3_dependencies():
    try_pip_install('pandas>=3,<4')
    try_pip_install('numpy>=2,<3')
    try_pip_install('pyarrow')


def should_use_pandas3(py_version=None):
    if py_version is None:
        py_version = sys.version_info[:2]
    return py_version >= (3, 11)


def install_default_dataframe_dependencies():
    # Pandas 3 currently requires Python 3.11+, so keep 3.10 wheel tests on
    # the pandas 2 / numpy 1.x-compatible path unless explicitly overridden.
    if should_use_pandas3():
        install_pandas3_dependencies()
    else:
        install_pandas2_dependencies()


def install_min_versions(with_duckdb):
    pins = dict(MIN_VERSIONS)
    if with_duckdb:
        pins['duckdb'] = MIN_DUCKDB_VERSION
    # One pip call, so that the resolver sees every pin together. Use
    # `pip_install`, not `try_pip_install`: a missing wheel must fail the job.
    pip_install([f'{name}=={version}' for name, version in pins.items()])
    pip_install('psutil')
    return pins


def check_versions(pins):
    import importlib.metadata
    wrong = []
    for name, version in pins.items():
        found = importlib.metadata.version(name)
        if found != version:
            wrong.append(f'{name}: want {version}, found {found}')
    if wrong:
        sys.stderr.write('Wrong dependency versions:\n  ' +
                         '\n  '.join(wrong) + '\n')
        sys.exit(1)


def main(args):
    has_pandas_version = (
        args.pandas_version is not None and args.pandas_version != '')
    if args.min_versions and has_pandas_version:
        arg_parser.error('--min-versions and --pandas-version exclude '
                         'each other')
    ensure_timezone()
    pip_install('pip')
    pip_install('setuptools')
    pip_install('packaging')
    if args.min_versions:
        check_versions(install_min_versions(args.with_duckdb))
    else:
        if has_pandas_version:
            install_pandas2_dependencies(args.pandas_version)
        else:
            install_default_dataframe_dependencies()

        try_pip_install('fastparquet>=2023.10.1')
        # pyarrow is installed with pandas above: `<26` on the NumPy 1.x
        # lane, latest on the pandas 3 lane. A pin overrides it.
        if args.pyarrow_version:
            pip_install_required('pyarrow', args.pyarrow_version)
        if args.polars_version:
            pip_install_required('polars', args.polars_version)
        else:
            try_pip_install('polars')
        try_pip_install('psutil')
        if args.with_duckdb:
            # A hard failure: the duckdb system tests skip without it,
            # which would hide a missing wheel.
            pip_install('duckdb')

    # Optional OIDC extra. Without it `_qr_ascii` / `_qr_data_uri` return None,
    # so every `qr=True` path in the renderers -- and every test that exercises
    # one -- silently does nothing. Installing it here is what makes that
    # coverage real rather than notional.
    #
    # Pillow matters for the same reason: bare `qrcode` gives the terminal
    # renderer's ASCII art, but `_qr_data_uri` needs Pillow to encode a PNG, so
    # the JUPYTER renderer's QR path stays notional without it.
    #
    # Two calls, deliberately. `qrcode[pil]` is ONE pip resolution: on a target
    # with no Pillow wheel it fails as a unit, so `try_pip_install` swallowed it
    # and `qrcode` itself was never installed -- losing BOTH QR tests, not the
    # one the old comment claimed. Worse, a Pillow source build that fails any
    # other way does not match `pip_install`'s "unsupported" patterns, so it
    # calls `sys.exit()` and hard-fails the whole dependency step. Installing
    # them separately degrades the way the comment always said it did: no
    # Pillow costs the Jupyter QR test only.
    try_pip_install('qrcode')
    try_pip_install('pillow')
    # The PG-wire OIDC adapters (`questdb.auth.sqlalchemy_engine` /
    # `psycopg_connect`). Without a real SQLAlchemy and driver their tests can
    # only inject stand-in modules, which never runs SQLAlchemy's actual
    # `do_connect` contract or libpq's handshake; `AdapterRealDriverTest`
    # skips itself when either is missing. `psycopg[binary]` bundles libpq, so
    # a platform without a binary wheel (PyPy, 32-bit) fails the install as a
    # unit and is skipped rather than left with a driver that cannot import.
    try_pip_install('sqlalchemy>=2')
    try_pip_install('psycopg[binary]')
    # The greenlet scoped-call test skips without it.
    try_pip_install('gevent')
    # `TestManifest` reads `examples.manifest.yaml` and skips without it.
    try_pip_install('pyyaml')

    on_linux_is_glibc = (
            (not platform.system() == 'Linux') or
            (platform.libc_ver()[0] == 'glibc'))
    is_64bits = sys.maxsize > 2 ** 32
    is_cpython = platform.python_implementation() == 'CPython'
    is_final = sys.version_info.releaselevel == 'final'
    is_free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))
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

        # PyYAML publishes no wheel for free-threaded 3.13.
        if not (is_free_threaded and py_version == (3, 13)):
            import yaml


if __name__ == "__main__":
    args = arg_parser.parse_args()
    main(args)
