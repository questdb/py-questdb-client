#!/usr/bin/env python3

import sys
sys.dont_write_bytecode = True
import os
import shutil
import subprocess
import shlex
import pathlib
import glob


PROJ_ROOT = pathlib.Path(__file__).parent


def _run(*args, env=None, cwd=None):
    """
    Log and run a command within the build dir.
    On error, exit with child's return code.
    """
    cwd = cwd or PROJ_ROOT
    args = [str(arg) for arg in args]
    sys.stderr.write('[CMD] ')
    if env is not None:
        env_str = ' '.join(f'{k}={shlex.quote(v)}' for k, v in env.items())
        sys.stderr.write(f'{env_str} ')
        env = {**os.environ, **env}
    escaped_cmd = ' '.join(shlex.quote(arg) for arg in args)
    sys.stderr.write(f'{escaped_cmd}\n')
    ret_code = subprocess.run(args, cwd=str(cwd), env=env).returncode
    if ret_code != 0:
        sys.exit(ret_code)


def _rm(path: pathlib.Path, pattern: str):
    paths = path.glob(pattern)
    for path in paths:
        sys.stderr.write(f'[RM] {path}\n')
        path.unlink()


def _rmtree(path: pathlib.Path):
    if not path.exists():
        return
    sys.stderr.write(f'[RMTREE] {path}\n')
    shutil.rmtree(path, ignore_errors=True)


COMMANDS = set()


def command(fn):
    COMMANDS.add(fn.__name__)
    return fn


@command
def bld():
    _run('python3', 'setup.py', 'build_ext', '--inplace')


@command
def srv(port=None):
    port = port or 8000
    docs_dir = PROJ_ROOT / 'build' / 'docs'
    _run('python3', '-m', 'http.server', port, cwd=docs_dir)


@command
def doc(serve=False):
    _run('python3', '-m', 'sphinx.cmd.build',
        '-b', 'html', 'docs', 'build/docs',
        env={'PYTHONPATH': str(PROJ_ROOT / 'src')})
    if serve:
        srv()


@command
def tst():
    _run('python3', 'test/test.py', '-v',
        env={'TEST_QUESTDB_PATCH_PATH': '1'})


@command
def cibuildwheel(platform, *args):
    _run('python3', '-m',
        'cibuildwheel', '--platform', platform, '--output-dir', 'dist', *args)


@command
def cln():
    _rmtree(PROJ_ROOT / 'build')
    _rmtree(PROJ_ROOT / 'dist')
    _rmtree(PROJ_ROOT / 'c-questdb-client' / 'target')
    _rmtree(PROJ_ROOT / 'c-questdb-client' / 'build')
    _rmtree(PROJ_ROOT / 'src' / 'questdb.egg-info')
    _rmtree(PROJ_ROOT / 'venv')
    _rmtree(PROJ_ROOT / 'wheelhouse')
    _rm(PROJ_ROOT / 'src', '**/*.pyd')
    _rm(PROJ_ROOT / 'src', '**/*.so')
    _rm(PROJ_ROOT / 'src', '**/*.dylib')
    _rm(PROJ_ROOT / 'src', '**/*.c')
    _rm(PROJ_ROOT / 'src', '**/*.html')


def main():
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: python3 proj.py <command>\n')
        sys.stderr.write('Commands:\n')
        for command in COMMANDS:
            sys.stderr.write(f'  {command}\n')
        sys.stderr.write('\n')
        sys.exit(0)
    fn = sys.argv[1]
    args = list(sys.argv)[2:]
    globals()[fn](*args)


if __name__ == '__main__':
    main()
