#!/usr/bin/env python3

import sys

sys.dont_write_bytecode = True
import os
import shutil
import subprocess
import shlex
import pathlib
import glob
import platform

PROJ_ROOT = pathlib.Path(__file__).parent


def _run(*args, env=None, cwd=None):
    """
    Log and run a command within the build dir.
    On error, exit with child's return code.
    """
    args = [str(arg) for arg in args]
    cwd = cwd or PROJ_ROOT
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


def _arg2bool(arg):
    if isinstance(arg, bool):
        return arg
    return arg.lower() in ('true', 'yes', '1')


COMMANDS = []


def command(fn):
    COMMANDS.append(fn.__name__)
    return fn


@command
def build():
    _run('python3', 'setup.py', 'build_ext', '--inplace')


@command
def build_fuzzing():
    _run('python3', 'setup.py', 'build_ext', '--inplace',
        env={'TEST_QUESTDB_FUZZING': '1'})


@command
def test(all=False, patch_path='1', *args):
    _run('cargo', 'test', cwd=PROJ_ROOT / 'pystr-to-utf8')
    env = {'TEST_QUESTDB_PATCH_PATH': patch_path}
    if _arg2bool(all):
        env['TEST_QUESTDB_INTEGRATION'] = '1'
    _run('python3', 'test/test.py', '-v', *args,
         env=env)


@command
def test_fuzzing(*args):
    import atheris
    import pathlib
    lib_path = pathlib.Path(atheris.path()) / 'asan_with_fuzzer.so'
    if not lib_path.exists():
        sys.stderr.write(f'WARNING: {lib_path} not found\n')
        sys.exit(42)
    ld_preload = os.environ.get('LD_PRELOAD', '')
    if ld_preload:
        ld_preload += ':'
    ld_preload += str(lib_path)
    cmd = [
        'python3',
        'test/test_dataframe_fuzz.py'] + list(args)
    if not args:
        cmd.extend([
            '-detect_leaks=0',
            '-rss_limit_mb=32768',
            '-artifact_prefix=fuzz-artifact/',
            '-create_missing_dirs=1'])
    _run(*cmd, env={'LD_PRELOAD': ld_preload})


@command
def benchmark(*args):
    env = {'TEST_QUESTDB_PATCH_PATH': '1'}
    _run('python3', 'test/benchmark.py', '-v', *args, env=env)


@command
def gdb_test(*args):
    env = {'TEST_QUESTDB_PATCH_PATH': '1', 'PYTHONMALLOC': 'malloc'}
    _run('gdb', '-ex', 'r', '--args', 'python3', 'test/test.py', '-v', *args,
         env=env)


@command
def valgrind_test(*args):
    env = {'TEST_QUESTDB_PATCH_PATH': '1', 'PYTHONMALLOC': 'malloc'}
    _run('valgrind', '--leak-check=full', '--show-leak-kinds=all',
         '--track-origins=yes', '--verbose',
         'python3', 'test/test.py', '-v', *args,
         env=env)


@command
def rr_test(*args):
    """
    Linux-only reverse debugger.
    https://github.com/rr-debugger/rr
    https://www.youtube.com/watch?v=61kD3x4Pu8I

    Install rr:
    $ sudo apt install rr
    $ sudo vim /proc/sys/kernel/perf_event_paranoid  # set to -1
    """
    env = {'TEST_QUESTDB_PATCH_PATH': '1'}
    try:
        _run('rr', 'record', 'python3', 'test/test.py', '-v', *args,
             env=env)
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        red = '\033[01;31m'
        reset = '\033[0m'
        sys.stderr.write(f'''\n{red}
            Now first re-run marking stdout/stderr events with a unique ID:
                $ rr -M replay -a
                
            Then re-run inside GDB, running up to a specific event:
                $ rr replay -g $EVENT_ID
                (rr) break ingress.c:9999
                (rr) continue  # or step, next, etc.{reset}\n\n''')
    


@command
def doc(http_serve=False, port=None):
    _run('python3', '-m', 'sphinx.cmd.build',
         '-b', 'html', 'docs', 'build/docs',
         env={'PYTHONPATH': str(PROJ_ROOT / 'src')})
    if _arg2bool(http_serve):
        serve(port)


@command
def serve(port=None):
    port = port or 8000
    docs_dir = PROJ_ROOT / 'build' / 'docs'
    _run('python3', '-m', 'http.server', port, cwd=docs_dir)


@command
def cibuildwheel(*args):
    plat = {
        'win32': 'windows',
        'darwin': 'macos',
        'linux': 'linux'}[sys.platform]
    python = 'python3'
    if sys.platform == 'darwin':
        # Launching with version other than 3.8 will
        # fail saying the 3.8 wheel is unsupported.
        # This is because the 3.8 wheel ends up getting loaded with another
        # Python version.
        python = '/Library/Frameworks/Python.framework/Versions/3.8/bin/python3'
    _run(python, '-m',
         'cibuildwheel',
         '--platform', plat,
         '--output-dir', 'dist',
         '--archs', platform.machine(),
         *args)


@command
def repl(*args):
    _run('python3', env={'PYTHONPATH': str(PROJ_ROOT / 'src')})


@command
def example(name, *args):
    _run('python3', 'examples/' + name + '.py', *args,
         env={'PYTHONPATH': str(PROJ_ROOT / 'src')})


@command
def cw(*args):
    cibuildwheel(args)


@command
def sdist():
    _run('python3', 'setup.py', 'sdist')


@command
def clean():
    _rmtree(PROJ_ROOT / 'build')
    _rmtree(PROJ_ROOT / 'dist')
    _rmtree(PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi' / 'target')
    _rmtree(PROJ_ROOT / 'c-questdb-client' / 'build')
    _rmtree(PROJ_ROOT / 'pystr-to-utf8' / 'target')
    _rmtree(PROJ_ROOT / 'src' / 'questdb.egg-info')
    _rmtree(PROJ_ROOT / 'venv')
    _rmtree(PROJ_ROOT / 'wheelhouse')
    _rm(PROJ_ROOT / 'src', '**/*.pyd')
    _rm(PROJ_ROOT / 'src', '**/*.so')
    _rm(PROJ_ROOT / 'src', '**/*.dylib')
    _rm(PROJ_ROOT / 'src', '**/*.c')
    _rm(PROJ_ROOT / 'src', '**/*.html')
    _rm(PROJ_ROOT, 'rustup-init.exe')
    _rm(PROJ_ROOT, 'rustup-init.sh')


@command
def venv():
    if pathlib.Path('venv').exists():
        sys.stderr.write('venv already exists, delete it, or run command clean\n')
        return
    _run('python3', '-m', 'venv', 'venv')
    _run('venv/bin/python3', '-m', 'pip', 'install', '-U', 'pip')
    _run('venv/bin/python3', '-m', 'pip', 'install', '-r', 'dev_requirements.txt')
    sys.stdout.write('NOTE: remember to activate the environment: source venv/bin/activate\n')


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
