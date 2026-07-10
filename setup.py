#!/usr/bin/env python3

import pathlib
import sys
import os
import shutil
import platform
import numpy as np

from setuptools import setup, find_packages
from setuptools.extension import Extension
from setuptools.command.build_ext import build_ext
import subprocess
from Cython.Build import cythonize

from install_rust import cargo_path, install_rust, export_cargo_to_path


PROJ_ROOT = pathlib.Path(__file__).parent
PLATFORM = sys.platform
MODE = platform.architecture()[0]  # '32bit' or '64bit'
WIN_32BIT_CARGO_TARGET = 'i686-pc-windows-msvc'


INSTRUMENT_FUZZING = False
if os.environ.get('TEST_QUESTDB_FUZZING') == '1':
    INSTRUMENT_FUZZING = True
    ORIG_CC = os.environ.get('CC')
    os.environ['CC'] = "clang"
    ORIG_CXX = os.environ.get('CXX')
    os.environ['CXX'] = "clang++"


def cargo_target_release_dir(crate_dir):
    """Locate a crate's cargo `release/` output dir, honouring
    `CARGO_TARGET_DIR` when set so every crate can share one target dir."""
    env_target = os.environ.get('CARGO_TARGET_DIR')
    base = pathlib.Path(env_target) if env_target else (crate_dir / 'target')
    if PLATFORM == 'win32' and MODE == '32bit':
        return base / WIN_32BIT_CARGO_TARGET / 'release'
    return base / 'release'


def client_extension():
    lib_prefix = ''
    lib_suffix = ''
    lib_paths = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []
    extra_objects = []

    questdb_rs_ffi_dir = PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi'
    rpyutils_dir = PROJ_ROOT / 'rpyutils'
    questdb_client_lib_dir = cargo_target_release_dir(questdb_rs_ffi_dir)
    rpyutils_lib_dir = cargo_target_release_dir(rpyutils_dir)
    if not (PLATFORM == 'win32' and MODE == '32bit'):
        if INSTRUMENT_FUZZING:
            extra_compile_args.append('-fsanitize=fuzzer-no-link')
            extra_link_args.append('-fsanitize=fuzzer-no-link')
        else:
            extra_compile_args.append('-flto')
            extra_link_args.append('-flto')

    if PLATFORM == 'darwin':
        lib_prefix = 'lib'
        lib_suffix = '.a'
        extra_link_args.extend(['-framework', 'Security', '-framework', 'CoreFoundation'])
    elif PLATFORM == 'win32':
        lib_prefix = ''
        lib_suffix = '.lib'
        libraries.extend(['wsock32', 'ws2_32', 'ntdll', 'AdvAPI32', 'bcrypt', 'UserEnv', 'crypt32', 'Secur32', 'NCrypt'])
    elif PLATFORM == 'linux':
        lib_prefix = 'lib'
        lib_suffix = '.a'
    else:
        raise NotImplementedError(f'Unsupported platform: {PLATFORM}')

    extra_objects = [
        str(loc / f'{lib_prefix}{name}{lib_suffix}')
        for loc, name in (
            (questdb_client_lib_dir, 'questdb_client'),
            (rpyutils_lib_dir, 'rpyutils'))]
    depends = list(extra_objects)

    return Extension(
        "questdb._client",
        ["src/questdb/_client.pyx"],
        include_dirs=[
            "c-questdb-client/include",
            "rpyutils/include",
            np.get_include()],
        library_dirs=lib_paths,
        libraries=libraries,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        extra_objects=extra_objects,
        depends=depends,
        define_macros = [
            ('NPY_NO_DEPRECATED_API', 'NPY_1_7_API_VERSION'),
            ('QUESTDB_CLIENT_HAS_ARROW', '1'),
            ('QUESTDB_CLIENT_ENABLE_ARROW', '1'),
        ]
    )


def cargo_build():
    if not (PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi').exists():
        if os.environ.get('SETUP_DO_GIT_SUBMODULE_INIT') == '1':
            # Non-recursive: the wheel needs only `c-questdb-client`, not its
            # nested `questdb` Java-server submodule (a large, useless clone).
            subprocess.check_call([
                'git', 'submodule', 'update', '--init', 'c-questdb-client'])
        else:
            sys.stderr.write('Could not find `c-questdb-client` submodule.\n')
            sys.stderr.write('You might need to run:\n')
            sys.stderr.write('    git submodule update --init --recursive\n')
            sys.stderr.write('\n')
            sys.stderr.write('Alternatively specify the '
                '`SETUP_DO_GIT_SUBMODULE_INIT=1` env variable\n')
            sys.exit(1)

    if shutil.which('cargo') is None:
        if cargo_path().exists():
            export_cargo_to_path()
        elif os.environ.get('SETUP_DO_RUSTUP_INSTALL') == '1':
            install_rust()
            export_cargo_to_path()
        else:
            sys.stderr.write('Could not find the `cargo` executable.\n')
            sys.stderr.write('You may install it via http://rustup.rs/.\n')
            sys.stderr.write('\n')
            sys.stderr.write('Alternatively specify the '
                '`SETUP_DO_RUSTUP_INSTALL=1` env variable\n')
            sys.exit(1)

    cargo_args = [
        'cargo',
        'build',
        '--release']

    if PLATFORM == 'win32' and MODE == '32bit':
        cargo_args.append(f'--target={WIN_32BIT_CARGO_TARGET}')

    env = os.environ.copy()
    if INSTRUMENT_FUZZING:
        if ORIG_CC is not None:
            env['CC'] = ORIG_CC
        else:
            del env['CC']
        if ORIG_CXX is not None:
            env['CXX'] = ORIG_CXX
        else:
            del env['CXX']
    # `insecure-skip-verify` exposes `tls_verify=unsafe_off`, which disables TLS
    # certificate verification. It must never be compiled into shipped wheels;
    # opt in explicitly (test harnesses, MITM debugging) via the env var.
    features = ['confstr-ffi', 'arrow']
    if os.environ.get('QUESTDB_INSECURE_SKIP_VERIFY') == '1':
        features.append('insecure-skip-verify')
    subprocess.check_call(
        cargo_args + ['--features', ','.join(features)],
        cwd=str(PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi'),
        env=env)

    subprocess.check_call(
        cargo_args,
        cwd=str(PROJ_ROOT / 'rpyutils'),
        env=env)


class questdb_build_ext(build_ext):
    """
    Build the extension, but first compile the pre-requisite
    library by invoking `cargo build --release --features ffi`.
    """
    def run(self):
        cargo_build()
        super().run()


def readme():
    with open(PROJ_ROOT / 'README.rst', 'r', encoding='utf-8') as readme:
        return readme.read()


setup(
    name='questdb',
    version='5.0.0',
    platforms=['any'],
    python_requires='>=3.10',
    install_requires=['numpy>=1.21.0'],
    ext_modules = cythonize([client_extension()], annotate=True),
    cmdclass={'build_ext': questdb_build_ext},
    zip_safe = False,
    package_dir={'': 'src'},
    packages=find_packages('src'),
    package_data={'questdb': ['py.typed', '*.pyi']})
