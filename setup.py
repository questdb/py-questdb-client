#!/usr/bin/env python3

import pathlib
import sys
import os
import shutil
import platform

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


def ingress_extension():
    lib_name = None
    lib_paths = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []
    extra_objects = []

    questdb_rs_ffi_dir = PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi'
    questdb_client_lib_dir = None
    if PLATFORM == 'win32' and MODE == '32bit':
        questdb_client_lib_dir = \
            questdb_rs_ffi_dir / 'target' / WIN_32BIT_CARGO_TARGET / 'release'
    else:
        questdb_client_lib_dir = questdb_rs_ffi_dir / 'target' / 'release'

    if PLATFORM == 'darwin':
        lib_name = 'libquestdb_client.a'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
        extra_link_args.extend(['-framework', 'Security'])
    elif PLATFORM == 'win32':
        lib_name = 'questdb_client.lib'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
        libraries.extend(['wsock32', 'ws2_32', 'AdvAPI32', 'bcrypt', 'UserEnv'])
    elif PLATFORM == 'linux':
        lib_name = 'libquestdb_client.a'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
    else:
        raise NotImplementedError(f'Unsupported platform: {PLATFORM}')

    return Extension(
        "questdb.ingress",
        ["src/questdb/ingress.pyx"],
        include_dirs=["c-questdb-client/include"],
        library_dirs=lib_paths,
        libraries=libraries,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        extra_objects=extra_objects)


def cargo_build():
    if not (PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi').exists():
        if os.environ.get('SETUP_DO_GIT_SUBMODULE_INIT') == '1':
            subprocess.check_call([
                'git', 'submodule', 'update', '--init', '--recursive'])
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

    subprocess.check_call(
        cargo_args,
        cwd=str(PROJ_ROOT / 'c-questdb-client' / 'questdb-rs-ffi'))


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
    version='1.0.2',
    platforms=['any'],
    python_requires='>=3.7',
    install_requires=[],
    ext_modules = cythonize([ingress_extension()], annotate=True),
    cmdclass={'build_ext': questdb_build_ext},
    zip_safe = False,
    package_dir={'': 'src'},
    test_suite="tests",
    packages=find_packages('src', exclude=['test']))
