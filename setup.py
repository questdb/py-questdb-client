#!/usr/bin/env python3

import pathlib
import sys

from setuptools import setup, find_packages
from setuptools.extension import Extension
from setuptools.command.build_ext import build_ext
import subprocess
from Cython.Build import cythonize


PROJ_ROOT = pathlib.Path(__file__).parent


def ilp_extension():
    questdb_client_lib_dir = (PROJ_ROOT /
        'c-questdb-client' / 'target' / 'release')

    lib_name = None
    lib_paths = []
    libraries = []
    extra_compile_args = []
    extra_link_args = []
    extra_objects = []

    if sys.platform == 'darwin':
        lib_name = 'libquestdb_client.a'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
        extra_link_args.extend(['-framework', 'Security'])
    elif sys.platform == 'win32':
        lib_name = 'questdb_client.lib'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
        libraries.extend(['wsock32', 'ws2_32', 'AdvAPI32', 'bcrypt', 'UserEnv'])
    elif sys.platform == 'linux':
        lib_name = 'libquestdb_client.a'
        extra_objects = [str(questdb_client_lib_dir / lib_name)]
    else:
        raise NotImplementedError(f'Unsupported platform: {sys.platform}')

    return Extension(
        "questdb.ilp",
        ["src/questdb/ilp.pyx"],
        include_dirs=["c-questdb-client/include"],
        library_dirs=lib_paths,
        libraries=libraries,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        extra_objects=extra_objects)


def cargo_build():
    if not (PROJ_ROOT / 'c-questdb-client' / 'src').exists():
        sys.stderr.write('Could not find `c-questdb-client` submodule.\n')
        sys.stderr.write('You might need to run:\n')
        sys.stderr.write('    git submodule update --init --recursive\n')
        sys.exit(1)
    subprocess.check_call(
        ['cargo', 'build', '--release', '--features', 'ffi'],
        cwd=str(PROJ_ROOT / 'c-questdb-client'))


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
    name = 'questdb',
    author='Adam Cimarosti',
    author_email='adam@questdb.io',
    version='0.0.1',
    description='QuestDB client library for Python',
    long_description=readme(),
    url='https://github.com/questdb/py-questdb-client/',
    license='Apache License 2.0',
    platforms=['any'],
    python_requires='>=3.7',
    install_requires=[],
    ext_modules = cythonize([ilp_extension()], annotate=True),
    cmdclass={'build_ext': questdb_build_ext},
    zip_safe = False,
    package_dir={'': 'src'},
    test_suite="tests",
    packages=find_packages('src', exclude=['test']),
    setup_requires=[
        # Setuptools 18.0 properly handles Cython extensions.
        'setuptools>=18.0',
        'cython'],
    extras_require={
        'publish': ['twine', 'wheel']},
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Plugins',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Cython',
        'Programming Language :: Rust',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        'Topic :: System :: Networking'])
