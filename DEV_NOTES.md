# Development Notes

## Source code

Clone from Github: https://github.com/questdb/py-questdb-client


## Pre-requisites

### Compiling

Install Rust as per https://rustup.rs/.

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


## Building and packaging

For a crash-course in `setuptools` you may want to watch this 
[video](https://www.youtube.com/watch?v=GIF3LaRqgXo&ab_channel=CodingTech>).

### Sync submodule before building

```shell
git submodule update --init --recursive
```

### Create a Python3 environment to work in

```shell
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
```

The last line installs the requirements needed to build, test, develop the project.

#### Leave/clean venv

```shell
deactivate
# optionally 
rm -rf venv
```

### Install the project locally (into venv)

```shell
python -m pip install -e .
```

The `-e` flag links to the current directory rather than copying.


## Project management with: python3 proj.py

This is a CLI offering these developer commands:

- `python proj.py bld` builds the project.
    ```shell
    # equivalent to
    python setup.py build_ext --inplace
    ```
- `python proj.py srv` starts document server.
    ```shell
    # equivalent to
    python setup.py -m http.server 8000 build/docs
    ```
- `python proj.py doc` builds the documentation (served by the document server).
    ```shell
    # equivalent to
    PYTHONPATH=src python setup.py -m sphinx.cmd.build -b html docs build/docs
    ```
- `python proj.py tst` runs the project's tests against a locally running QuestDB instance.
    ```shell
    # equivalent to
    python test/test.py -v
    ```
- `python proj.py cibuildwheel` packages the project locally (platform is dynamic and
   dependent on user system), equivalent to:
    ```shell
    # equivalent to
    python setup.py -m cibuildwheel --platform macosx --output-dir dist
    ```
- `python proj.py cln` deletes temporal/build files, including `venv`.
        
  
### Creating a wheel

```shell
python3 setup.py bdist_wheel
```

### Creating a source distribution

```shell
python3 setup.py sdist
```

### Uploading to PyPI

Build wheel and source:

```shell
python3 setup.py bdist_wheel sdist
```

Install dependencies for uploading to PyPI:

```shell
python3 -m pip install -e ".[publish]"
```

Upload to PyPI:

```shell
python3 -m twine upload dist/*
```

## Building, packaging and testing with ``cibuildwheel``

Builds, tests and packages across a number of operating systems, architectures
and configurations.

### Testing locally

Package locally:

```shell
# Or `--platform` set to `macos` or `windows`
cibuildwheel --platform linux --output-dir dist
```

The wheels will end up in the ``dist/`` directory when packaging locally.


## Debugging with on Linux

### GDB

Debugging with GDB is best done with the debug build of the Python interpreter.
This automatically loads the debug helper scripts for GDB.

On Ubuntu, you can install the debug build of Python with:

```shell
sudo apt-get install python3-dbg
```

When in a GDB session, you can now also use additional commands like ``py-bt``.

Read more on the `Python GDB 
<https://devguide.python.org/advanced-tools/gdb/index.html>`_ documentation.

### Valgrind

We can set ``PYTHONMALLOC`` to disable python custom memory pools.

```shell
export PYTHONMALLOC=malloc
valgrind \
        --leak-check=full \
        --show-leak-kinds=all \
        --track-origins=yes \
        --verbose \
        python3 test/test.py -v
```

## Debugging in side a ``cibuildwheel`` container

In ``pyproject.toml``, add the following to the ``[tool.cibuildwheel]`` section:

```toml
[tool.cibuildwheel]

# .. other existing config

# With GDB
test-command = """
echo set auto-load python-scripts on >> ~/.gdbinit
echo add-auto-load-safe-path {project}/gdb >> ~/.gdbinit
cat ~/.gdbinit

ulimit -u unlimited
export PYTHONMALLOC=malloc
gdb -x {project}/commands.txt --batch --return-child-result --args \
    python {project}/test/test.py -v
"""

# With Valgrind
test-command = """
export PYTHONMALLOC=malloc
valgrind \
    --leak-check=full \
    --show-leak-kinds=all \
    --track-origins=yes \
    --verbose \
    python {project}/test/test.py -v
"""

[tool.cibuildwheel.linux]
before-all = """
yum -y install gdb
yum -y install valgrind
"""
```

Note the ``gdb/commands.txt`` file. Review it and change it to fit your needs.