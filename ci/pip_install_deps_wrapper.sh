#!/bin/sh
python ci/pip_install_deps.py 2>&1 | tee pip_install_deps.log
cat pip_install_deps.log
