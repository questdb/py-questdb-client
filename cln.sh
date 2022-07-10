(cd c-questdb-client && cargo clean)
python3 setup.py clean --all
rm -vfR build > /dev/null 2>&1
rm src/questdb/ilp.cpython-* > /dev/null 2>&1
rm -fR src/questdb/__pycache__ > /dev/null 2>&1
rm -fR src/questdb.egg-info > /dev/null 2>&1
rm -fR dist
