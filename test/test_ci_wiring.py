import importlib.util
import pathlib
import unittest
from unittest import mock

import patch_path


PROJ_ROOT = patch_path.PROJ_ROOT


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestProjCibuildwheelRouting(unittest.TestCase):

    def test_cibuildwheel_runs_through_arrow_lock_wrapper(self):
        proj = _load_module('questdb_test_proj', PROJ_ROOT / 'proj.py')
        with mock.patch.object(proj, '_run') as run, \
                mock.patch.object(proj.sys, 'platform', 'linux'), \
                mock.patch.object(proj.platform, 'machine', return_value='x86_64'):
            proj.cibuildwheel('--only', 'cp312-manylinux_x86_64')

        run.assert_called_once_with(
            proj.PYTHON,
            PROJ_ROOT / 'ci' / 'run_cibuildwheel.py',
            '--platform', 'linux',
            '--output-dir', 'dist',
            '--archs', 'x86_64',
            '--only', 'cp312-manylinux_x86_64')
