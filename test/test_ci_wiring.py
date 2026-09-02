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


class TestPinnedCiDependencies(unittest.TestCase):

    def setUp(self):
        self.deps = _load_module(
            'questdb_test_pip_install_deps',
            PROJ_ROOT / 'ci' / 'pip_install_deps.py')

    def test_unsatisfiable_explicit_pin_is_fatal(self):
        error = self.deps.UnsupportedDependency('no matching wheel')
        with mock.patch.object(
                self.deps, 'pip_install', side_effect=error), \
                self.assertRaisesRegex(
                    SystemExit,
                    r'Required dependency pyarrow==25\.0\.1 is not installable'):
            self.deps.pip_install_required('pyarrow', '25.0.1')

    def test_explicit_pin_verifies_the_installed_version(self):
        with mock.patch.object(self.deps, 'pip_install'), \
                mock.patch.object(
                    self.deps.importlib.metadata, 'version',
                    return_value='25.0.0'), \
                self.assertRaisesRegex(
                    SystemExit,
                    r'pyarrow==25\.0\.1 .* version 25\.0\.0 is installed'):
            self.deps.pip_install_required('pyarrow', '25.0.1')

    def test_unpinned_optional_dependency_remains_best_effort(self):
        error = self.deps.UnsupportedDependency('no matching wheel')
        with mock.patch.object(
                self.deps, 'pip_install', side_effect=error), \
                mock.patch.object(self.deps.sys, 'stderr'):
            self.deps.try_pip_install('pyarrow')
