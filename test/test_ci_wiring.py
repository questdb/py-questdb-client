import importlib.util
import pathlib
import subprocess
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


class TestCibuildwheelArrowLockGuard(unittest.TestCase):

    def setUp(self):
        self.wrapper = _load_module(
            'questdb_test_run_cibuildwheel',
            PROJ_ROOT / 'ci' / 'run_cibuildwheel.py')
        self.guard_command = [
            self.wrapper.sys.executable,
            str(PROJ_ROOT / 'c-questdb-client' / 'ci' /
                'check_arrow_ffi_lock.py'),
        ]

    def test_arrow_lock_guard_runs_before_cibuildwheel(self):
        args = ['--only', 'cp312-manylinux_x86_64']
        with mock.patch.object(self.wrapper.subprocess, 'check_call') as run, \
                mock.patch.object(self.wrapper.sys, 'argv', ['wrapper', *args]):
            self.wrapper.main()

        self.assertEqual(run.call_args_list, [
            mock.call(self.guard_command),
            mock.call([
                self.wrapper.sys.executable, '-m', 'cibuildwheel', *args,
            ], cwd=PROJ_ROOT),
        ])

    def test_arrow_lock_failure_prevents_cibuildwheel(self):
        error = subprocess.CalledProcessError(1, self.guard_command)
        with mock.patch.object(
                self.wrapper.subprocess, 'check_call', side_effect=error) as run, \
                self.assertRaises(subprocess.CalledProcessError) as raised:
            self.wrapper.main()

        self.assertIs(raised.exception, error)
        run.assert_called_once_with(self.guard_command)


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

    def test_main_preserves_numpy1_arrow_cap_and_explicit_pins(self):
        for pins in ([], ['--pyarrow-version=25.0.1', '--polars-version=1.32.3']):
            with self.subTest(pins=pins), \
                    mock.patch.object(self.deps, 'ensure_timezone'), \
                    mock.patch.object(self.deps, 'pip_install'), \
                    mock.patch.object(self.deps, 'pip_install_required') as required, \
                    mock.patch.object(self.deps, 'try_pip_install') as optional, \
                    mock.patch.object(
                        self.deps.platform, 'python_implementation',
                        return_value='PyPy'):
                self.deps.main(self.deps.arg_parser.parse_args(
                    ['--pandas-version=2.3.3', *pins]))
            self.assertIn(mock.call('numpy<2'), optional.call_args_list)
            self.assertIn(mock.call('pyarrow<26'), optional.call_args_list)
            self.assertNotIn(mock.call('pyarrow'), optional.call_args_list)
            if pins:
                self.assertIn(mock.call('pyarrow', '25.0.1'), required.call_args_list)
                self.assertIn(mock.call('polars', '1.32.3'), required.call_args_list)

    def test_pyyaml_is_installed_for_the_manifest_tests(self):
        """`TestManifest` skips without PyYAML, so every CI leg that
        installs test dependencies through this script tries to install
        it."""
        # Reported as PyPy so that `main` skips its check that the
        # required dependencies import.
        with mock.patch.object(self.deps, 'ensure_timezone'), \
                mock.patch.object(self.deps, 'pip_install'), \
                mock.patch.object(self.deps, 'pip_install_required'), \
                mock.patch.object(
                    self.deps, 'install_default_dataframe_dependencies'), \
                mock.patch.object(
                    self.deps, 'try_pip_install') as try_install, \
                mock.patch.object(
                    self.deps.platform, 'python_implementation',
                    return_value='PyPy'):
            self.deps.main(self.deps.arg_parser.parse_args([]))
        self.assertIn(mock.call('pyyaml'), try_install.call_args_list)

    def test_minimum_versions_are_not_upgraded_after_install(self):
        with mock.patch.object(self.deps, 'ensure_timezone'), \
                mock.patch.object(self.deps, 'pip_install') as install, \
                mock.patch.object(self.deps, 'check_versions') as check, \
                mock.patch.object(self.deps, 'pip_install_required') as required, \
                mock.patch.object(self.deps, 'try_pip_install') as optional, \
                mock.patch.object(
                    self.deps.platform, 'python_implementation',
                    return_value='PyPy'):
            self.deps.main(self.deps.arg_parser.parse_args(
                ['--min-versions', '--with-duckdb']))
        pins = dict(self.deps.MIN_VERSIONS, duckdb=self.deps.MIN_DUCKDB_VERSION)
        self.assertEqual(install.call_args_list, [
            mock.call('pip'), mock.call('setuptools'), mock.call('packaging'),
            mock.call([f'{name}=={version}' for name, version in pins.items()]),
            mock.call('psutil'),
        ])
        check.assert_called_once_with(pins)
        required.assert_not_called()
        self.assertEqual(optional.call_args_list, [
            mock.call('gevent'), mock.call('pyyaml'),
        ])

    def test_minimum_versions_reject_conflicting_explicit_pins(self):
        for pin in ('--pandas-version=2.3.3', '--pyarrow-version=25.0.1',
                    '--polars-version=1.32.3'):
            with self.subTest(pin=pin), \
                    mock.patch.object(self.deps, 'pip_install') as install, \
                    mock.patch.object(self.deps.sys, 'stderr'), \
                    self.assertRaises(SystemExit) as cm:
                self.deps.main(self.deps.arg_parser.parse_args(
                    ['--min-versions', pin]))
            self.assertEqual(cm.exception.code, 2)
            install.assert_not_called()

    def test_gevent_is_installed_for_the_greenlet_test(self):
        """`test_scoped_calls_from_greenlets_on_one_thread` skips without
        gevent, so every CI leg that installs test dependencies through
        this script tries to install it."""
        # Reported as PyPy so that `main` skips its check that the
        # required dependencies import.
        with mock.patch.object(self.deps, 'ensure_timezone'), \
                mock.patch.object(self.deps, 'pip_install'), \
                mock.patch.object(self.deps, 'pip_install_required'), \
                mock.patch.object(
                    self.deps, 'install_default_dataframe_dependencies'), \
                mock.patch.object(
                    self.deps, 'try_pip_install') as try_install, \
                mock.patch.object(
                    self.deps.platform, 'python_implementation',
                    return_value='PyPy'):
            self.deps.main(self.deps.arg_parser.parse_args([]))
        self.assertIn(mock.call('gevent'), try_install.call_args_list)

    def test_unpinned_optional_dependency_remains_best_effort(self):
        error = self.deps.UnsupportedDependency('no matching wheel')
        with mock.patch.object(
                self.deps, 'pip_install', side_effect=error), \
                mock.patch.object(self.deps.sys, 'stderr'):
            self.deps.try_pip_install('pyarrow')
