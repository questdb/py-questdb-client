import argparse
import unittest
from unittest import mock

import pip_install_deps as deps


class TestDependencySelection(unittest.TestCase):
    def run_installer(self, py_version, pandas_version=None):
        use_pandas3 = deps.should_use_pandas3(py_version)
        with mock.patch.object(deps, 'pip_install') as install, \
                mock.patch.object(deps, 'ensure_timezone'), \
                mock.patch.object(deps, 'should_use_pandas3',
                                  return_value=use_pandas3), \
                mock.patch.object(deps.platform, 'python_implementation',
                                  return_value='PyPy'):
            # Skip real dependency imports; these tests check the pip requests.
            deps.main(argparse.Namespace(pandas_version=pandas_version))
        return install

    def assert_arrow_requirement(self, install, requirement):
        arrow_requests = [
            call.args[0] for call in install.call_args_list
            if call.args[0].startswith('pyarrow')
        ]
        # In particular, do not upgrade to unbounded PyArrow later in main().
        self.assertEqual(arrow_requests, [requirement])

    def test_explicit_pandas2_keeps_numpy1_and_compatible_arrow(self):
        install = self.run_installer((3, 12), '2.3.3')
        install.assert_any_call('pandas', '2.3.3')
        install.assert_any_call('numpy<2', None)
        self.assert_arrow_requirement(install, 'pyarrow<26')

    def test_python310_defaults_keep_numpy1_and_compatible_arrow(self):
        install = self.run_installer((3, 10))
        install.assert_any_call('pandas>=2,<3', None)
        install.assert_any_call('numpy<2', None)
        self.assert_arrow_requirement(install, 'pyarrow<26')

    def test_python311_defaults_keep_current_arrow(self):
        install = self.run_installer((3, 11))
        install.assert_any_call('pandas>=3,<4', None)
        install.assert_any_call('numpy>=2,<3', None)
        self.assert_arrow_requirement(install, 'pyarrow')

    def test_empty_override_uses_defaults(self):
        install = self.run_installer((3, 12), '')
        install.assert_any_call('pandas>=3,<4', None)
        install.assert_any_call('numpy>=2,<3', None)
        self.assert_arrow_requirement(install, 'pyarrow')


if __name__ == '__main__':
    unittest.main()
