"""
Hackily fix up the path to pick up the package.

This should be replaced by TOX.
"""

import sys
import os
import pathlib
PROJ_ROOT = pathlib.Path(__file__).parent.parent

def patch():
    sys.path.append(str(PROJ_ROOT / 'src'))

if os.environ.get('TEST_QUESTDB_PATCH_PATH') == '1':
    patch()