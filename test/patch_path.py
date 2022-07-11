"""
Hackily fix up the path to pick up the package.

This should be replaced by TOX.
"""

import sys
import pathlib
PROJ_ROOT = pathlib.Path(__file__).parent.parent
sys.path.append(str(PROJ_ROOT / 'src'))