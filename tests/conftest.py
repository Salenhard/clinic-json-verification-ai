"""Shared pytest configuration.

Adds the project root to sys.path so tests can import
`config`, `repository`, `service`, `controller` without installing the package.
"""

import sys
import os

# project root is one level up from this file (tests/)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
