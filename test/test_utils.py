"""
Test utilities for dbchecker test suite.

This module provides common utilities and path setup for all test files.
"""

import sys
import os
from pathlib import Path

def setup_test_path():
    """Setup Python path to find dbchecker package from test directories."""
    # Get the project root (two levels up from test/unit or test/functional)
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent
    dbchecker_package = project_root / 'dbchecker'
    
    # Add paths if not already present
    paths_to_add = [str(project_root), str(dbchecker_package)]
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)

# Call setup immediately when module is imported
setup_test_path()
