"""
Pytest configuration and fixtures for dbchecker tests.
"""

import sys
import os
from pathlib import Path

# Add the dbchecker package to Python path
test_dir = Path(__file__).parent
project_root = test_dir.parent  # This is the dbchecker project directory

# Add paths to sys.path if not already present
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest

@pytest.fixture
def temp_db_path():
    """Provide a temporary database path for tests."""
    import tempfile
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
        yield f.name
    # Cleanup
    try:
        os.unlink(f.name)
    except:
        pass
