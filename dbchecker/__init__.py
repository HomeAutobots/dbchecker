"""
SQLite Database Comparator

A Python application that compares two SQLite databases for structural and data equality
while ignoring UUID differences.
"""

from .comparator import DatabaseComparator
from .models import ComparisonOptions, ComparisonResult
from .exceptions import DatabaseComparisonError

__version__ = "1.0.0"
__author__ = "DBChecker Team"

__all__ = [
    "DatabaseComparator", 
    "ComparisonOptions", 
    "ComparisonResult",
    "DatabaseComparisonError"
]
