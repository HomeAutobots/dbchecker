#!/usr/bin/env python3
"""
Comprehensive test runner for dbchecker package.

This script runs all unit and functional tests with proper path configuration.
"""

import unittest
import sys
import os
from pathlib import Path

# Add the project root to the Python path to ensure imports work
# Now that test/ is inside dbchecker/, we need to go up one level to find the dbchecker package
test_dir = Path(__file__).parent
project_root = test_dir.parent  # This is now the dbchecker project directory
dbchecker_package = project_root  # The package is at this level

# Add the project root to sys.path so we can import dbchecker modules
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

def run_unit_tests():
    """Run all unit tests."""
    print("Running Unit Tests")
    print("=" * 50)
    
    # Discover unit tests
    test_dir = Path(__file__).parent / 'unit'
    loader = unittest.TestLoader()
    suite = loader.discover(str(test_dir), pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result

def run_functional_tests():
    """Run all functional tests."""
    print("\nRunning Functional Tests")
    print("=" * 50)
    
    # Discover functional tests
    test_dir = Path(__file__).parent / 'functional'
    loader = unittest.TestLoader()
    suite = loader.discover(str(test_dir), pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result

def run_all_tests():
    """Run both unit and functional tests."""
    print("DBChecker Comprehensive Test Suite")
    print("=" * 60)
    
    # Run unit tests
    unit_result = run_unit_tests()
    
    # Run functional tests  
    functional_result = run_functional_tests()
    
    # Print combined summary
    total_tests = unit_result.testsRun + functional_result.testsRun
    total_failures = len(unit_result.failures) + len(functional_result.failures)
    total_errors = len(unit_result.errors) + len(functional_result.errors)
    total_skipped = len(unit_result.skipped) + len(functional_result.skipped)
    
    print("\n" + "=" * 60)
    print("COMPREHENSIVE TEST SUMMARY")
    print("=" * 60)
    print(f"Total tests run: {total_tests}")
    print(f"Unit tests: {unit_result.testsRun}")
    print(f"Functional tests: {functional_result.testsRun}")
    print(f"Failures: {total_failures}")
    print(f"Errors: {total_errors}")
    print(f"Skipped: {total_skipped}")
    
    success_rate = ((total_tests - total_failures - total_errors) / total_tests) * 100 if total_tests > 0 else 0
    print(f"Success Rate: {success_rate:.1f}%")
    
    overall_success = unit_result.wasSuccessful() and functional_result.wasSuccessful()
    return overall_success

def run_specific_test_type(test_type):
    """Run tests of a specific type (unit or functional)."""
    if test_type.lower() == 'unit':
        result = run_unit_tests()
    elif test_type.lower() == 'functional':
        result = run_functional_tests()
    else:
        print(f"Unknown test type: {test_type}")
        print("Valid options: unit, functional")
        return False
    
    return result.wasSuccessful()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        success = run_specific_test_type(test_type)
    else:
        success = run_all_tests()
    
    sys.exit(0 if success else 1)
