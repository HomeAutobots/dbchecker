#!/usr/bin/env python3
"""
Comprehensive test runner for dbchecker package with advanced reporting.

This script runs all unit and functional tests with proper path configuration
and generates detailed coverage and test reports.
"""

import unittest
import sys
import os
import argparse
from pathlib import Path

# Add the project root to the Python path to ensure imports work
# Now that test/ is inside dbchecker/, we need to go up one level to find the dbchecker package
test_dir = Path(__file__).parent
project_root = test_dir.parent  # This is now the dbchecker project directory
dbchecker_package = project_root  # The package is at this level

# Add the project root to sys.path so we can import dbchecker modules
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import our test reporter
try:
    from test.test_reporter import TestReporter
    USE_ADVANCED_REPORTING = True
except ImportError as e:
    print(f"Advanced reporting not available: {e}")
    print("Falling back to basic reporting...")
    USE_ADVANCED_REPORTING = False
    TestReporter = None

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
    if USE_ADVANCED_REPORTING:
        try:
            return run_all_tests_with_coverage()
        except Exception as e:
            print(f"Advanced reporting failed: {e}")
            print("Falling back to basic reporting...")
            return run_all_tests_basic()
    else:
        return run_all_tests_basic()

def run_all_tests_with_coverage():
    """Run all tests with advanced coverage reporting."""
    if not TestReporter:
        return run_all_tests_basic()
        
    print("DBChecker Comprehensive Test Suite with Coverage Analysis")
    print("=" * 70)
    
    reporter = TestReporter(project_root)
    
    try:
        # Run tests with coverage
        report = reporter.run_tests_with_coverage("all")
        
        # Print summary
        summary = report['summary']
        print("\n" + "=" * 70)
        print("COMPREHENSIVE TEST & COVERAGE SUMMARY")
        print("=" * 70)
        print(f"Total tests run: {summary['total_tests']}")
        print(f"Passed: {summary['passed_tests']}")
        print(f"Failed: {summary['failed_tests']}")
        print(f"Errors: {summary['error_tests']}")
        print(f"Skipped: {summary['skipped_tests']}")
        print(f"Success Rate: {summary['success_rate']}%")
        print(f"Code Coverage: {summary['coverage_percentage']}%")
        print(f"Duration: {report['report_metadata']['total_duration']}s")
        
        # Print report locations
        print("\n" + "=" * 70)
        print("GENERATED REPORTS")
        print("=" * 70)
        for report_type, path in report.get('report_files', {}).items():
            print(f"{report_type.upper()}: {path}")
        
        coverage_reports = report['coverage']['reports']
        print(f"Coverage HTML: {coverage_reports['html']}")
        print(f"Coverage JSON: {coverage_reports['json']}")
        print(f"Coverage XML: {coverage_reports['xml']}")
        
        # Determine overall success
        success_threshold = 80.0  # 80% success rate threshold
        coverage_threshold = 70.0  # 70% coverage threshold
        
        overall_success = (summary['success_rate'] >= success_threshold and 
                          summary['coverage_percentage'] >= coverage_threshold)
        
        if not overall_success:
            print(f"\nWARNING: Test results below thresholds!")
            print(f"Success rate: {summary['success_rate']}% (threshold: {success_threshold}%)")
            print(f"Coverage: {summary['coverage_percentage']}% (threshold: {coverage_threshold}%)")
        
        return overall_success
        
    except Exception as e:
        print(f"Error running advanced tests: {e}")
        print("Falling back to basic test runner...")
        return run_all_tests_basic()

def run_all_tests_basic():
    """Run both unit and functional tests with basic reporting."""
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
    if USE_ADVANCED_REPORTING:
        return run_specific_test_type_with_coverage(test_type)
    else:
        return run_specific_test_type_basic(test_type)

def run_specific_test_type_with_coverage(test_type):
    """Run specific test type with coverage reporting."""
    if not TestReporter:
        return run_specific_test_type_basic(test_type)
        
    print(f"Running {test_type} tests with coverage analysis...")
    
    reporter = TestReporter(project_root)
    
    try:
        # Run tests with coverage
        report = reporter.run_tests_with_coverage(test_type)
        
        # Print summary
        summary = report['summary']
        print(f"\n{test_type.upper()} TEST RESULTS WITH COVERAGE")
        print("=" * 50)
        print(f"Tests run: {summary['total_tests']}")
        print(f"Success Rate: {summary['success_rate']}%")
        print(f"Code Coverage: {summary['coverage_percentage']}%")
        print(f"Duration: {report['report_metadata']['total_duration']}s")
        
        # Print report locations
        print(f"\nReports generated:")
        for report_type, path in report.get('report_files', {}).items():
            if 'latest' in report_type:
                print(f"  {report_type}: {path}")
        
        return summary['success_rate'] >= 80.0
        
    except Exception as e:
        print(f"Error running advanced {test_type} tests: {e}")
        return run_specific_test_type_basic(test_type)

def run_specific_test_type_basic(test_type):
    """Run tests of a specific type (unit or functional) with basic reporting."""
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
    # Add argument parsing for better control
    parser = argparse.ArgumentParser(description='DBChecker Test Runner with Coverage')
    parser.add_argument('test_type', nargs='?', default='all', 
                       choices=['all', 'unit', 'functional'],
                       help='Type of tests to run (default: all)')
    parser.add_argument('--no-coverage', action='store_true',
                       help='Run tests without coverage analysis')
    parser.add_argument('--coverage-threshold', type=float, default=70.0,
                       help='Coverage percentage threshold (default: 70.0)')
    parser.add_argument('--success-threshold', type=float, default=80.0,
                       help='Success rate percentage threshold (default: 80.0)')
    
    args = parser.parse_args()
    
    # Override advanced reporting if requested
    if args.no_coverage:
        USE_ADVANCED_REPORTING = False
    
    if args.test_type == 'all':
        success = run_all_tests()
    else:
        success = run_specific_test_type(args.test_type)
    
    sys.exit(0 if success else 1)
