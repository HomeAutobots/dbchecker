#!/usr/bin/env python3
"""
Test script to verify that user-specified column exclusions work correctly.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_test_database_with_user_columns(db_path, include_differences=False):
    """Create a test database with columns that the user might want to exclude"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a table with various types of columns including ones a user might want to exclude
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            internal_notes TEXT,
            admin_comments TEXT,
            debug_field TEXT,
            temp_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Insert test data
    if include_differences:
        # DB with differences in user-excluded columns but identical business data
        cursor.execute('''
            INSERT INTO users (username, email, first_name, last_name, internal_notes, admin_comments, debug_field, temp_data)
            VALUES 
                ('jdoe', 'john@example.com', 'John', 'Doe', 'Internal note v2', 'Admin updated this', 'debug_value_2', 'temp_v2'),
                ('jsmith', 'jane@example.com', 'Jane', 'Smith', 'Different internal note', 'Admin comment changed', 'debug_alt', 'temp_different')
        ''')
    else:
        # Standard data
        cursor.execute('''
            INSERT INTO users (username, email, first_name, last_name, internal_notes, admin_comments, debug_field, temp_data)
            VALUES 
                ('jdoe', 'john@example.com', 'John', 'Doe', 'Internal note v1', 'Admin created this', 'debug_value_1', 'temp_v1'),
                ('jsmith', 'jane@example.com', 'Jane', 'Smith', 'Internal note for Jane', 'Admin comment for Jane', 'debug_jane', 'temp_jane')
        ''')
    
    conn.commit()
    conn.close()

def test_explicit_column_exclusion():
    """Test excluding specific columns by name"""
    print("=== Testing Explicit Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "production.db")
        db2_path = os.path.join(temp_dir, "staging.db")
        
        # Create databases with differences only in columns we'll exclude
        print("Creating test databases...")
        create_test_database_with_user_columns(db1_path, include_differences=False)
        create_test_database_with_user_columns(db2_path, include_differences=True)
        
        print("Comparing without column exclusions...")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        
        # First, compare without exclusions to see differences
        options_no_exclusions = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,  # This will exclude timestamps automatically
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options_no_exclusions)
        result_no_exclusions = comparator.compare()
        
        print(f"Without user exclusions - differences found: {result_no_exclusions.summary.total_differences_found}")
        
        print("\nComparing WITH user column exclusions...")
        
        # Now compare with user-specified column exclusions
        options_with_exclusions = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            excluded_columns=['internal_notes', 'admin_comments', 'debug_field', 'temp_data'],
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options_with_exclusions)
        result_with_exclusions = comparator.compare()
        
        print(f"WITH user exclusions - differences found: {result_with_exclusions.summary.total_differences_found}")
        
        if result_with_exclusions.summary.total_differences_found == 0:
            print("✅ SUCCESS: User-specified column exclusions working correctly!")
            return True
        else:
            print("❌ ISSUE: User exclusions didn't work as expected")
            return False

def test_pattern_based_exclusion():
    """Test excluding columns based on patterns"""
    print("\n=== Testing Pattern-Based Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "test1.db")
        db2_path = os.path.join(temp_dir, "test2.db")
        
        create_test_database_with_user_columns(db1_path, include_differences=False)
        create_test_database_with_user_columns(db2_path, include_differences=True)
        
        print("Testing pattern-based exclusions...")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        
        # Use patterns to exclude columns
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            excluded_column_patterns=[
                r'.*notes.*',     # Matches internal_notes
                r'.*admin.*',     # Matches admin_comments  
                r'.*debug.*',     # Matches debug_field
                r'.*temp.*'       # Matches temp_data
            ],
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Pattern-based exclusions - differences found: {result.summary.total_differences_found}")
        
        if result.summary.total_differences_found == 0:
            print("✅ SUCCESS: Pattern-based column exclusions working correctly!")
            return True
        else:
            print("❌ ISSUE: Pattern-based exclusions didn't work as expected")
            return False

def test_combined_exclusions():
    """Test combining explicit columns with patterns"""
    print("\n=== Testing Combined Exclusions ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "combined1.db")
        db2_path = os.path.join(temp_dir, "combined2.db")
        
        create_test_database_with_user_columns(db1_path, include_differences=False)
        create_test_database_with_user_columns(db2_path, include_differences=True)
        
        print("Testing combined explicit + pattern exclusions...")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        
        # Combine explicit columns and patterns
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            excluded_columns=['internal_notes', 'admin_comments'],  # Explicit
            excluded_column_patterns=[r'.*debug.*', r'.*temp.*'],    # Patterns
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Combined exclusions - differences found: {result.summary.total_differences_found}")
        
        if result.summary.total_differences_found == 0:
            print("✅ SUCCESS: Combined exclusions working correctly!")
            return True
        else:
            print("❌ ISSUE: Combined exclusions didn't work as expected")
            return False

def main():
    """Main test function"""
    print("=== Testing User-Specified Column Exclusions ===")
    print("This test verifies that users can exclude specific columns from comparison")
    print()
    
    test1_success = test_explicit_column_exclusion()
    test2_success = test_pattern_based_exclusion()
    test3_success = test_combined_exclusions()
    
    print("\n" + "="*60)
    print("OVERALL RESULTS")
    print("="*60)
    
    if test1_success:
        print("✅ Explicit column exclusions: PASSED")
    else:
        print("❌ Explicit column exclusions: FAILED")
    
    if test2_success:
        print("✅ Pattern-based exclusions: PASSED")
    else:
        print("❌ Pattern-based exclusions: FAILED")
    
    if test3_success:
        print("✅ Combined exclusions: PASSED")
    else:
        print("❌ Combined exclusions: FAILED")
    
    all_passed = test1_success and test2_success and test3_success
    
    if all_passed:
        print("\n🎉 All column exclusion tests PASSED!")
        print("User-specified column exclusions are working correctly.")
    else:
        print("\n⚠️  Some tests FAILED. Check the implementation.")

if __name__ == "__main__":
    main()
