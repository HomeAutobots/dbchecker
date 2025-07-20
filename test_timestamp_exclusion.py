#!/usr/bin/env python3
"""
Test script to verify that timestamp columns are properly ignored during comparison.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_test_database_with_timestamps(db_path, add_time_difference=False):
    """Create a test database with timestamp columns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table with various timestamp column types
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT,
            created_at DATETIME,
            updated_timestamp TIMESTAMP,
            last_login_time TIME,
            birth_date DATE,
            profile_data TEXT
        )
    ''')
    
    # Create table with timestamp column name patterns
    cursor.execute('''
        CREATE TABLE posts (
            post_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            created TEXT,
            modified TEXT,
            deleted_at TEXT,
            author_id INTEGER
        )
    ''')
    
    # Insert test data
    base_time = "2024-01-01 10:00:00"
    modified_time = "2024-01-01 10:05:00" if add_time_difference else base_time
    
    cursor.execute('''
        INSERT INTO users (username, email, created_at, updated_timestamp, last_login_time, birth_date, profile_data) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', ("john_doe", "john@example.com", base_time, modified_time, "10:00:00", "1990-01-01", "profile info"))
    
    cursor.execute('''
        INSERT INTO posts (title, content, created, modified, deleted_at, author_id) 
        VALUES (?, ?, ?, ?, ?, ?)
    ''', ("Test Post", "Content here", base_time, modified_time, None, 1))
    
    conn.commit()
    conn.close()

def create_test_database_with_metadata_differences(db_path, add_metadata_difference=False):
    """Create a test database with various metadata that could differ"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Table with audit fields
    cursor.execute('''
        CREATE TABLE audit_table (
            id INTEGER PRIMARY KEY,
            data TEXT,
            created_by TEXT,
            modified_by TEXT,
            session_id TEXT,
            transaction_id TEXT,
            row_version INTEGER,
            record_uuid TEXT
        )
    ''')
    
    # Different metadata based on timing
    if add_metadata_difference:
        session_id = "session_456"
        transaction_id = "txn_789"
        created_by = "user_b"
        row_version = 2
        record_uuid = "550e8400-e29b-41d4-a716-446655440001"
    else:
        session_id = "session_123" 
        transaction_id = "txn_456"
        created_by = "user_a"
        row_version = 1
        record_uuid = "550e8400-e29b-41d4-a716-446655440000"
    
    cursor.execute('''
        INSERT INTO audit_table (data, created_by, session_id, transaction_id, row_version, record_uuid)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', ("test data", created_by, session_id, transaction_id, row_version, record_uuid))
    
    conn.commit()
    conn.close()

def test_enhanced_metadata_exclusion():
    """Test the enhanced metadata exclusion capabilities"""
    print("\\n=== Testing Enhanced Metadata Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "enhanced_db1.db")
        db2_path = os.path.join(temp_dir, "enhanced_db2.db")
        
        # Create databases with different metadata
        print("Creating enhanced test databases...")
        create_test_database_with_metadata_differences(db1_path, add_metadata_difference=False)
        create_test_database_with_metadata_differences(db2_path, add_metadata_difference=True)
        
        # Compare with enhanced detection
        print("Comparing with enhanced metadata detection...")
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            auto_detect_metadata=True,
            auto_detect_sequences=True,
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Enhanced test - Total differences found: {result.summary.total_differences_found}")
        
        if result.summary.total_differences_found == 0:
            print("✅ Enhanced metadata exclusion working correctly!")
            return True
        else:
            print("❌ Enhanced metadata exclusion found unexpected differences")
            return False
    """Main test function"""
    print("=== Testing Timestamp Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create two databases - one with identical data, one with different timestamps
        print("Creating test databases...")
        create_test_database_with_timestamps(db1_path, add_time_difference=False)
        create_test_database_with_timestamps(db2_path, add_time_difference=True)
        
        # Compare databases
        print("Comparing databases...")
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        # Display results
        print(f"\nComparison completed at: {result.timestamp}")
        print(f"Tables compared: {result.summary.total_tables}")
        print(f"Identical tables: {result.summary.identical_tables}")
        print(f"Tables with differences: {result.summary.tables_with_differences}")
        print(f"Total differences found: {result.summary.total_differences_found}")
        
        # Check if timestamp differences were ignored
        if result.data_comparison:
            print("\nTable-level results:")
            for table_name, table_comp in result.data_comparison.table_results.items():
                print(f"  {table_name}:")
                print(f"    - Matching rows: {table_comp.matching_rows}")
                print(f"    - Rows with differences: {len(table_comp.rows_with_differences)}")
                print(f"    - Rows only in DB1: {len(table_comp.rows_only_in_db1)}")
                print(f"    - Rows only in DB2: {len(table_comp.rows_only_in_db2)}")
                
                # Show any field differences (should be none if timestamps are ignored)
                if table_comp.rows_with_differences:
                    print(f"    - Field differences found:")
                    for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                        print(f"      Row {i} ({row_diff.row_identifier}):")
                        for field_diff in row_diff.differences:
                            print(f"        {field_diff.field_name}: '{field_diff.value_db1}' vs '{field_diff.value_db2}'")
        
        # Test enhanced metadata exclusion
        enhanced_success = test_enhanced_metadata_exclusion()
        
        # Expected result: should show no differences since only timestamps differ
        main_success = result.summary.total_differences_found == 0
        
        print("\\n" + "="*50)
        if main_success:
            print("✅ SUCCESS: Timestamp differences were properly ignored!")
        else:
            print(f"❌ ISSUE: Found {result.summary.total_differences_found} differences (should be 0)")
        
        if enhanced_success:
            print("✅ SUCCESS: Enhanced metadata exclusion working!")
        else:
            print("❌ ISSUE: Enhanced metadata exclusion failed!")

def main():
    """Main test function"""
    print("=== Testing Timestamp Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create two databases - one with identical data, one with different timestamps
        print("Creating test databases...")
        create_test_database_with_timestamps(db1_path, add_time_difference=False)
        create_test_database_with_timestamps(db2_path, add_time_difference=True)
        
        # Compare databases
        print("Comparing databases...")
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,  # Enable timestamp detection
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        # Display results
        print(f"\\nComparison completed at: {result.timestamp}")
        print(f"Tables compared: {result.summary.total_tables}")
        print(f"Identical tables: {result.summary.identical_tables}")
        print(f"Tables with differences: {result.summary.tables_with_differences}")
        print(f"Total differences found: {result.summary.total_differences_found}")
        
        # Check if timestamp differences were ignored
        if result.data_comparison:
            print("\\nTable-level results:")
            for table_name, table_comp in result.data_comparison.table_results.items():
                print(f"  {table_name}:")
                print(f"    - Matching rows: {table_comp.matching_rows}")
                print(f"    - Rows with differences: {len(table_comp.rows_with_differences)}")
                print(f"    - Rows only in DB1: {len(table_comp.rows_only_in_db1)}")
                print(f"    - Rows only in DB2: {len(table_comp.rows_only_in_db2)}")
                
                # Show any field differences (should be none if timestamps are ignored)
                if table_comp.rows_with_differences:
                    print(f"    - Field differences found:")
                    for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                        print(f"      Row {i} ({row_diff.row_identifier}):")
                        for field_diff in row_diff.differences:
                            print(f"        {field_diff.field_name}: '{field_diff.value_db1}' vs '{field_diff.value_db2}'")
        
        # Test enhanced metadata exclusion
        enhanced_success = test_enhanced_metadata_exclusion()
        
        # Expected result: should show no differences since only timestamps differ
        main_success = result.summary.total_differences_found == 0
        
        print("\\n" + "="*50)
        if main_success:
            print("✅ SUCCESS: Timestamp differences were properly ignored!")
        else:
            print(f"❌ ISSUE: Found {result.summary.total_differences_found} differences (should be 0)")
        
        if enhanced_success:
            print("✅ SUCCESS: Enhanced metadata exclusion working!")
        else:
            print("❌ ISSUE: Enhanced metadata exclusion failed!")

if __name__ == "__main__":
    main()
