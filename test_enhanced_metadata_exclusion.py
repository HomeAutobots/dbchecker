#!/usr/bin/env python3
"""
Enhanced test script to verify that metadata columns are properly ignored during comparison.
This includes timestamps, UUID, audit fields, sequences, and other system-generated data.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_test_database_with_all_metadata(db_path, add_differences=False):
    """Create a test database with various types of metadata columns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create comprehensive test table with all types of metadata
    cursor.execute('''
        CREATE TABLE comprehensive_test (
            -- Primary key (sequence/auto-increment)
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Business data (should be compared)
            username TEXT NOT NULL,
            email TEXT,
            status TEXT,
            
            -- Timestamp columns
            created_at DATETIME,
            updated_timestamp TIMESTAMP,
            last_login_time TIME,
            birth_date DATE,
            
            -- Audit metadata
            created_by TEXT,
            modified_by TEXT,
            session_id TEXT,
            transaction_id TEXT,
            
            -- System metadata
            row_version INTEGER,
            record_checksum TEXT,
            source_system TEXT,
            audit_log TEXT,
            
            -- UUID columns
            record_uuid TEXT,
            external_id TEXT
        )
    ''')
    
    # Create another table with different metadata patterns
    cursor.execute('''
        CREATE TABLE audit_table (
            post_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            
            -- Timestamp patterns
            created TEXT,
            modified TEXT,
            deleted_at TEXT,
            published_date TEXT,
            
            -- User tracking
            author_user TEXT,
            editor_user TEXT,
            reviewer_by TEXT,
            
            -- System tracking
            version_number INTEGER,
            trace_id TEXT,
            system_hash TEXT,
            
            -- Reference data
            author_id INTEGER,
            category_id INTEGER
        )
    ''')
    
    # Base data for consistent business logic
    base_data = {
        'username': 'john_doe',
        'email': 'john@example.com', 
        'status': 'active',
        'title': 'Test Post',
        'content': 'This is test content',
        'author_id': 1,
        'category_id': 2
    }
    
    # Different metadata based on timing/system state
    if add_differences:
        metadata = {
            'created_at': '2024-01-01 10:05:00',
            'updated_timestamp': '2024-01-01 10:05:00',
            'last_login_time': '10:05:00',
            'created_by': 'system_b',
            'modified_by': 'user_b',
            'session_id': 'session_456',
            'transaction_id': 'txn_789',
            'row_version': 2,
            'record_checksum': 'abc123def456',
            'source_system': 'system_b',
            'audit_log': 'updated by system_b',
            'record_uuid': '550e8400-e29b-41d4-a716-446655440001',
            'external_id': '789def456abc',
            'created': '2024-01-01 10:05:00',
            'modified': '2024-01-01 10:05:00',
            'author_user': 'john_b',
            'editor_user': 'editor_b',
            'reviewer_by': 'reviewer_b',
            'version_number': 2,
            'trace_id': 'trace_789',
            'system_hash': 'hash789'
        }
    else:
        metadata = {
            'created_at': '2024-01-01 10:00:00',
            'updated_timestamp': '2024-01-01 10:00:00',
            'last_login_time': '10:00:00',
            'created_by': 'system_a',
            'modified_by': 'user_a',
            'session_id': 'session_123',
            'transaction_id': 'txn_456',
            'row_version': 1,
            'record_checksum': '123abc456def',
            'source_system': 'system_a',
            'audit_log': 'created by system_a',
            'record_uuid': '550e8400-e29b-41d4-a716-446655440000',
            'external_id': '456abc123def',
            'created': '2024-01-01 10:00:00',
            'modified': '2024-01-01 10:00:00',
            'author_user': 'john_a',
            'editor_user': 'editor_a',
            'reviewer_by': 'reviewer_a',
            'version_number': 1,
            'trace_id': 'trace_456',
            'system_hash': 'hash456'
        }
    
    # Insert data into comprehensive_test table
    cursor.execute('''
        INSERT INTO comprehensive_test (
            username, email, status, created_at, updated_timestamp, last_login_time, birth_date,
            created_by, modified_by, session_id, transaction_id, row_version, record_checksum,
            source_system, audit_log, record_uuid, external_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        base_data['username'], base_data['email'], base_data['status'],
        metadata['created_at'], metadata['updated_timestamp'], metadata['last_login_time'], '1990-01-01',
        metadata['created_by'], metadata['modified_by'], metadata['session_id'], metadata['transaction_id'],
        metadata['row_version'], metadata['record_checksum'], metadata['source_system'], metadata['audit_log'],
        metadata['record_uuid'], metadata['external_id']
    ))
    
    # Insert data into audit_table
    cursor.execute('''
        INSERT INTO audit_table (
            title, content, created, modified, deleted_at, published_date,
            author_user, editor_user, reviewer_by, version_number, trace_id, system_hash,
            author_id, category_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        base_data['title'], base_data['content'], metadata['created'], metadata['modified'], None, '2024-01-01',
        metadata['author_user'], metadata['editor_user'], metadata['reviewer_by'], metadata['version_number'],
        metadata['trace_id'], metadata['system_hash'], base_data['author_id'], base_data['category_id']
    ))
    
    conn.commit()
    conn.close()

def test_custom_patterns():
    """Test custom metadata patterns"""
    print("\\n=== Testing Custom Metadata Patterns ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create two databases with different metadata
        print("Creating test databases...")
        create_test_database_with_all_metadata(db1_path, add_differences=False)
        create_test_database_with_all_metadata(db2_path, add_differences=True)
        
        # Test with custom patterns
        print("Testing with custom metadata patterns...")
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            auto_detect_metadata=True,
            auto_detect_sequences=True,
            
            # Add custom patterns
            metadata_patterns=[
                r'.*custom.*',
                r'.*special.*'
            ],
            timestamp_patterns=[
                r'.*published.*'
            ],
            
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        # Display results
        print(f"\\nCustom Pattern Test Results:")
        print(f"Tables compared: {result.summary.total_tables}")
        print(f"Identical tables: {result.summary.identical_tables}")
        print(f"Tables with differences: {result.summary.tables_with_differences}")
        print(f"Total differences found: {result.summary.total_differences_found}")
        
        return result.summary.total_differences_found == 0

def test_disable_auto_detection():
    """Test disabling auto-detection and using explicit columns only"""
    print("\\n=== Testing Disabled Auto-Detection ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create databases with differences
        create_test_database_with_all_metadata(db1_path, add_differences=False)
        create_test_database_with_all_metadata(db2_path, add_differences=True)
        
        print("Testing with auto-detection disabled...")
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path
        )
        
        # Disable auto-detection, only use explicit columns
        options = ComparisonOptions(
            auto_detect_uuids=False,
            auto_detect_timestamps=False,
            auto_detect_metadata=False,
            auto_detect_sequences=False,
            
            # Explicitly specify only some columns to exclude
            explicit_timestamp_columns=['created_at', 'updated_timestamp'],
            explicit_uuid_columns=['record_uuid'],
            explicit_metadata_columns=['created_by', 'session_id'],
            explicit_sequence_columns=['id'],
            
            verbose=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"\\nDisabled Auto-Detection Results:")
        print(f"Total differences found: {result.summary.total_differences_found}")
        
        # Should find more differences since many metadata columns won't be excluded
        return result.summary.total_differences_found > 0

def main():
    """Main test function"""
    print("=== Testing Enhanced Metadata Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create two databases - identical business data, different metadata
        print("Creating test databases...")
        create_test_database_with_all_metadata(db1_path, add_differences=False)
        create_test_database_with_all_metadata(db2_path, add_differences=True)
        
        # Compare databases with full auto-detection
        print("Comparing databases with enhanced metadata detection...")
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
        
        # Display results
        print(f"\\nComparison completed at: {result.timestamp}")
        print(f"Tables compared: {result.summary.total_tables}")
        print(f"Identical tables: {result.summary.identical_tables}")
        print(f"Tables with differences: {result.summary.tables_with_differences}")
        print(f"Total differences found: {result.summary.total_differences_found}")
        
        # Show detailed results
        if result.data_comparison:
            print("\\nDetailed table results:")
            for table_name, table_comp in result.data_comparison.table_results.items():
                print(f"  {table_name}:")
                print(f"    - Matching rows: {table_comp.matching_rows}")
                print(f"    - Rows with differences: {len(table_comp.rows_with_differences)}")
                
                # Show any remaining differences (should be none if all metadata excluded)
                if table_comp.rows_with_differences:
                    print(f"    - Unexpected differences found:")
                    for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                        print(f"      Row {i} ({row_diff.row_identifier}):")
                        for field_diff in row_diff.differences:
                            print(f"        {field_diff.field_name}: '{field_diff.value_db1}' vs '{field_diff.value_db2}'")
        
        # Main test result
        main_test_passed = result.summary.total_differences_found == 0
        
        # Run additional tests
        custom_patterns_passed = test_custom_patterns()
        disabled_detection_passed = test_disable_auto_detection()
        
        # Summary
        print("\\n" + "="*60)
        print("TEST SUMMARY:")
        if main_test_passed:
            print("✅ Main Test: All metadata properly excluded!")
        else:
            print(f"❌ Main Test: Found {result.summary.total_differences_found} unexpected differences")
        
        if custom_patterns_passed:
            print("✅ Custom Patterns: Working correctly!")
        else:
            print("❌ Custom Patterns: Not working as expected")
        
        if disabled_detection_passed:
            print("✅ Disabled Auto-Detection: Working correctly!")
        else:
            print("❌ Disabled Auto-Detection: Not working as expected")
        
        overall_success = main_test_passed and custom_patterns_passed and disabled_detection_passed
        print(f"\\nOverall Result: {'✅ SUCCESS' if overall_success else '❌ SOME TESTS FAILED'}")

if __name__ == "__main__":
    main()
