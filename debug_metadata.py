#!/usr/bin/env python3
"""
Debug script to understand what's happening with the metadata exclusion.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_simple_test_database(db_path, add_differences=False):
    """Create a simple test database for debugging"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE simple_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            created_at TEXT,
            session_id TEXT
        )
    ''')
    
    if add_differences:
        data = ("john_doe", "john@example.com", "2024-01-01 10:05:00", "session_456")
    else:
        data = ("john_doe", "john@example.com", "2024-01-01 10:00:00", "session_123")
    
    cursor.execute('''
        INSERT INTO simple_test (name, email, created_at, session_id) VALUES (?, ?, ?, ?)
    ''', data)
    
    conn.commit()
    conn.close()

def main():
    print("=== Debug Metadata Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "db1.db")
        db2_path = os.path.join(temp_dir, "db2.db")
        
        print("Creating simple test databases...")
        create_simple_test_database(db1_path, add_differences=False)
        create_simple_test_database(db2_path, add_differences=True)
        
        print("\\nTesting with metadata exclusion...")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        
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
        
        print(f"\\nResults:")
        print(f"Total differences: {result.summary.total_differences_found}")
        
        if result.data_comparison:
            for table_name, table_comp in result.data_comparison.table_results.items():
                print(f"\\nTable {table_name}:")
                print(f"  - Rows in DB1: {table_comp.row_count_db1}")
                print(f"  - Rows in DB2: {table_comp.row_count_db2}")
                print(f"  - Matching rows: {table_comp.matching_rows}")
                print(f"  - Rows with differences: {len(table_comp.rows_with_differences)}")
                print(f"  - Rows only in DB1: {len(table_comp.rows_only_in_db1)}")
                print(f"  - Rows only in DB2: {len(table_comp.rows_only_in_db2)}")
                
                if table_comp.rows_only_in_db1:
                    print(f"  - Rows only in DB1: {table_comp.rows_only_in_db1}")
                if table_comp.rows_only_in_db2:
                    print(f"  - Rows only in DB2: {table_comp.rows_only_in_db2}")
                if table_comp.rows_with_differences:
                    print(f"  - Field differences:")
                    for row_diff in table_comp.rows_with_differences:
                        for field_diff in row_diff.differences:
                            print(f"    {field_diff.field_name}: '{field_diff.value_db1}' vs '{field_diff.value_db2}'")

if __name__ == "__main__":
    main()
