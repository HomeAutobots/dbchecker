#!/usr/bin/env python3
"""
Example demonstrating UUID tracking functionality in database comparison.

This example shows how to use the new UUID tracking mode that includes UUIDs
in comparison while tracking their statistics, allowing you to verify data
integrity even when UUIDs differ between databases.
"""

import os
import sqlite3
import tempfile
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions


def create_test_database_with_uuids(db_path: str, suffix: str = ""):
    """Create a test database with UUID columns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create users table with UUID
    cursor.execute('''
        CREATE TABLE users (
            id TEXT PRIMARY KEY,  -- UUID column
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            department_id TEXT,   -- UUID reference
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create departments table with UUID
    cursor.execute('''
        CREATE TABLE departments (
            id TEXT PRIMARY KEY,  -- UUID column
            name TEXT NOT NULL,
            manager_id TEXT       -- UUID reference
        )
    ''')
    
    # Insert test data with different UUIDs but same business logic
    departments_data = [
        (f'dept-uuid-001{suffix}', 'Engineering', f'mgr-uuid-001{suffix}'),
        (f'dept-uuid-002{suffix}', 'Sales', f'mgr-uuid-002{suffix}'),
        (f'dept-uuid-003{suffix}', 'Marketing', f'mgr-uuid-003{suffix}')
    ]
    
    users_data = [
        (f'user-uuid-001{suffix}', 'john_doe', 'john@company.com', f'dept-uuid-001{suffix}'),
        (f'user-uuid-002{suffix}', 'jane_smith', 'jane@company.com', f'dept-uuid-001{suffix}'),
        (f'user-uuid-003{suffix}', 'bob_johnson', 'bob@company.com', f'dept-uuid-002{suffix}'),
        (f'user-uuid-004{suffix}', 'alice_brown', 'alice@company.com', f'dept-uuid-002{suffix}'),
        (f'user-uuid-005{suffix}', 'charlie_davis', 'charlie@company.com', f'dept-uuid-003{suffix}')
    ]
    
    cursor.executemany('INSERT INTO departments (id, name, manager_id) VALUES (?, ?, ?)', departments_data)
    cursor.executemany('INSERT INTO users (id, username, email, department_id) VALUES (?, ?, ?, ?)', users_data)
    
    conn.commit()
    conn.close()


def demo_uuid_tracking():
    """Demonstrate UUID tracking functionality"""
    print("=== UUID Tracking Demo ===")
    print("This demo shows how to track UUIDs while including them in comparison")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "production.db")
        db2_path = os.path.join(temp_dir, "staging.db")
        
        # Create databases with different UUIDs but same business data structure
        print("Creating test databases with different UUIDs...")
        create_test_database_with_uuids(db1_path, "-prod")
        create_test_database_with_uuids(db2_path, "-stage")
        
        print("Databases created with identical business data but different UUIDs")
        print()
        
        # Demo 1: Traditional mode (exclude UUIDs)
        print("--- Demo 1: Traditional UUID Exclusion Mode ---")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        options = ComparisonOptions(
            uuid_comparison_mode='exclude',  # Traditional mode
            auto_detect_uuids=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"Traditional mode results:")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print(f"  UUID columns tracked: {result.summary.total_uuid_columns}")
        print()
        
        # Demo 2: New UUID tracking mode
        print("--- Demo 2: UUID Tracking Mode ---")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        options = ComparisonOptions(
            uuid_comparison_mode='include_with_tracking',  # New tracking mode
            auto_detect_uuids=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"UUID tracking mode results:")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print(f"  UUID columns tracked: {result.summary.total_uuid_columns}")
        print(f"  UUID values in DB1: {result.summary.total_uuid_values_db1}")
        print(f"  UUID values in DB2: {result.summary.total_uuid_values_db2}")
        print(f"  UUID integrity check: {'✅ PASS' if result.summary.uuid_integrity_check else '❌ FAIL'}")
        print()
        
        # Show detailed UUID statistics per table
        if result.data_comparison:
            print("--- Detailed UUID Statistics per Table ---")
            for table_name, table_comp in result.data_comparison.table_results.items():
                if table_comp.uuid_statistics:
                    stats = table_comp.uuid_statistics
                    print(f"Table: {table_name}")
                    print(f"  UUID columns: {stats.uuid_columns}")
                    print(f"  Total UUID values DB1: {stats.total_uuid_values_db1}")
                    print(f"  Total UUID values DB2: {stats.total_uuid_values_db2}")
                    print(f"  Unique UUID values DB1: {stats.unique_uuid_values_db1}")
                    print(f"  Unique UUID values DB2: {stats.unique_uuid_values_db2}")
                    print(f"  UUID value differences: {stats.uuid_value_differences}")
                    print()
        
        # Demo 3: Normal mode (treat UUIDs as regular columns)
        print("--- Demo 3: Normal Mode (UUIDs as Regular Columns) ---")
        comparator = DatabaseComparator(db1_path=db1_path, db2_path=db2_path)
        options = ComparisonOptions(
            uuid_comparison_mode='include_normal',  # Treat as normal columns
            auto_detect_uuids=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"Normal mode results (UUIDs treated as regular columns):")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print(f"  UUID columns tracked: {result.summary.total_uuid_columns}")
        print()
        
        print("=== Summary ===")
        print("• Traditional mode: Excludes UUIDs completely")
        print("• Tracking mode: Includes UUIDs but tracks their statistics for data integrity")
        print("• Normal mode: Treats UUIDs as regular columns (will show many differences)")
        print("• Tracking mode is ideal for verifying data relationships while accounting for UUID differences")


if __name__ == "__main__":
    demo_uuid_tracking()
