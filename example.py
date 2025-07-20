#!/usr/bin/env python3
"""
Example usage of the SQLite Database Comparator.

This script demonstrates how to use the dbchecker library programmatically.
"""

import sqlite3
import os
import tempfile
from datetime import datetime

from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions


def create_sample_database(db_path: str, include_differences: bool = False):
    """Create a sample SQLite database for testing"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute("""
        CREATE TABLE users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL,
            is_active INTEGER DEFAULT 1
        )
    """)
    
    # Create posts table
    cursor.execute("""
        CREATE TABLE posts (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)
    
    # Insert sample data
    base_data = [
        ('user1', 'john_doe', 'john@example.com', '2024-01-01 10:00:00', 1),
        ('user2', 'jane_smith', 'jane@example.com', '2024-01-02 11:00:00', 1),
        ('user3', 'bob_wilson', 'bob@example.com', '2024-01-03 12:00:00', 0),
    ]
    
    for i, (user_id, username, email, created_at, is_active) in enumerate(base_data):
        # Use different UUIDs but same other data
        if include_differences and i == 0:
            # Add a difference in the first user
            cursor.execute("""
                INSERT INTO users (id, username, email, created_at, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{user_id}-different", username, "john.doe@example.com", created_at, is_active))
        else:
            cursor.execute("""
                INSERT INTO users (id, username, email, created_at, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{user_id}", username, email, created_at, is_active))
    
    # Insert posts
    posts_data = [
        ('post1', 'user1', 'First Post', 'This is the first post content', '2024-01-01 15:00:00'),
        ('post2', 'user2', 'Second Post', 'This is the second post content', '2024-01-02 16:00:00'),
    ]
    
    for post_id, user_id, title, content, created_at in posts_data:
        if include_differences and post_id == 'post1':
            # Add different content for first post
            cursor.execute("""
                INSERT INTO posts (id, user_id, title, content, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{post_id}", f"uuid-{user_id}", title, "Modified content here", created_at))
        else:
            cursor.execute("""
                INSERT INTO posts (id, user_id, title, content, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{post_id}", f"uuid-{user_id}", title, content, created_at))
    
    conn.commit()
    conn.close()


def demo_identical_databases():
    """Demo comparing two identical databases (except for UUIDs)"""
    print("=== Demo 1: Comparing Identical Databases ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create identical databases
        create_sample_database(db1_path, include_differences=False)
        create_sample_database(db2_path, include_differences=False)
        
        # Compare databases
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path,
            uuid_columns=['id', 'user_id']  # Explicitly mark UUID columns
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            verbose=True,
            output_format=['json']
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Comparison completed at: {result.timestamp}")
        print(f"Schema identical: {result.schema_comparison.identical if result.schema_comparison else 'Not compared'}")
        print(f"Total differences: {result.data_comparison.total_differences if result.data_comparison else 'Not compared'}")
        print(f"Tables: {result.summary.identical_tables} identical, {result.summary.tables_with_differences} with differences")
        print()


def demo_different_databases():
    """Demo comparing databases with actual differences"""
    print("=== Demo 2: Comparing Databases with Differences ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create databases with differences
        create_sample_database(db1_path, include_differences=False)
        create_sample_database(db2_path, include_differences=True)
        
        # Compare databases
        comparator = DatabaseComparator(
            db1_path=db1_path,
            db2_path=db2_path,
            uuid_columns=['id', 'user_id']
        )
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            verbose=True,
            output_format=['json', 'markdown']
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Comparison completed at: {result.timestamp}")
        print(f"Schema identical: {result.schema_comparison.identical if result.schema_comparison else 'Not compared'}")
        print(f"Total differences: {result.data_comparison.total_differences if result.data_comparison else 'Not compared'}")
        print(f"Tables: {result.summary.identical_tables} identical, {result.summary.tables_with_differences} with differences")
        
        # Show detailed differences
        if result.data_comparison:
            for table_name, table_comp in result.data_comparison.table_results.items():
                if table_comp.rows_with_differences:
                    print(f"\nDifferences in table '{table_name}':")
                    for row_diff in table_comp.rows_with_differences[:3]:  # Show first 3
                        print(f"  Row {row_diff.row_identifier}:")
                        for field_diff in row_diff.differences:
                            print(f"    {field_diff.field_name}: '{field_diff.value_db1}' vs '{field_diff.value_db2}'")
        print()


def demo_report_generation():
    """Demo generating reports in different formats"""
    print("=== Demo 3: Report Generation ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create databases with differences
        create_sample_database(db1_path, include_differences=False)
        create_sample_database(db2_path, include_differences=True)
        
        # Compare databases
        comparator = DatabaseComparator(db1_path, db2_path, ['id', 'user_id'])
        
        options = ComparisonOptions(
            verbose=False,
            output_format=['json', 'html', 'markdown', 'csv']
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        # Generate reports in current directory
        comparator.generate_reports(result, output_dir=".", filename_prefix="demo_report")
        
        print("Reports generated:")
        for format_type in options.output_format:
            filename = f"demo_report.{format_type}"
            if os.path.exists(filename):
                size = os.path.getsize(filename)
                print(f"  {filename} ({size} bytes)")
        print()


def main():
    """Run all demos"""
    print("SQLite Database Comparator - Demo Script")
    print("=" * 50)
    print()
    
    try:
        demo_identical_databases()
        demo_different_databases()
        demo_report_generation()
        
        print("All demos completed successfully!")
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
