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
            # Add differences in the first user
            cursor.execute("""
                INSERT INTO users (id, username, email, created_at, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{user_id}-different", username, "john.doe@example.com", created_at, 0))  # Changed email and is_active
        elif include_differences and i == 1:
            # Add difference in second user
            cursor.execute("""
                INSERT INTO users (id, username, email, created_at, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{user_id}", "jane_doe", email, created_at, is_active))  # Changed username
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
            # Add different content and title for first post
            cursor.execute("""
                INSERT INTO posts (id, user_id, title, content, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{post_id}", f"uuid-{user_id}", "Modified First Post", "This content has been completely changed!", created_at))
        else:
            cursor.execute("""
                INSERT INTO posts (id, user_id, title, content, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (f"uuid-{post_id}", f"uuid-{user_id}", title, content, created_at))
    
    conn.commit()
    conn.close()


def display_differences(result):
    """Display detailed differences in a readable format"""
    if not result.data_comparison or not result.data_comparison.table_results:
        print("No data differences found.")
        return
    
    total_differences = 0
    has_any_differences = False
    
    for table_name, table_comp in result.data_comparison.table_results.items():
        has_table_differences = False
        
        # Check for field differences in matching rows
        if table_comp.rows_with_differences:
            has_table_differences = True
            has_any_differences = True
            print(f"\n{'='*60}")
            print(f"FIELD DIFFERENCES IN TABLE: {table_name.upper()}")
            print(f"{'='*60}")
            print(f"Total rows with field differences: {len(table_comp.rows_with_differences)}")
            print(f"Total rows in DB1: {table_comp.row_count_db1}")
            print(f"Total rows in DB2: {table_comp.row_count_db2}")
            print()
            
            for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                print(f"Difference #{i} - Row: {row_diff.row_identifier}")
                print("-" * 40)
                
                for field_diff in row_diff.differences:
                    print(f"  Field: {field_diff.field_name}")
                    print(f"    Database 1: '{field_diff.value_db1}'")
                    print(f"    Database 2: '{field_diff.value_db2}'")
                    print()
                
                total_differences += len(row_diff.differences)
        
        # Check for rows only in DB1
        if table_comp.rows_only_in_db1:
            has_table_differences = True
            has_any_differences = True
            print(f"\n{'='*60}")
            print(f"ROWS ONLY IN DB1 - TABLE: {table_name.upper()}")
            print(f"{'='*60}")
            print(f"Total rows only in DB1: {len(table_comp.rows_only_in_db1)}")
            print()
            
            for i, row in enumerate(table_comp.rows_only_in_db1, 1):
                print(f"Row #{i}: {dict(row)}")
                total_differences += 1
            print()
        
        # Check for rows only in DB2
        if table_comp.rows_only_in_db2:
            has_table_differences = True
            has_any_differences = True
            print(f"\n{'='*60}")
            print(f"ROWS ONLY IN DB2 - TABLE: {table_name.upper()}")
            print(f"{'='*60}")
            print(f"Total rows only in DB2: {len(table_comp.rows_only_in_db2)}")
            print()
            
            for i, row in enumerate(table_comp.rows_only_in_db2, 1):
                print(f"Row #{i}: {dict(row)}")
                total_differences += 1
            print()
        
        if not has_table_differences:
            print(f"\nTable '{table_name}': No differences found (✓)")
    
    if has_any_differences:
        print(f"\n{'='*60}")
        print(f"SUMMARY: {total_differences} total differences found across all tables")
        print(f"{'='*60}")
    else:
        print("\nSUMMARY: No differences found in any table")


def display_schema_differences(result):
    """Display schema differences in detail"""
    if not result.schema_comparison:
        print("Schema comparison not performed.")
        return
    
    if result.schema_comparison.identical:
        print("✓ Database schemas are identical")
        return
    
    print(f"\n{'='*60}")
    print("SCHEMA DIFFERENCES FOUND")
    print(f"{'='*60}")
    
    for table_name, table_diff in result.schema_comparison.table_differences.items():
        if not table_diff.identical:
            print(f"\nTable: {table_name}")
            print("-" * 30)
            
            # Show column differences
            if hasattr(table_diff, 'column_differences'):
                for col_name, col_diff in table_diff.column_differences.items():
                    if not col_diff.identical:
                        print(f"  Column '{col_name}': DIFFERENT")
                        # Add more specific column difference details here
            
            # Show constraint differences
            if hasattr(table_diff, 'constraint_differences'):
                for constraint_diff in table_diff.constraint_differences:
                    print(f"  Constraint difference: {constraint_diff}")


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
            output_format=['json'],
            parallel_tables=False  # Disable parallel processing
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Comparison completed at: {result.timestamp}")
        
        # Display schema results
        display_schema_differences(result)
        
        # Display data differences
        display_differences(result)
        
        print(f"\nSUMMARY: {result.summary.identical_tables} identical tables, {result.summary.tables_with_differences} with differences")
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
            output_format=['json', 'markdown'],
            parallel_tables=False  # Disable parallel processing
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Comparison completed at: {result.timestamp}")
        
        # Display schema results
        display_schema_differences(result)
        
        # Display detailed data differences
        display_differences(result)
        
        print(f"\nSUMMARY: {result.summary.identical_tables} identical tables, {result.summary.tables_with_differences} with differences")
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
            output_format=['json', 'html', 'markdown', 'csv'],
            parallel_tables=False  # Disable parallel processing
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        # Display differences before generating reports
        print("Differences found:")
        display_differences(result)
        
        # Generate reports in current directory
        comparator.generate_reports(result, output_dir=".", filename_prefix="demo_report")
        
        print("\nReports generated:")
        for format_type in options.output_format:
            filename = f"demo_report.{format_type}"
            if os.path.exists(filename):
                size = os.path.getsize(filename)
                print(f"  {filename} ({size} bytes)")
        print()


def demo_detailed_comparison():
    """Demo showing very detailed comparison output"""
    print("=== Demo 4: Detailed Comparison Analysis ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "database1.db")
        db2_path = os.path.join(temp_dir, "database2.db")
        
        # Create databases with multiple types of differences
        create_sample_database(db1_path, include_differences=False)
        create_sample_database(db2_path, include_differences=True)
        
        # Compare databases
        comparator = DatabaseComparator(db1_path, db2_path, ['id', 'user_id'])
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            verbose=True,
            compare_schema=True,
            compare_data=True,
            output_format=['json'],
            parallel_tables=False
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        
        print(f"Detailed Analysis Results:")
        print(f"Comparison completed at: {result.timestamp}")
        print()
        
        # Show what was compared
        if result.data_comparison:
            print("Tables analyzed:")
            for table_name, table_comp in result.data_comparison.table_results.items():
                status = "✓ IDENTICAL" if not table_comp.rows_with_differences else f"✗ {len(table_comp.rows_with_differences)} DIFFERENCES"
                print(f"  {table_name}: {status}")
            print()
        
        # Show detailed differences
        display_schema_differences(result)
        display_differences(result)
        
        # Summary
        print(f"\nFINAL SUMMARY:")
        print(f"Schema identical: {result.schema_comparison.identical if result.schema_comparison else 'Not compared'}")
        print(f"Data identical: {result.summary.total_differences_found == 0}")
        print(f"Total field differences: {result.summary.total_differences_found}")
        print(f"Tables with differences: {result.summary.tables_with_differences}")
        print(f"Identical tables: {result.summary.identical_tables}")


def main():
    """Run all demos"""
    print("SQLite Database Comparator - Demo Script")
    print("=" * 50)
    print()
    
    try:
        demo_identical_databases()
        demo_different_databases()
        demo_report_generation()
        demo_detailed_comparison()
        
        print("All demos completed successfully!")
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
