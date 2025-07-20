#!/usr/bin/env python3
"""
Example demonstrating user-specified column exclusions.

This example shows how to exclude specific columns that shouldn't be compared,
such as internal notes, debug fields, or temporary data.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_example_database(db_path, environment="prod"):
    """Create an example database with environment-specific data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a users table with various types of columns
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT,
            -- Columns that might differ between environments
            internal_notes TEXT,
            debug_flag TEXT,
            admin_comments TEXT,
            environment_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    if environment == "prod":
        cursor.execute('''
            INSERT INTO users (username, email, status, internal_notes, debug_flag, admin_comments, environment_data)
            VALUES 
                ('alice', 'alice@company.com', 'active', 'Production user', 'prod_debug', 'Created in prod', 'prod_env_data'),
                ('bob', 'bob@company.com', 'active', 'Another prod user', 'prod_debug_2', 'Prod admin note', 'prod_specific')
        ''')
    else:  # staging
        cursor.execute('''
            INSERT INTO users (username, email, status, internal_notes, debug_flag, admin_comments, environment_data)
            VALUES 
                ('alice', 'alice@company.com', 'active', 'Staging environment user', 'stage_debug', 'Created in staging', 'stage_env_data'),
                ('bob', 'bob@company.com', 'active', 'Staging user copy', 'stage_debug_2', 'Staging admin note', 'stage_specific')
        ''')
    
    conn.commit()
    conn.close()

def demo_column_exclusion():
    """Demonstrate column exclusion functionality"""
    print("=== Column Exclusion Demo ===")
    print("This demo shows how to exclude specific columns from database comparison.")
    print("We'll compare production and staging databases that have identical business data")
    print("but different internal/debug/admin fields.")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        # Create databases
        print("Creating production database...")
        create_example_database(prod_db, "prod")
        
        print("Creating staging database...")
        create_example_database(staging_db, "staging")
        
        # First comparison without exclusions
        print("\n" + "="*60)
        print("COMPARISON 1: Without Column Exclusions")
        print("="*60)
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            verbose=True,
            output_format=['json']
        )
        comparator.set_comparison_options(options)
        result1 = comparator.compare()
        
        print(f"Differences found: {result1.summary.total_differences_found}")
        
        # Second comparison with column exclusions
        print("\n" + "="*60)
        print("COMPARISON 2: With User Column Exclusions")
        print("="*60)
        print("Excluding: internal_notes, debug_flag, admin_comments, environment_data")
        print()
        
        options_with_exclusions = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            excluded_columns=[
                'internal_notes',
                'debug_flag', 
                'admin_comments',
                'environment_data'
            ],
            verbose=True,
            output_format=['json']
        )
        comparator.set_comparison_options(options_with_exclusions)
        result2 = comparator.compare()
        
        print(f"Differences found: {result2.summary.total_differences_found}")
        
        # Third comparison with pattern-based exclusions
        print("\n" + "="*60)
        print("COMPARISON 3: With Pattern-Based Exclusions")
        print("="*60)
        print("Using patterns: .*internal.*, .*debug.*, .*admin.*, .*environment.*")
        print()
        
        options_with_patterns = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            excluded_column_patterns=[
                r'.*internal.*',
                r'.*debug.*',
                r'.*admin.*',
                r'.*environment.*'
            ],
            verbose=True,
            output_format=['json']
        )
        comparator.set_comparison_options(options_with_patterns)
        result3 = comparator.compare()
        
        print(f"Differences found: {result3.summary.total_differences_found}")
        
        # Summary
        print("\n" + "="*60)
        print("SUMMARY")
        print("="*60)
        
        if result1.summary.total_differences_found > 0:
            print("✅ Without exclusions: Found differences (as expected)")
        else:
            print("❌ Without exclusions: Should have found differences")
        
        if result2.summary.total_differences_found == 0:
            print("✅ With explicit exclusions: No differences found (business data identical)")
        else:
            print("❌ With explicit exclusions: Still found differences")
        
        if result3.summary.total_differences_found == 0:
            print("✅ With pattern exclusions: No differences found (business data identical)")
        else:
            print("❌ With pattern exclusions: Still found differences")
        
        print("\n💡 Use Case:")
        print("   This is perfect for comparing databases across environments where")
        print("   business data should be identical but environment-specific fields differ.")

if __name__ == "__main__":
    demo_column_exclusion()
