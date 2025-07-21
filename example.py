#!/usr/bin/env python3
"""
Comprehensive DBChecker Example

This example demonstrates the key features of DBChecker including:
- Column exclusions (explicit and pattern-based)
- UUID handling and tracking
- Timestamp detection
- Comprehensive database comparison

Use this as a starting point for understanding how to use DBChecker effectively.
"""

import tempfile
import os
import sqlite3
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

def create_comprehensive_database(db_path, environment="prod"):
    """Create a comprehensive example database with various data types"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create a users table with various types of columns
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            user_uuid TEXT NOT NULL,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            status TEXT,
            -- Environment-specific columns that should be excluded
            internal_notes TEXT,
            debug_flag TEXT,
            admin_comments TEXT,
            environment_data TEXT,
            -- Timestamps that are auto-detected
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create a reports table demonstrating UUID tracking
    cursor.execute('''
        CREATE TABLE reports (
            id INTEGER PRIMARY KEY,
            report_id TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            author_uuid TEXT,
            status TEXT,
            temp_data TEXT,
            debug_info TEXT,
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    if environment == "prod":
        # Production data
        cursor.execute('''
            INSERT INTO users (user_uuid, username, email, status, internal_notes, debug_flag, admin_comments, environment_data)
            VALUES 
                ('user-123-abc', 'alice', 'alice@company.com', 'active', 'Production user', 'prod_debug', 'Created in prod', 'prod_env_data'),
                ('user-456-def', 'bob', 'bob@company.com', 'active', 'Another prod user', 'prod_debug_2', 'Prod admin note', 'prod_specific')
        ''')
        
        cursor.execute('''
            INSERT INTO reports (report_id, title, content, author_uuid, status, temp_data, debug_info)
            VALUES 
                ('report-001-xyz', 'Q1 Report', 'Quarterly analysis', 'user-123-abc', 'published', 'prod_temp', 'prod_debug_report'),
                ('report-002-xyz', 'Sales Report', 'Monthly sales data', 'user-456-def', 'draft', 'prod_temp_2', 'prod_debug_sales')
        ''')
    else:  # staging
        # Staging data - same business logic, different environment fields and UUIDs
        cursor.execute('''
            INSERT INTO users (user_uuid, username, email, status, internal_notes, debug_flag, admin_comments, environment_data)
            VALUES 
                ('user-123-stage', 'alice', 'alice@company.com', 'active', 'Staging environment user', 'stage_debug', 'Created in staging', 'stage_env_data'),
                ('user-456-stage', 'bob', 'bob@company.com', 'active', 'Staging user copy', 'stage_debug_2', 'Staging admin note', 'stage_specific')
        ''')
        
        cursor.execute('''
            INSERT INTO reports (report_id, title, content, author_uuid, status, temp_data, debug_info)
            VALUES 
                ('record-001-abc', 'Q1 Report', 'Quarterly analysis', 'user-123-stage', 'published', 'stage_temp', 'stage_debug_report'),
                ('record-002-abc', 'Sales Report', 'Monthly sales data', 'user-456-stage', 'draft', 'stage_temp_2', 'stage_debug_sales')
        ''')
    
    conn.commit()
    conn.close()

def demo_basic_comparison():
    """Demonstrate basic database comparison without any exclusions"""
    print("=== DEMO 1: Basic Comparison ===")
    print("Comparing databases without any exclusions - will show all differences")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        create_comprehensive_database(prod_db, "prod")
        create_comprehensive_database(staging_db, "staging")
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(verbose=True)
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Total differences found: {result.summary.total_differences_found}")
        print("This shows all the environment-specific differences.")

def demo_column_exclusions():
    """Demonstrate column exclusion functionality"""
    print("\n" + "="*60)
    print("DEMO 2: Column Exclusions")
    print("="*60)
    print("Excluding environment-specific columns to focus on business data")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        create_comprehensive_database(prod_db, "prod")
        create_comprehensive_database(staging_db, "staging")
        
        # Explicit column exclusions
        print("Using explicit column exclusions:")
        print("- internal_notes, debug_flag, admin_comments, environment_data")
        print("- temp_data, debug_info")
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(
            excluded_columns=[
                'internal_notes', 'debug_flag', 'admin_comments', 'environment_data',
                'temp_data', 'debug_info'
            ],
            auto_detect_timestamps=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Differences found: {result.summary.total_differences_found}")

def demo_pattern_exclusions():
    """Demonstrate pattern-based column exclusions"""
    print("\n" + "="*60)
    print("DEMO 3: Pattern-Based Exclusions")
    print("="*60)
    print("Using regex patterns to exclude columns")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        create_comprehensive_database(prod_db, "prod")
        create_comprehensive_database(staging_db, "staging")
        
        print("Using pattern exclusions:")
        print("- .*debug.*   (matches debug_flag, debug_info)")
        print("- .*temp.*    (matches temp_data)")
        print("- .*admin.*   (matches admin_comments)")
        print("- .*internal.* (matches internal_notes)")
        print("- .*environment.* (matches environment_data)")
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(
            excluded_column_patterns=[
                r'.*debug.*',
                r'.*temp.*',
                r'.*admin.*',
                r'.*internal.*',
                r'.*environment.*'
            ],
            auto_detect_timestamps=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Differences found: {result.summary.total_differences_found}")

def demo_uuid_tracking():
    """Demonstrate UUID handling functionality"""
    print("\n" + "="*60)
    print("DEMO 4: UUID Handling")
    print("="*60)
    print("Handling UUIDs in database comparison")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        create_comprehensive_database(prod_db, "prod")
        create_comprehensive_database(staging_db, "staging")
        
        print("Using UUID comparison with tracking to handle different UUID values")
        print("while comparing the underlying business relationships")
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(
            # Exclude environment-specific columns
            excluded_column_patterns=[r'.*debug.*', r'.*temp.*', r'.*admin.*', r'.*internal.*', r'.*environment.*'],
            # Configure UUID handling
            uuid_comparison_mode='include_with_tracking',
            auto_detect_timestamps=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"Differences found: {result.summary.total_differences_found}")
        print("This shows how UUID tracking provides statistics while comparing business data")

def demo_comprehensive_comparison():
    """Demonstrate all features working together"""
    print("\n" + "="*60)
    print("DEMO 5: Comprehensive Comparison")
    print("="*60)
    print("Using all features together for production-ready comparison")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        create_comprehensive_database(prod_db, "prod")
        create_comprehensive_database(staging_db, "staging")
        
        print("Configuration:")
        print("✓ Auto-detect timestamps")
        print("✓ Auto-detect UUIDs") 
        print("✓ UUID comparison with tracking")
        print("✓ Pattern-based column exclusions")
        print("✓ Verbose output")
        
        comparator = DatabaseComparator(prod_db, staging_db)
        options = ComparisonOptions(
            # Automatic detection
            auto_detect_timestamps=True,
            auto_detect_uuids=True,
            
            # UUID handling
            uuid_comparison_mode='include_with_tracking',
            
            # Column exclusions
            excluded_column_patterns=[
                r'.*debug.*',
                r'.*temp.*', 
                r'.*admin.*',
                r'.*internal.*',
                r'.*environment.*'
            ],
            
            # Output configuration
            verbose=True,
            output_format=['json', 'html']
        )
        
        comparator.set_comparison_options(options)
        result = comparator.compare()
        
        print(f"\nFinal result: {result.summary.total_differences_found} differences found")
        
        if result.summary.total_differences_found == 0:
            print("✅ SUCCESS: Business data is identical between environments!")
        else:
            print("⚠️  Some differences remain - check the output for details")

def main():
    """Run all demonstration scenarios"""
    print("🔍 DBChecker Comprehensive Example")
    print("="*60)
    print("This example demonstrates key DBChecker features for comparing")
    print("databases across environments while handling common differences.")
    print()
    
    # Run all demos
    demo_basic_comparison()
    demo_column_exclusions()
    demo_pattern_exclusions()
    demo_uuid_tracking()
    demo_comprehensive_comparison()
    
    print("\n" + "="*60)
    print("💡 KEY TAKEAWAYS")
    print("="*60)
    print("1. Use column exclusions to ignore environment-specific data")
    print("2. Pattern-based exclusions are powerful for systematic exclusions")
    print("3. UUID tracking helps compare business logic across environments")
    print("4. Combine all features for production-ready database comparison")
    print("5. Always test your configuration with known data first")
    print("\n🚀 You're ready to use DBChecker in your environment!")

if __name__ == "__main__":
    main()
