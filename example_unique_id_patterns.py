#!/usr/bin/env python3
"""
Example demonstrating unique identifier pattern detection and normalization.

This example shows how to compare databases that use different unique identifier 
patterns (like report-123 vs record-123) while tracking them as logically equivalent.
"""

import os
import sqlite3
import tempfile
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions


def create_database_with_custom_ids(db_path: str, id_prefix: str = "report"):
    """Create a test database with custom unique identifier patterns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create reports table with custom ID pattern
    cursor.execute('''
        CREATE TABLE reports (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            author_id TEXT,
            created_date DATE
        )
    ''')
    
    # Create authors table with custom ID pattern
    cursor.execute('''
        CREATE TABLE authors (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            department TEXT
        )
    ''')
    
    # Insert test data with specific ID patterns
    authors_data = [
        (f'{id_prefix}-author-001', 'John Doe', 'john@company.com', 'Engineering'),
        (f'{id_prefix}-author-002', 'Jane Smith', 'jane@company.com', 'Marketing'),
        (f'{id_prefix}-author-003', 'Bob Johnson', 'bob@company.com', 'Sales')
    ]
    
    reports_data = [
        (f'{id_prefix}-report-001', 'Q1 Engineering Report', 'Technical progress report...', f'{id_prefix}-author-001', '2024-01-15'),
        (f'{id_prefix}-report-002', 'Marketing Campaign Analysis', 'Campaign performance data...', f'{id_prefix}-author-002', '2024-01-20'),
        (f'{id_prefix}-report-003', 'Sales Performance Review', 'Sales metrics and trends...', f'{id_prefix}-author-003', '2024-01-25'),
        (f'{id_prefix}-report-004', 'Q1 System Optimization', 'System improvements made...', f'{id_prefix}-author-001', '2024-02-01'),
        (f'{id_prefix}-report-005', 'Customer Feedback Summary', 'Collected customer insights...', f'{id_prefix}-author-002', '2024-02-05')
    ]
    
    cursor.executemany('INSERT INTO authors (id, name, email, department) VALUES (?, ?, ?, ?)', authors_data)
    cursor.executemany('INSERT INTO reports (id, title, content, author_id, created_date) VALUES (?, ?, ?, ?, ?)', reports_data)
    
    conn.commit()
    conn.close()


def demo_unique_id_patterns():
    """Demonstrate unique identifier pattern detection and normalization"""
    print("=== Unique Identifier Pattern Detection Demo ===")
    print("This demo shows how to handle different unique ID patterns between databases")
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create two databases with different ID patterns but same logical data
        prod_db = os.path.join(temp_dir, "production.db")
        staging_db = os.path.join(temp_dir, "staging.db")
        
        print("Creating databases with different ID patterns...")
        create_database_with_custom_ids(prod_db, "report")      # Uses "report-author-001", "report-report-001"
        create_database_with_custom_ids(staging_db, "record")   # Uses "record-author-001", "record-report-001"
        print("✅ Created production.db with 'report-' prefixed IDs")
        print("✅ Created staging.db with 'record-' prefixed IDs")
        print()
        
        # Demo 1: Traditional comparison (will show many differences)
        print("--- Demo 1: Traditional UUID Exclusion ---")
        comparator = DatabaseComparator(db1_path=prod_db, db2_path=staging_db)
        options = ComparisonOptions(
            uuid_comparison_mode='exclude',
            auto_detect_uuids=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"Traditional mode (exclude UUIDs):")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print()
        
        # Demo 2: Include UUIDs without pattern normalization
        print("--- Demo 2: Include UUIDs Without Pattern Normalization ---")
        comparator = DatabaseComparator(db1_path=prod_db, db2_path=staging_db)
        options = ComparisonOptions(
            uuid_comparison_mode='include_with_tracking',
            auto_detect_uuids=True,
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"Include UUIDs without normalization:")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print(f"  UUID integrity check: {'✅ PASS' if result.summary.uuid_integrity_check else '❌ FAIL'}")
        print()
        
        # Demo 3: Include UUIDs WITH pattern normalization (NEW FEATURE)
        print("--- Demo 3: Include UUIDs WITH Pattern Normalization ---")
        comparator = DatabaseComparator(db1_path=prod_db, db2_path=staging_db)
        options = ComparisonOptions(
            uuid_comparison_mode='include_with_tracking',
            auto_detect_uuids=True,
            # Custom patterns to detect our unique IDs
            unique_id_patterns=[
                r'^(report|record)-(author|report)-\d+$'
            ],
            # Normalization rules to make them comparable
            unique_id_normalize_patterns=[
                {
                    'pattern': r'^(report|record)-(author|report)-(\d+)$',
                    'replacement': r'id-\2-\3'  # Convert both to 'id-author-001' or 'id-report-001'
                }
            ],
            verbose=True
        )
        comparator.set_comparison_options(options)
        
        result = comparator.compare()
        print(f"Include UUIDs with pattern normalization:")
        print(f"  Tables identical: {result.summary.identical_tables}")
        print(f"  Tables with differences: {result.summary.tables_with_differences}")
        print(f"  Total differences: {result.summary.total_differences_found}")
        print(f"  UUID integrity check: {'✅ PASS' if result.summary.uuid_integrity_check else '❌ FAIL'}")
        print()
        
        # Show detailed UUID statistics with pattern detection
        if result.data_comparison:
            print("--- Detailed Pattern Detection Results ---")
            for table_name, table_comp in result.data_comparison.table_results.items():
                if table_comp.uuid_statistics:
                    stats = table_comp.uuid_statistics
                    print(f"Table: {table_name}")
                    print(f"  UUID columns: {stats.uuid_columns}")
                    print(f"  Detected patterns: {stats.detected_patterns}")
                    print(f"  Total UUID values DB1: {stats.total_uuid_values_db1}")
                    print(f"  Total UUID values DB2: {stats.total_uuid_values_db2}")
                    print(f"  Normalized matches: {stats.normalized_match_count}")
                    print(f"  Raw UUID differences: {stats.uuid_value_differences}")
                    print()
        
        print("=== Summary ===")
        print("📊 Pattern Detection:")
        print("• Automatically detected 'prefix-type-number' pattern")
        print("• report-author-001 and record-author-001 are logically equivalent")
        print("• Pattern normalization converts both to id-author-001")
        print()
        print("🎯 Use Cases:")
        print("• Compare production (report-*) vs staging (record-*) databases")
        print("• Validate data migration between systems with different ID schemes")
        print("• Verify backup integrity when ID prefixes change")
        print("• Track data relationships across different database environments")


def demo_cli_usage():
    """Show CLI usage examples for the new features"""
    print("\n=== CLI Usage Examples ===")
    print()
    print("# Basic pattern detection:")
    print("dbchecker prod.db staging.db --uuid-comparison-mode include_with_tracking")
    print()
    print("# With custom unique ID patterns:")
    print("dbchecker prod.db staging.db \\")
    print("  --uuid-comparison-mode include_with_tracking \\")
    print("  --unique-id-patterns '^(report|record)-(author|item)-\\d+$'")
    print()
    print("# With pattern normalization:")
    print("dbchecker prod.db staging.db \\")
    print("  --uuid-comparison-mode include_with_tracking \\")
    print("  --unique-id-patterns '^(report|record)-(author|item)-\\d+$' \\")
    print("  --normalize-patterns '^(report|record)-(author|item)-(\\d+)$:id-\\2-\\3'")
    print()
    print("# Multiple normalization rules:")
    print("dbchecker prod.db staging.db \\")
    print("  --uuid-comparison-mode include_with_tracking \\")
    print("  --normalize-patterns \\")
    print("    '^(report|record)-(\\w+)-(\\d+)$:id-\\2-\\3' \\")
    print("    '^(old|new)_prefix_(\\d+)$:normalized_\\2'")


if __name__ == "__main__":
    demo_unique_id_patterns()
    demo_cli_usage()
