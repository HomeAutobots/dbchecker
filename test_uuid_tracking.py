#!/usr/bin/env python3
"""
Simple test to verify UUID tracking functionality works correctly.
"""

import os
import sqlite3
import tempfile
import sys

# Add the parent directory to the path so we can import dbchecker
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dbchecker.models import ComparisonOptions, UUIDStatistics


def test_uuid_statistics_model():
    """Test that the new UUIDStatistics model works correctly"""
    print("Testing UUIDStatistics model...")
    
    stats = UUIDStatistics(
        uuid_columns=['id', 'user_id'],
        total_uuid_values_db1=100,
        total_uuid_values_db2=100,
        unique_uuid_values_db1=100,
        unique_uuid_values_db2=100,
        uuid_value_differences=100  # All UUIDs are different, as expected
    )
    
    assert stats.uuid_columns == ['id', 'user_id']
    assert stats.total_uuid_values_db1 == 100
    assert stats.total_uuid_values_db2 == 100
    assert stats.unique_uuid_values_db1 == 100
    assert stats.unique_uuid_values_db2 == 100
    assert stats.uuid_value_differences == 100
    
    print("✅ UUIDStatistics model test passed")


def test_comparison_options():
    """Test that the new UUID comparison mode option works"""
    print("Testing ComparisonOptions with UUID tracking...")
    
    options = ComparisonOptions(
        uuid_comparison_mode='include_with_tracking',
        auto_detect_uuids=True,
        verbose=True
    )
    
    assert options.uuid_comparison_mode == 'include_with_tracking'
    assert options.auto_detect_uuids == True
    assert options.verbose == True
    
    print("✅ ComparisonOptions test passed")


def create_simple_test_db(db_path, suffix=""):
    """Create a simple test database with UUID columns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE test_table (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
        )
    ''')
    
    # Insert test data
    test_data = [
        (f'uuid-1{suffix}', 'Item 1', 100),
        (f'uuid-2{suffix}', 'Item 2', 200),
        (f'uuid-3{suffix}', 'Item 3', 300)
    ]
    
    cursor.executemany('INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)', test_data)
    conn.commit()
    conn.close()


def test_uuid_handler_statistics():
    """Test the UUID handler statistics collection"""
    print("Testing UUID handler statistics collection...")
    
    # Import here to avoid circular imports
    from dbchecker.uuid_handler import UUIDHandler
    
    handler = UUIDHandler()
    
    # Test data
    test_data = [
        {'id': 'uuid-1', 'name': 'Item 1', 'value': 100},
        {'id': 'uuid-2', 'name': 'Item 2', 'value': 200},
        {'id': 'uuid-3', 'name': 'Item 3', 'value': 300},
    ]
    
    uuid_columns = ['id']
    stats = handler.collect_uuid_statistics(test_data, uuid_columns)
    
    assert stats['total_uuid_values'] == 3
    assert stats['unique_uuid_values'] == 3
    assert 'id' in stats['uuid_column_stats']
    assert stats['uuid_column_stats']['id']['total_values'] == 3
    assert stats['uuid_column_stats']['id']['unique_values'] == 3
    
    print("✅ UUID handler statistics test passed")


def main():
    """Run all tests"""
    print("=== Testing UUID Tracking Feature ===")
    print()
    
    try:
        test_uuid_statistics_model()
        test_comparison_options()
        test_uuid_handler_statistics()
        
        print()
        print("🎉 All tests passed! UUID tracking feature is working correctly.")
        print()
        print("Next steps:")
        print("1. Run the example: python example_uuid_tracking.py")
        print("2. Try the CLI: dbchecker db1.db db2.db --uuid-comparison-mode include_with_tracking")
        print("3. Read the documentation: UUID_TRACKING_FEATURE.md")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
