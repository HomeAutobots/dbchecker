#!/usr/bin/env python3
"""
Test the enhanced unique identifier pattern detection and normalization functionality.
"""

import os
import sys

# Add the dbchecker package to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dbchecker_path = os.path.join(project_root, 'dbchecker')
sys.path.insert(0, dbchecker_path)

from dbchecker.uuid_handler import UUIDHandler
from dbchecker.models import ComparisonOptions


def test_pattern_detection():
    """Test automatic pattern detection for unique identifiers"""
    print("Testing pattern detection...")
    
    handler = UUIDHandler()
    
    # Test data with different patterns
    test_cases = [
        {
            'values': ['report-001', 'report-002', 'report-003'],
            'expected_pattern': 'prefix-number'
        },
        {
            'values': ['001-report', '002-report', '003-report'],
            'expected_pattern': 'number-suffix'
        },
        {
            'values': ['ABC123456', 'DEF789012', 'GHI345678'],
            'expected_pattern': 'code-number'
        },
        {
            'values': ['user_001', 'user_002', 'user_003'],
            'expected_pattern': 'prefix_number'
        }
    ]
    
    for test_case in test_cases:
        detected = handler._detect_unique_id_pattern(test_case['values'])
        expected = test_case['expected_pattern']
        
        print(f"  Values: {test_case['values'][:2]}... -> Detected: {detected}, Expected: {expected}")
        assert detected == expected, f"Expected {expected}, got {detected}"
    
    print("✅ Pattern detection test passed")


def test_normalization():
    """Test pattern normalization functionality"""
    print("Testing pattern normalization...")
    
    handler = UUIDHandler()
    
    # Test normalization options
    options = ComparisonOptions(
        unique_id_normalize_patterns=[
            {
                'pattern': r'^(report|record)-(author|item)-(\d+)$',
                'replacement': r'id-\2-\3'
            }
        ]
    )
    
    test_cases = [
        ('report-author-001', 'id-author-001'),
        ('record-author-001', 'id-author-001'),
        ('report-item-123', 'id-item-123'),
        ('record-item-123', 'id-item-123'),
        ('something-else', 'something-else')  # No match, unchanged
    ]
    
    for input_val, expected in test_cases:
        result = handler._normalize_unique_id(input_val, options)
        print(f"  {input_val} -> {result} (expected: {expected})")
        assert result == expected, f"Expected {expected}, got {result}"
    
    print("✅ Pattern normalization test passed")


def test_uuid_statistics_with_patterns():
    """Test UUID statistics collection with pattern detection"""
    print("Testing UUID statistics with pattern detection...")
    
    handler = UUIDHandler()
    
    # Test data with custom ID patterns
    test_data = [
        {'id': 'report-001', 'name': 'Item 1', 'category_id': 'cat-001'},
        {'id': 'report-002', 'name': 'Item 2', 'category_id': 'cat-002'},
        {'id': 'report-003', 'name': 'Item 3', 'category_id': 'cat-001'},
    ]
    
    uuid_columns = ['id', 'category_id']
    
    options = ComparisonOptions(
        unique_id_patterns=[r'^(report|cat)-\d+$'],
        unique_id_normalize_patterns=[
            {
                'pattern': r'^(report|cat)-(\d+)$',
                'replacement': r'id-\2'
            }
        ]
    )
    
    stats = handler.collect_uuid_statistics(test_data, uuid_columns, options)
    
    # Verify statistics
    assert stats['total_uuid_values'] == 6  # 3 rows * 2 UUID columns
    assert stats['unique_uuid_values'] == 5  # report-001,002,003 + cat-001,002
    assert 'detected_patterns' in stats
    assert 'normalized_values' in stats
    
    print(f"  Total UUID values: {stats['total_uuid_values']}")
    print(f"  Unique UUID values: {stats['unique_uuid_values']}")
    print(f"  Detected patterns: {stats['detected_patterns']}")
    print("✅ UUID statistics with patterns test passed")


def test_normalized_comparison():
    """Test comparing normalized unique IDs between datasets"""
    print("Testing normalized unique ID comparison...")
    
    handler = UUIDHandler()
    
    # Two datasets with different prefixes but same logical data
    data1 = [
        {'id': 'report-001', 'name': 'Item 1'},
        {'id': 'report-002', 'name': 'Item 2'},
        {'id': 'report-003', 'name': 'Item 3'},
    ]
    
    data2 = [
        {'id': 'record-001', 'name': 'Item 1'},
        {'id': 'record-002', 'name': 'Item 2'},
        {'id': 'record-003', 'name': 'Item 3'},
    ]
    
    uuid_columns = ['id']
    
    options = ComparisonOptions(
        unique_id_normalize_patterns=[
            {
                'pattern': r'^(report|record)-(\d+)$',
                'replacement': r'id-\2'
            }
        ]
    )
    
    comparison = handler.compare_normalized_unique_ids(data1, data2, uuid_columns, options)
    
    print(f"  Normalized matches: {comparison['normalized_matches']}")
    print(f"  Total comparisons: {comparison['total_comparisons']}")
    print(f"  Match percentage: {comparison['match_percentage']:.1f}%")
    
    # Should have 100% match after normalization
    assert comparison['match_percentage'] == 100.0, f"Expected 100% match, got {comparison['match_percentage']}"
    
    print("✅ Normalized comparison test passed")


def main():
    """Run all tests"""
    print("=== Testing Enhanced Unique Identifier Detection ===")
    print()
    
    try:
        test_pattern_detection()
        test_normalization()
        test_uuid_statistics_with_patterns()
        test_normalized_comparison()
        
        print()
        print("🎉 All enhanced UUID tests passed!")
        print()
        print("The system can now:")
        print("• Detect custom unique identifier patterns (like report-123, record-456)")
        print("• Normalize different patterns to equivalent forms")
        print("• Track logical matches even when raw values differ")
        print("• Provide detailed pattern analysis and statistics")
        print()
        print("Next steps:")
        print("1. Run the example: python example_unique_id_patterns.py")
        print("2. Try CLI: dbchecker db1.db db2.db --uuid-comparison-mode include_with_tracking --unique-id-patterns '^(report|record)-\\d+$'")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
