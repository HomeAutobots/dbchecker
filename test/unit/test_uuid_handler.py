"""
Test cases for the UUID handler module.
"""

import unittest
import re
from unittest.mock import Mock, patch
from dbchecker.uuid_handler import UUIDHandler
from dbchecker.models import TableStructure, Column
from dbchecker.exceptions import UUIDDetectionError


class TestUUIDHandler(unittest.TestCase):
    """Test cases for UUIDHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.uuid_handler = UUIDHandler(['explicit_uuid_col'])
    
    def test_init_default(self):
        """Test UUIDHandler initialization with defaults"""
        handler = UUIDHandler()
        self.assertEqual(handler.explicit_uuid_columns, set())
        self.assertEqual(handler.custom_patterns, [])
        self.assertEqual(handler.detected_uuid_columns, {})
        self.assertEqual(len(handler.default_patterns), 4)
        self.assertEqual(handler.all_patterns, handler.default_patterns)
    
    def test_init_with_parameters(self):
        """Test UUIDHandler initialization with parameters"""
        explicit_cols = ['col1', 'col2']
        custom_patterns = [r'^custom-\d+$']
        handler = UUIDHandler(explicit_cols, custom_patterns)
        
        self.assertEqual(handler.explicit_uuid_columns, set(explicit_cols))
        self.assertEqual(handler.custom_patterns, custom_patterns)
        self.assertEqual(len(handler.all_patterns), len(handler.default_patterns) + 1)
    
    def test_is_valid_uuid_standard_format(self):
        """Test UUID validation with standard format"""
        valid_uuids = [
            '123e4567-e89b-12d3-a456-426614174000',
            'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE',
            '00000000-0000-0000-0000-000000000000'
        ]
        
        for uuid_val in valid_uuids:
            with self.subTest(uuid=uuid_val):
                self.assertTrue(self.uuid_handler.is_valid_uuid(uuid_val))
    
    def test_is_valid_uuid_no_hyphens(self):
        """Test UUID validation without hyphens"""
        valid_uuids = [
            '123e4567e89b12d3a456426614174000',
            'AAAAAAAABBBBCCCCDDDDEEEEEEEEEEEE',
            '00000000000000000000000000000000'
        ]
        
        for uuid_val in valid_uuids:
            with self.subTest(uuid=uuid_val):
                self.assertTrue(self.uuid_handler.is_valid_uuid(uuid_val))
    
    def test_is_valid_uuid_invalid_format(self):
        """Test UUID validation with invalid formats"""
        invalid_uuids = [
            'not-a-uuid',
            '123e4567-e89b-12d3-a456',  # Too short
            '123e4567-e89b-12d3-a456-426614174000-extra',  # Too long
            '',
            None,
            123456
        ]
        
        for uuid_val in invalid_uuids:
            with self.subTest(uuid=uuid_val):
                self.assertFalse(self.uuid_handler.is_valid_uuid(uuid_val))
    
    def test_is_valid_uuid_with_custom_patterns(self):
        """Test UUID validation with custom patterns"""
        handler = UUIDHandler(custom_patterns=[r'^custom-\d{4}$'])
        
        # Should match custom pattern
        self.assertTrue(handler.is_valid_uuid('custom-1234'))
        # Should not match custom pattern
        self.assertFalse(handler.is_valid_uuid('custom-12'))
        # Should still match standard UUID
        self.assertTrue(handler.is_valid_uuid('123e4567-e89b-12d3-a456-426614174000'))
    
    def test_is_uuid_column_explicit(self):
        """Test explicit UUID column detection"""
        self.assertTrue(self.uuid_handler.is_uuid_column('explicit_uuid_col'))
        self.assertFalse(self.uuid_handler.is_uuid_column('regular_column'))
    
    def test_is_uuid_column_case_insensitive(self):
        """Test explicit UUID column detection is case insensitive"""
        self.assertTrue(self.uuid_handler.is_uuid_column('EXPLICIT_UUID_COL'))
        self.assertTrue(self.uuid_handler.is_uuid_column('Explicit_Uuid_Col'))
    
    def test_is_uuid_column_pattern_matching(self):
        """Test UUID column detection by pattern"""
        uuid_columns = [
            'entity_uuid',
            'guid_field',
            'record_guid',
            'user_uuid',  # contains 'uuid'
            'item_guid'   # contains 'guid'
        ]
        
        non_uuid_columns = [
            'id',         # plain id won't match
            'user_id',    # plain id won't match
            'name',
            'description',
            'created_at',
            'is_active'
        ]
        
        for col_name in uuid_columns:
            with self.subTest(column=col_name):
                self.assertTrue(self.uuid_handler.is_uuid_column(col_name))
        
        for col_name in non_uuid_columns:
            with self.subTest(column=col_name):
                self.assertFalse(self.uuid_handler.is_uuid_column(col_name))
    
    def test_is_uuid_column_by_type(self):
        """Test UUID column detection by column type"""
        self.assertTrue(self.uuid_handler.is_uuid_column('test_col', 'UUID'))
        self.assertTrue(self.uuid_handler.is_uuid_column('test_col', 'GUID'))
        self.assertTrue(self.uuid_handler.is_uuid_column('test_col', 'uuid'))
        self.assertTrue(self.uuid_handler.is_uuid_column('test_col', 'guid'))
        self.assertFalse(self.uuid_handler.is_uuid_column('test_col', 'VARCHAR'))
    
    def test_detect_uuid_columns_explicit_only(self):
        """Test UUID column detection with explicit columns only"""
        columns = [
            Column('id', 'INT', False, None, True),
            Column('explicit_uuid_col', 'VARCHAR', True, None, False),
            Column('name', 'VARCHAR', False, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        detected = self.uuid_handler.detect_uuid_columns(table)
        self.assertEqual(detected, ['explicit_uuid_col'])
    
    def test_detect_uuid_columns_by_type(self):
        """Test UUID column detection by column type"""
        columns = [
            Column('id', 'INT', False, None, True),
            Column('uuid_col', 'UUID', True, None, False),
            Column('guid_col', 'GUID', True, None, False),
            Column('name', 'VARCHAR', False, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        detected = self.uuid_handler.detect_uuid_columns(table)
        self.assertIn('uuid_col', detected)
        self.assertIn('guid_col', detected)
        self.assertNotIn('id', detected)
        self.assertNotIn('name', detected)
    
    def test_detect_uuid_columns_by_name_pattern(self):
        """Test UUID column detection by name pattern"""
        columns = [
            Column('user_uuid', 'VARCHAR', True, None, False),
            Column('entity_guid', 'VARCHAR', True, None, False),
            Column('regular_col', 'VARCHAR', False, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        detected = self.uuid_handler.detect_uuid_columns(table)
        self.assertIn('user_uuid', detected)
        self.assertIn('entity_guid', detected)
        self.assertNotIn('regular_col', detected)
    
    def test_detect_uuid_columns_with_sample_data(self):
        """Test UUID column detection with sample data analysis"""
        columns = [
            Column('id', 'VARCHAR', False, None, False),
            Column('name', 'VARCHAR', False, None, False),
            Column('possible_uuid', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        # Sample data where 'possible_uuid' has mostly UUID values
        sample_data = [
            {'id': '1', 'name': 'test1', 'possible_uuid': '123e4567-e89b-12d3-a456-426614174000'},
            {'id': '2', 'name': 'test2', 'possible_uuid': '223e4567-e89b-12d3-a456-426614174001'},
            {'id': '3', 'name': 'test3', 'possible_uuid': '323e4567-e89b-12d3-a456-426614174002'},
            {'id': '4', 'name': 'test4', 'possible_uuid': '423e4567-e89b-12d3-a456-426614174003'},
            {'id': '5', 'name': 'test5', 'possible_uuid': 'not-a-uuid'}  # Only one non-UUID
        ]
        
        detected = self.uuid_handler.detect_uuid_columns(table, sample_data)
        self.assertIn('possible_uuid', detected)

    def test_detect_uuid_columns_with_sample_data_exactly_80_percent(self):
        """Test UUID column detection with exactly 80% UUID ratio"""
        columns = [
            Column('uuid_col', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        # Sample data with exactly 80% UUID values (4 out of 5)
        sample_data = [
            {'uuid_col': '123e4567-e89b-12d3-a456-426614174000'},
            {'uuid_col': '223e4567-e89b-12d3-a456-426614174001'}, 
            {'uuid_col': '323e4567-e89b-12d3-a456-426614174002'},
            {'uuid_col': '423e4567-e89b-12d3-a456-426614174003'},
            {'uuid_col': 'not-a-uuid'}  # 20% non-UUID
        ]
        
        detected = self.uuid_handler.detect_uuid_columns(table, sample_data)
        self.assertIn('uuid_col', detected)  # Should be detected as 80% >= 0.8

    def test_detect_uuid_columns_sample_data_not_already_identified(self):
        """Test UUID column detection through sample data for columns not already identified"""
        # Create a handler with no explicit UUID columns
        handler = UUIDHandler([])
        
        # Column with a non-UUID name and type that won't be auto-detected
        columns = [
            Column('mysterious_col', 'VARCHAR', True, None, False)  # Won't match name patterns
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        # Sample data with mostly UUID values to trigger sample-based detection
        sample_data = [
            {'mysterious_col': '123e4567-e89b-12d3-a456-426614174000'},
            {'mysterious_col': '223e4567-e89b-12d3-a456-426614174001'},
            {'mysterious_col': '323e4567-e89b-12d3-a456-426614174002'},
            {'mysterious_col': '423e4567-e89b-12d3-a456-426614174003'},
            {'mysterious_col': '523e4567-e89b-12d3-a456-426614174004'},
        ]
        
        detected = handler.detect_uuid_columns(table, sample_data)
        self.assertIn('mysterious_col', detected)  # Should be detected via sample data analysis
    
    def test_detect_uuid_columns_with_sample_data_insufficient_ratio(self):
        """Test UUID column detection with insufficient UUID ratio in sample data"""
        columns = [
            Column('mixed_col', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        # Sample data where 'mixed_col' has less than 80% UUID values
        sample_data = [
            {'mixed_col': '123e4567-e89b-12d3-a456-426614174000'},
            {'mixed_col': 'not-a-uuid'},
            {'mixed_col': 'also-not-uuid'},
            {'mixed_col': 'another-string'},
            {'mixed_col': '423e4567-e89b-12d3-a456-426614174003'}
        ]
        
        detected = self.uuid_handler.detect_uuid_columns(table, sample_data)
        self.assertNotIn('mixed_col', detected)
    
    def test_detect_uuid_columns_with_sample_data_null_values(self):
        """Test UUID column detection with sample data containing null values"""
        columns = [
            Column('nullable_uuid', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        sample_data = [
            {'nullable_uuid': '123e4567-e89b-12d3-a456-426614174000'},
            {'nullable_uuid': None},
            {'nullable_uuid': '323e4567-e89b-12d3-a456-426614174002'},
            {'nullable_uuid': None},
        ]
        
        detected = self.uuid_handler.detect_uuid_columns(table, sample_data)
        self.assertIn('nullable_uuid', detected)
    
    def test_detect_uuid_columns_empty_sample_data(self):
        """Test UUID column detection with empty sample data"""
        columns = [
            Column('test_col', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        detected = self.uuid_handler.detect_uuid_columns(table, [])
        self.assertEqual(detected, [])
    
    def test_detect_uuid_columns_caches_result(self):
        """Test that detect_uuid_columns caches the result"""
        columns = [
            Column('explicit_uuid_col', 'VARCHAR', True, None, False)
        ]
        table = TableStructure('test_table', columns, None, [], [], [])
        
        detected = self.uuid_handler.detect_uuid_columns(table)
        self.assertEqual(self.uuid_handler.detected_uuid_columns['test_table'], set(['explicit_uuid_col']))
    
    def test_get_uuid_columns(self):
        """Test getting cached UUID columns"""
        # Initially empty
        self.assertEqual(self.uuid_handler.get_uuid_columns('nonexistent'), [])
        
        # Add some cached data
        self.uuid_handler.detected_uuid_columns['test_table'] = {'col1', 'col2'}
        result = self.uuid_handler.get_uuid_columns('test_table')
        self.assertEqual(set(result), {'col1', 'col2'})
    
    def test_normalize_row_for_comparison(self):
        """Test row normalization for comparison"""
        row = {
            'id': 'uuid-123',
            'name': 'John Doe',
            'email': 'john@example.com',
            'user_id': 'uuid-456'
        }
        
        uuid_columns = ['id', 'user_id']
        normalized = self.uuid_handler.normalize_row_for_comparison(row, uuid_columns)
        
        expected = {
            'name': 'John Doe',
            'email': 'john@example.com'
        }
        
        self.assertEqual(normalized, expected)
    
    def test_normalize_row_for_comparison_with_string_values(self):
        """Test row normalization with string trimming"""
        row = {
            'id': 'uuid-123',
            'name': '  John Doe  ',  # Has spaces to trim
            'email': 'john@example.com',
            'number': 42
        }
        
        uuid_columns = ['id']
        normalized = self.uuid_handler.normalize_row_for_comparison(row, uuid_columns)
        
        expected = {
            'name': 'John Doe',  # Trimmed
            'email': 'john@example.com',
            'number': 42
        }
        
        self.assertEqual(normalized, expected)
    
    def test_normalize_row_for_comparison_with_none_string(self):
        """Test row normalization with None string values"""
        row = {
            'id': 'uuid-123',
            'name': None,
            'email': ''
        }
        
        uuid_columns = ['id']
        normalized = self.uuid_handler.normalize_row_for_comparison(row, uuid_columns)
        
        expected = {
            'name': None,
            'email': ''
        }
        
        self.assertEqual(normalized, expected)
    
    def test_exclude_columns(self):
        """Test column exclusion functionality"""
        row = {
            'col1': 'value1',
            'col2': 'value2',
            'col3': 'value3'
        }
        
        excluded = self.uuid_handler.exclude_columns(row, ['col2'])
        expected = {
            'col1': 'value1',
            'col3': 'value3'
        }
        
        self.assertEqual(excluded, expected)
    
    def test_exclude_columns_empty_exclusion_list(self):
        """Test column exclusion with empty exclusion list"""
        row = {
            'col1': 'value1',
            'col2': 'value2'
        }
        
        excluded = self.uuid_handler.exclude_columns(row, [])
        self.assertEqual(excluded, row)
    
    def test_add_explicit_uuid_column(self):
        """Test adding explicit UUID column"""
        initial_count = len(self.uuid_handler.explicit_uuid_columns)
        self.uuid_handler.add_explicit_uuid_column('new_uuid_col')
        
        self.assertIn('new_uuid_col', self.uuid_handler.explicit_uuid_columns)
        self.assertEqual(len(self.uuid_handler.explicit_uuid_columns), initial_count + 1)
    
    def test_remove_explicit_uuid_column(self):
        """Test removing explicit UUID column"""
        # Add a column first
        self.uuid_handler.add_explicit_uuid_column('temp_col')
        self.assertIn('temp_col', self.uuid_handler.explicit_uuid_columns)
        
        # Remove it
        self.uuid_handler.remove_explicit_uuid_column('temp_col')
        self.assertNotIn('temp_col', self.uuid_handler.explicit_uuid_columns)
    
    def test_remove_explicit_uuid_column_nonexistent(self):
        """Test removing non-existent explicit UUID column"""
        initial_count = len(self.uuid_handler.explicit_uuid_columns)
        self.uuid_handler.remove_explicit_uuid_column('nonexistent')
        
        # Should not raise error and count should remain same
        self.assertEqual(len(self.uuid_handler.explicit_uuid_columns), initial_count)
    
    def test_add_custom_pattern_valid(self):
        """Test adding valid custom pattern"""
        initial_count = len(self.uuid_handler.custom_patterns)
        pattern = r'^test-\d{4}$'
        
        self.uuid_handler.add_custom_pattern(pattern)
        
        self.assertIn(pattern, self.uuid_handler.custom_patterns)
        self.assertEqual(len(self.uuid_handler.custom_patterns), initial_count + 1)
        self.assertIn(pattern, self.uuid_handler.all_patterns)
    
    def test_add_custom_pattern_invalid(self):
        """Test adding invalid custom pattern"""
        invalid_pattern = r'[invalid regex'
        
        with self.assertRaises(UUIDDetectionError) as cm:
            self.uuid_handler.add_custom_pattern(invalid_pattern)
        
        self.assertIn('Invalid regex pattern', str(cm.exception))
    
    def test_get_statistics(self):
        """Test getting UUID detection statistics"""
        # Add some test data
        self.uuid_handler.detected_uuid_columns = {
            'table1': {'col1', 'col2'},
            'table2': {'col3'},
            'table3': set()  # No UUID columns
        }
        self.uuid_handler.add_explicit_uuid_column('extra_col')
        self.uuid_handler.add_custom_pattern(r'^custom-\d+$')
        
        stats = self.uuid_handler.get_statistics()
        
        expected = {
            'total_tables_analyzed': 3,
            'total_uuid_columns_detected': 3,  # col1, col2, col3
            'explicit_uuid_columns': 2,  # explicit_uuid_col + extra_col
            'custom_patterns': 1,
            'tables_with_uuid_columns': 2  # table1 and table2 have UUID columns
        }
        
        self.assertEqual(stats, expected)
    
    def test_collect_uuid_statistics_empty_input(self):
        """Test collecting UUID statistics with empty input"""
        stats = self.uuid_handler.collect_uuid_statistics([], [])
        
        expected = {
            'total_uuid_values': 0,
            'unique_uuid_values': 0,
            'uuid_column_stats': {},
            'detected_patterns': {},
            'normalized_values': {}
        }
        
        self.assertEqual(stats, expected)
    
    def test_collect_uuid_statistics_no_uuid_columns(self):
        """Test collecting UUID statistics with no UUID columns"""
        table_data = [{'col1': 'value1'}, {'col1': 'value2'}]
        stats = self.uuid_handler.collect_uuid_statistics(table_data, [])
        
        expected = {
            'total_uuid_values': 0,
            'unique_uuid_values': 0,
            'uuid_column_stats': {},
            'detected_patterns': {},
            'normalized_values': {}
        }
        
        self.assertEqual(stats, expected)
    
    def test_collect_uuid_statistics_with_data(self):
        """Test collecting UUID statistics with real data"""
        table_data = [
            {'uuid_col': '123e4567-e89b-12d3-a456-426614174000', 'name': 'test1'},
            {'uuid_col': '223e4567-e89b-12d3-a456-426614174001', 'name': 'test2'},
            {'uuid_col': '123e4567-e89b-12d3-a456-426614174000', 'name': 'test3'},  # Duplicate UUID
            {'uuid_col': None, 'name': 'test4'}  # Null UUID
        ]
        uuid_columns = ['uuid_col']
        
        stats = self.uuid_handler.collect_uuid_statistics(table_data, uuid_columns)
        
        self.assertEqual(stats['total_uuid_values'], 3)  # 3 non-null values
        self.assertEqual(stats['unique_uuid_values'], 2)  # 2 unique values
        self.assertIn('uuid_col', stats['uuid_column_stats'])
        
        col_stats = stats['uuid_column_stats']['uuid_col']
        self.assertEqual(col_stats['total_values'], 3)
        self.assertEqual(col_stats['unique_values'], 2)
        self.assertEqual(col_stats['null_values'], 1)
        self.assertEqual(len(col_stats['sample_values']), 3)
    
    def test_collect_uuid_statistics_with_comparison_options(self):
        """Test collecting UUID statistics with comparison options"""
        # Mock comparison options
        comparison_options = Mock()
        comparison_options.unique_id_patterns = True
        comparison_options.unique_id_normalize_patterns = [
            {'pattern': r'-\d+$', 'replacement': '-XXX'}
        ]
        
        table_data = [
            {'uuid_col': 'report-123'},
            {'uuid_col': 'report-456'}
        ]
        uuid_columns = ['uuid_col']
        
        with patch.object(self.uuid_handler, '_detect_unique_id_pattern', return_value='prefix-number'):
            with patch.object(self.uuid_handler, '_normalize_unique_id', side_effect=['report-XXX', 'report-XXX']):
                stats = self.uuid_handler.collect_uuid_statistics(table_data, uuid_columns, comparison_options)
        
        self.assertIn('detected_patterns', stats)
        self.assertIn('normalized_values', stats)
        self.assertEqual(stats['detected_patterns']['uuid_col'], 'prefix-number')
    
    def test_detect_unique_id_pattern_empty_values(self):
        """Test pattern detection with empty values"""
        result = self.uuid_handler._detect_unique_id_pattern([])
        self.assertIsNone(result)
    
    def test_detect_unique_id_pattern_prefix_number(self):
        """Test pattern detection for prefix-number pattern"""
        values = ['report-123', 'document-456', 'file-789']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'prefix-number')
    
    def test_detect_unique_id_pattern_number_suffix(self):
        """Test pattern detection for number-suffix pattern"""
        values = ['123-report', '456-document', '789-file']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'number-suffix')
    
    def test_detect_unique_id_pattern_prefix_underscore_number(self):
        """Test pattern detection for prefix_number pattern"""
        values = ['report_123', 'document_456', 'file_789']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'prefix_number')
    
    def test_detect_unique_id_pattern_number_underscore_suffix(self):
        """Test pattern detection for number_suffix pattern"""
        values = ['123_report', '456_document', '789_file']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'number_suffix')
    
    def test_detect_unique_id_pattern_code_number(self):
        """Test pattern detection for code-number pattern"""
        values = ['ABC123456', 'DEFG789012', 'XYZ345678']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'code-number')
    
    def test_detect_unique_id_pattern_timestamp_serial(self):
        """Test pattern detection for timestamp-serial pattern"""
        values = ['20240101-1234', '20240102-5678', '20240103-9012']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'timestamp-serial')
    
    def test_detect_unique_id_pattern_custom(self):
        """Test pattern detection for unrecognized patterns"""
        values = ['custom_format_123', 'custom_format_456', 'custom_format_789']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'custom')
    
    def test_detect_unique_id_pattern_insufficient_match(self):
        """Test pattern detection with insufficient match ratio"""
        values = ['report-123', 'not-matching', 'also-different', 'completely-other']
        result = self.uuid_handler._detect_unique_id_pattern(values)
        self.assertEqual(result, 'custom')
    
    def test_normalize_unique_id_no_comparison_options(self):
        """Test unique ID normalization without comparison options"""
        comparison_options = Mock()
        delattr(comparison_options, 'unique_id_normalize_patterns')
        
        result = self.uuid_handler._normalize_unique_id('test-123', comparison_options)
        self.assertEqual(result, 'test-123')
    
    def test_normalize_unique_id_with_patterns(self):
        """Test unique ID normalization with patterns"""
        comparison_options = Mock()
        comparison_options.unique_id_normalize_patterns = [
            {'pattern': r'-\d+$', 'replacement': '-XXX'},
            {'pattern': r'^report', 'replacement': 'doc'}
        ]
        
        result = self.uuid_handler._normalize_unique_id('report-123', comparison_options)
        self.assertEqual(result, 'doc-XXX')
    
    def test_normalize_unique_id_with_invalid_pattern(self):
        """Test unique ID normalization with invalid regex pattern"""
        comparison_options = Mock()
        comparison_options.unique_id_normalize_patterns = [
            {'pattern': r'[invalid', 'replacement': 'XXX'},  # Invalid regex
            {'pattern': r'-\d+$', 'replacement': '-YYY'}     # Valid regex
        ]
        
        result = self.uuid_handler._normalize_unique_id('test-123', comparison_options)
        self.assertEqual(result, 'test-YYY')  # Only valid pattern applied
    
    def test_normalize_unique_id_missing_pattern_or_replacement(self):
        """Test unique ID normalization with missing pattern or replacement"""
        comparison_options = Mock()
        comparison_options.unique_id_normalize_patterns = [
            {'pattern': r'-\d+$'},  # Missing replacement
            {'replacement': '-XXX'},  # Missing pattern
            {'pattern': r'^test', 'replacement': 'demo'}  # Valid
        ]
        
        result = self.uuid_handler._normalize_unique_id('test-123', comparison_options)
        self.assertEqual(result, 'demo-123')  # Only valid pattern applied
    
    def test_compare_normalized_unique_ids_empty_input(self):
        """Test normalized unique ID comparison with empty input"""
        result = self.uuid_handler.compare_normalized_unique_ids([], [], [], None)
        expected = {'normalized_matches': 0, 'total_comparisons': 0}
        self.assertEqual(result, expected)
    
    def test_compare_normalized_unique_ids_no_uuid_columns(self):
        """Test normalized unique ID comparison with no UUID columns"""
        data1 = [{'col': 'val1'}]
        data2 = [{'col': 'val2'}]
        result = self.uuid_handler.compare_normalized_unique_ids(data1, data2, [], None)
        expected = {'normalized_matches': 0, 'total_comparisons': 0}
        self.assertEqual(result, expected)
    
    def test_compare_normalized_unique_ids_with_data(self):
        """Test normalized unique ID comparison with actual data"""
        data1 = [
            {'uuid_col': 'report-123'},
            {'uuid_col': 'report-456'}
        ]
        data2 = [
            {'uuid_col': 'doc-123'},
            {'uuid_col': 'doc-789'}
        ]
        uuid_columns = ['uuid_col']
        
        # Mock comparison options that normalize report->doc
        comparison_options = Mock()
        comparison_options.unique_id_patterns = True
        comparison_options.unique_id_normalize_patterns = [
            {'pattern': r'^report', 'replacement': 'doc'}
        ]
        
        with patch.object(self.uuid_handler, 'collect_uuid_statistics') as mock_collect:
            # Mock return values for collect_uuid_statistics
            mock_collect.side_effect = [
                {'normalized_values': {'uuid_col': ['doc-123', 'doc-456']}},
                {'normalized_values': {'uuid_col': ['doc-123', 'doc-789']}}
            ]
            
            result = self.uuid_handler.compare_normalized_unique_ids(data1, data2, uuid_columns, comparison_options)
        
        # Should find 1 match (doc-123) out of 2 total comparisons
        self.assertEqual(result['normalized_matches'], 1)
        self.assertEqual(result['total_comparisons'], 2)
        self.assertEqual(result['match_percentage'], 50.0)
    
    def test_compare_normalized_unique_ids_no_overlap(self):
        """Test normalized unique ID comparison with no overlap"""
        with patch.object(self.uuid_handler, 'collect_uuid_statistics') as mock_collect:
            mock_collect.side_effect = [
                {'normalized_values': {'uuid_col': ['doc-123', 'doc-456']}},
                {'normalized_values': {'uuid_col': ['doc-789', 'doc-012']}}
            ]
            
            result = self.uuid_handler.compare_normalized_unique_ids(
                [{'uuid_col': 'test'}], [{'uuid_col': 'test'}], ['uuid_col'], Mock()
            )
        
        self.assertEqual(result['normalized_matches'], 0)
        self.assertEqual(result['total_comparisons'], 2)
        self.assertEqual(result['match_percentage'], 0.0)
    
    def test_compare_normalized_unique_ids_zero_total_comparisons(self):
        """Test normalized unique ID comparison with zero total comparisons"""
        with patch.object(self.uuid_handler, 'collect_uuid_statistics') as mock_collect:
            mock_collect.side_effect = [
                {'normalized_values': {'uuid_col': []}},
                {'normalized_values': {'uuid_col': []}}
            ]
            
            result = self.uuid_handler.compare_normalized_unique_ids(
                [{'uuid_col': 'test'}], [{'uuid_col': 'test'}], ['uuid_col'], Mock()
            )
        
        self.assertEqual(result['normalized_matches'], 0)
        self.assertEqual(result['total_comparisons'], 0)
        self.assertEqual(result['match_percentage'], 0)


if __name__ == '__main__':
    unittest.main()
