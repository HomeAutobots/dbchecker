"""
Test cases for the UUID handler module.
"""

import unittest
from dbchecker.uuid_handler import UUIDHandler
from dbchecker.models import TableStructure, Column


class TestUUIDHandler(unittest.TestCase):
    """Test cases for UUIDHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.uuid_handler = UUIDHandler(['explicit_uuid_col'])
    
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
    
    def test_is_uuid_column_explicit(self):
        """Test explicit UUID column detection"""
        self.assertTrue(self.uuid_handler.is_uuid_column('explicit_uuid_col'))
        self.assertFalse(self.uuid_handler.is_uuid_column('regular_column'))
    
    def test_is_uuid_column_pattern_matching(self):
        """Test UUID column detection by pattern"""
        uuid_columns = [
            'id',
            'user_id',
            'entity_uuid',
            'guid_field',
            'record_guid'
        ]
        
        non_uuid_columns = [
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


if __name__ == '__main__':
    unittest.main()
