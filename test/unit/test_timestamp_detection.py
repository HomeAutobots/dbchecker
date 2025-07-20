"""
Unit tests for timestamp column detection and exclusion functionality.
"""

import unittest
import tempfile
import os
import sqlite3
from dbchecker.data_comparator import DataComparator
from dbchecker.uuid_handler import UUIDHandler
from dbchecker.database_connector import DatabaseConnector
from dbchecker.models import Column, TableStructure


class TestTimestampDetection(unittest.TestCase):
    """Test cases for timestamp column detection and exclusion"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.uuid_handler = UUIDHandler()
        self.data_comparator = DataComparator(self.uuid_handler)
    
    def test_detect_timestamp_columns_by_type(self):
        """Test detection of timestamp columns by data type"""
        # Create a table structure with various timestamp types
        columns = [
            Column("id", "INTEGER", False, None, True),
            Column("name", "TEXT", False, None, False),
            Column("created_at", "DATETIME", True, None, False),
            Column("updated_time", "TIMESTAMP", True, None, False),
            Column("birth_date", "DATE", True, None, False),
            Column("login_time", "TIME", True, None, False),
            Column("description", "TEXT", True, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        timestamp_columns = self.data_comparator._detect_timestamp_columns(table_structure)
        
        expected_columns = ["created_at", "updated_time", "birth_date", "login_time"]
        self.assertEqual(set(timestamp_columns), set(expected_columns))
    
    def test_detect_timestamp_columns_by_name_pattern(self):
        """Test detection of timestamp columns by naming patterns"""
        columns = [
            Column("id", "INTEGER", False, None, True),
            Column("username", "TEXT", False, None, False),
            Column("created_at", "TEXT", True, None, False),  # Pattern: *_at
            Column("updated_timestamp", "TEXT", True, None, False),  # Pattern: *timestamp*
            Column("last_modified", "TEXT", True, None, False),  # Pattern: *modified*
            Column("deleted_time", "TEXT", True, None, False),  # Pattern: *_time
            Column("birth_date", "TEXT", True, None, False),  # Pattern: *_date
            Column("is_active", "INTEGER", False, None, False),  # Should not match
            Column("created_by", "TEXT", True, None, False),  # Pattern: *created* but not timestamp
            Column("regular_field", "TEXT", True, None, False)  # Should not match
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        timestamp_columns = self.data_comparator._detect_timestamp_columns(table_structure)
        
        expected_columns = ["created_at", "updated_timestamp", "last_modified", "deleted_time", "birth_date", "created_by"]
        self.assertEqual(set(timestamp_columns), set(expected_columns))
    
    def test_get_excluded_columns_info(self):
        """Test getting information about excluded columns"""
        columns = [
            Column("id", "INTEGER", False, None, True),  # Will be detected as UUID
            Column("username", "TEXT", False, None, False),
            Column("created_at", "DATETIME", True, None, False),  # Timestamp
            Column("user_id", "TEXT", True, None, False),  # Will be detected as UUID
            Column("email", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        excluded_info = self.data_comparator.get_excluded_columns_info(table_structure)
        
        # Check that we get the expected structure
        self.assertIn('uuid_columns', excluded_info)
        self.assertIn('timestamp_columns', excluded_info)
        self.assertIn('all_excluded', excluded_info)
        
        # Check timestamp detection
        self.assertIn('created_at', excluded_info['timestamp_columns'])
        
        # Check UUID detection  
        self.assertIn('id', excluded_info['uuid_columns'])
        self.assertIn('user_id', excluded_info['uuid_columns'])
        
        # Check combined exclusions
        expected_excluded = set(excluded_info['uuid_columns'] + excluded_info['timestamp_columns'])
        self.assertEqual(set(excluded_info['all_excluded']), expected_excluded)
    
    def test_timestamp_exclusion_in_comparison(self):
        """Test that timestamp columns are actually excluded during data comparison"""
        # This would be a more complex integration test
        # For now, we'll just verify the exclusion logic works
        
        row1 = {
            'id': 1,
            'name': 'John',
            'created_at': '2024-01-01 10:00:00',
            'updated_at': '2024-01-01 10:00:00'
        }
        
        row2 = {
            'id': 1,
            'name': 'John', 
            'created_at': '2024-01-01 12:00:00',  # Different timestamp
            'updated_at': '2024-01-01 12:00:00'   # Different timestamp
        }
        
        # Exclude timestamp columns
        exclude_columns = ['id', 'created_at', 'updated_at']
        
        differences = self.data_comparator.identify_differences(row1, row2, exclude_columns)
        
        # Should find no differences since timestamps are excluded
        self.assertEqual(len(differences), 0)
        
        # Test with non-timestamp difference
        row2['name'] = 'Jane'
        differences = self.data_comparator.identify_differences(row1, row2, exclude_columns)
        
        # Should find 1 difference (name field)
        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0].field_name, 'name')


if __name__ == '__main__':
    unittest.main()
