"""
Comprehensive unit tests for the DataComparator class.
"""

import unittest
import tempfile
import os
import sqlite3
from unittest.mock import MagicMock, patch

from dbchecker.data_comparator import DataComparator
from dbchecker.uuid_handler import UUIDHandler
from dbchecker.database_connector import DatabaseConnector
from dbchecker.models import (
    ComparisonOptions, Column, TableStructure, PrimaryKey,
    FieldDifference, RowDifference, TableDataComparison, DataComparisonResult
)


class TestDataComparator(unittest.TestCase):
    """Test cases for DataComparator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.uuid_handler = UUIDHandler()
        self.options = ComparisonOptions()
        self.data_comparator = DataComparator(self.uuid_handler, self.options)
        
        # Create test table structure
        self.test_columns = [
            Column("id", "INTEGER", False, None, True),
            Column("name", "TEXT", False, None, False),
            Column("email", "TEXT", True, None, False),
            Column("created_at", "DATETIME", True, None, False)
        ]
        
        self.test_table_structure = TableStructure(
            name="users",
            columns=self.test_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        # Set up temporary databases
        self.temp_dir = tempfile.mkdtemp()
        self.db1_path = os.path.join(self.temp_dir, "db1.sqlite")
        self.db2_path = os.path.join(self.temp_dir, "db2.sqlite")
        
    def tearDown(self):
        """Clean up test fixtures"""
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))
        os.rmdir(self.temp_dir)
    
    def _create_test_database(self, db_path, data_set=1):
        """Create a test database with sample data"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                created_at DATETIME,
                user_uuid TEXT
            )
        ''')
        
        if data_set == 1:
            cursor.execute("INSERT INTO users VALUES (1, 'John', 'john@test.com', '2024-01-01', 'uuid-1')")
            cursor.execute("INSERT INTO users VALUES (2, 'Jane', 'jane@test.com', '2024-01-02', 'uuid-2')")
        else:
            cursor.execute("INSERT INTO users VALUES (1, 'John Updated', 'john@test.com', '2024-01-01', 'uuid-1')")
            cursor.execute("INSERT INTO users VALUES (2, 'Jane', 'jane@test.com', '2024-01-02', 'uuid-2')")
            cursor.execute("INSERT INTO users VALUES (3, 'Bob', 'bob@test.com', '2024-01-03', 'uuid-3')")
        
        conn.commit()
        conn.close()
    
    def test_identify_differences_no_differences(self):
        """Test identifying differences when rows are identical"""
        row1 = {"id": 1, "name": "John", "email": "john@test.com"}
        row2 = {"id": 1, "name": "John", "email": "john@test.com"}
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        self.assertEqual(len(differences), 0)
    
    def test_identify_differences_with_differences(self):
        """Test identifying differences when rows differ"""
        row1 = {"id": 1, "name": "John", "email": "john@test.com"}
        row2 = {"id": 1, "name": "Jane", "email": "jane@test.com"}
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        self.assertEqual(len(differences), 2)
        
        # Check name difference
        name_diff = next((d for d in differences if d.field_name == "name"), None)
        self.assertIsNotNone(name_diff)
        if name_diff:
            self.assertEqual(name_diff.value_db1, "John")
            self.assertEqual(name_diff.value_db2, "Jane")
        
        # Check email difference
        email_diff = next((d for d in differences if d.field_name == "email"), None)
        self.assertIsNotNone(email_diff)
        if email_diff:
            self.assertEqual(email_diff.value_db1, "john@test.com")
            self.assertEqual(email_diff.value_db2, "jane@test.com")
    
    def test_identify_differences_with_exclusions(self):
        """Test identifying differences with excluded columns"""
        row1 = {"id": 1, "name": "John", "email": "john@test.com", "created_at": "2024-01-01"}
        row2 = {"id": 1, "name": "Jane", "email": "john@test.com", "created_at": "2024-01-02"}
        
        # Exclude 'name' and 'created_at' columns
        exclude_columns = ["name", "created_at"]
        differences = self.data_comparator.identify_differences(row1, row2, exclude_columns)
        
        # Should find no differences since different fields are excluded
        self.assertEqual(len(differences), 0)
    
    def test_identify_differences_null_values(self):
        """Test identifying differences with NULL values"""
        row1 = {"id": 1, "name": "John", "email": None}
        row2 = {"id": 1, "name": "John", "email": "john@test.com"}
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        self.assertEqual(len(differences), 1)
        email_diff = differences[0]
        self.assertEqual(email_diff.field_name, "email")
        self.assertIsNone(email_diff.value_db1)
        self.assertEqual(email_diff.value_db2, "john@test.com")
    
    def test_identify_differences_different_types(self):
        """Test identifying differences with different data types"""
        row1 = {"id": 1, "age": 25}
        row2 = {"id": 1, "age": "25"}  # String vs integer
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        # Should detect type difference
        self.assertEqual(len(differences), 1)
        age_diff = differences[0]
        self.assertEqual(age_diff.field_name, "age")
        self.assertEqual(age_diff.value_db1, 25)
        self.assertEqual(age_diff.value_db2, "25")
    
    def test_create_row_signature_basic(self):
        """Test creating row signature for basic row"""
        row = {"id": 1, "name": "John", "email": "john@test.com"}
        exclude_columns = ["id"]
        
        signature = self.data_comparator.get_row_hash(row, exclude_columns)
        
        self.assertIsInstance(signature, str)
        # Hash should be consistent
        signature2 = self.data_comparator.get_row_hash(row, exclude_columns)
        self.assertEqual(signature, signature2)
    
    def test_create_row_signature_with_nulls(self):
        """Test creating row signature with NULL values"""
        row = {"id": 1, "name": "John", "email": None}
        exclude_columns = ["id"]
        
        signature = self.data_comparator.get_row_hash(row, exclude_columns)
        
        self.assertIsInstance(signature, str)
        # Should handle NULL values gracefully
    
    def test_create_row_signature_empty_row(self):
        """Test creating row signature for empty row"""
        row = {}
        exclude_columns = []
        
        signature = self.data_comparator.get_row_hash(row, exclude_columns)
        
        self.assertIsInstance(signature, str)
    
    def test_get_excluded_columns_info(self):
        """Test getting excluded columns information"""
        # Mock sample data
        sample_data = [
            {"id": 1, "name": "John", "created_at": "2024-01-01", "user_uuid": "uuid-1"},
            {"id": 2, "name": "Jane", "created_at": "2024-01-02", "user_uuid": "uuid-2"}
        ]
        
        excluded_info = self.data_comparator.get_excluded_columns_info(
            self.test_table_structure, sample_data
        )
        
        self.assertIn('uuid_columns', excluded_info)
        self.assertIn('timestamp_columns', excluded_info)
        self.assertIn('metadata_columns', excluded_info)
        self.assertIn('all_excluded', excluded_info)
        
        # Should detect timestamp column
        self.assertIn('created_at', excluded_info['timestamp_columns'])
    
    def test_get_excluded_columns_info_no_sample_data(self):
        """Test getting excluded columns information without sample data"""
        excluded_info = self.data_comparator.get_excluded_columns_info(
            self.test_table_structure, None
        )
        
        self.assertIn('uuid_columns', excluded_info)
        self.assertIn('timestamp_columns', excluded_info)
        self.assertIn('all_excluded', excluded_info)
    
    def test_compare_table_data_identical(self):
        """Test comparing identical table data"""
        # Create identical databases
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=1)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        result = self.data_comparator.compare_table_data(
            "users", conn1, conn2
        )
        
        self.assertIsInstance(result, TableDataComparison)
        self.assertEqual(result.table_name, "users")
        self.assertEqual(result.row_count_db1, 2)
        self.assertEqual(result.row_count_db2, 2)
        self.assertEqual(result.matching_rows, 2)
        self.assertEqual(len(result.rows_only_in_db1), 0)
        self.assertEqual(len(result.rows_only_in_db2), 0)
        self.assertEqual(len(result.rows_with_differences), 0)
    
    def test_compare_table_data_with_differences(self):
        """Test comparing table data with differences"""
        # Create databases with differences
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=2)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        result = self.data_comparator.compare_table_data(
            "users", conn1, conn2
        )
        
        self.assertEqual(result.row_count_db1, 2)
        self.assertEqual(result.row_count_db2, 3)
        self.assertEqual(len(result.rows_only_in_db2), 1)  # Bob is only in DB2
        self.assertEqual(len(result.rows_with_differences), 1)  # John was updated
    
    def test_compare_table_data_with_timestamp_exclusion(self):
        """Test comparing table data with timestamp exclusion"""
        # Set options to exclude timestamps
        options = ComparisonOptions(auto_detect_timestamps=True)
        data_comparator = DataComparator(self.uuid_handler, options)
        
        # Create databases with only timestamp differences
        conn = sqlite3.connect(self.db1_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                created_at DATETIME
            )
        ''')
        cursor.execute("INSERT INTO events VALUES (1, 'Event1', '2024-01-01 10:00:00')")
        conn.commit()
        conn.close()
        
        conn = sqlite3.connect(self.db2_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                created_at DATETIME
            )
        ''')
        cursor.execute("INSERT INTO events VALUES (1, 'Event1', '2024-01-01 12:00:00')")  # Different time
        conn.commit()
        conn.close()
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        table_structure = TableStructure(
            name="events",
            columns=[
                Column("id", "INTEGER", False, None, True),
                Column("name", "TEXT", False, None, False),
                Column("created_at", "DATETIME", True, None, False)
            ],
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[], unique_constraints=[], check_constraints=[]
        )
        
        result = data_comparator.compare_table_data(
            "events", conn1, conn2
        )
        
        # Should find no differences since timestamps are excluded
        self.assertEqual(len(result.rows_with_differences), 0)
        self.assertEqual(result.matching_rows, 1)
    
    def test_compare_all_tables(self):
        """Test comparing all tables in databases"""
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=2)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        table_names = ["users"]
        result = self.data_comparator.compare_all_tables(conn1, conn2, table_names)
        
        self.assertIsInstance(result, DataComparisonResult)
        self.assertEqual(len(result.table_results), 1)
        self.assertIn("users", result.table_results)
        
        users_result = result.table_results["users"]
        self.assertEqual(users_result.table_name, "users")
    
    def test_compare_with_batch_size(self):
        """Test comparing data with different batch sizes"""
        # Create larger dataset
        conn = sqlite3.connect(self.db1_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE large_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        # Insert 100 rows
        for i in range(100):
            cursor.execute("INSERT INTO large_table VALUES (?, ?, ?)", 
                          (i, f"name_{i}", i * 10))
        conn.commit()
        conn.close()
        
        # Create identical second database
        conn = sqlite3.connect(self.db2_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE large_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        for i in range(100):
            cursor.execute("INSERT INTO large_table VALUES (?, ?, ?)", 
                          (i, f"name_{i}", i * 10))
        conn.commit()
        conn.close()
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        table_structure = TableStructure(
            name="large_table",
            columns=[
                Column("id", "INTEGER", False, None, True),
                Column("name", "TEXT", False, None, False),
                Column("value", "INTEGER", False, None, False)
            ],
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[], unique_constraints=[], check_constraints=[]
        )
        
        # Test with small batch size
        result = self.data_comparator.compare_table_data(
            "large_table", conn1, conn2, batch_size=10
        )
        
        self.assertEqual(result.row_count_db1, 100)
        self.assertEqual(result.row_count_db2, 100)
        self.assertEqual(result.matching_rows, 100)
    
    def test_compare_with_uuid_tracking_mode(self):
        """Test comparing data with UUID tracking mode"""
        options = ComparisonOptions(
            uuid_comparison_mode='include_with_tracking',
            auto_detect_uuids=True
        )
        data_comparator = DataComparator(self.uuid_handler, options)
        
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=1)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        result = data_comparator.compare_table_data(
            "users", conn1, conn2
        )
        
        # Should include UUID statistics
        self.assertIsNotNone(result.uuid_statistics)
    
    def test_compare_empty_tables(self):
        """Test comparing empty tables"""
        # Create empty databases
        conn = sqlite3.connect(self.db1_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE empty_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
        conn = sqlite3.connect(self.db2_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE empty_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        table_structure = TableStructure(
            name="empty_table",
            columns=[
                Column("id", "INTEGER", False, None, True),
                Column("name", "TEXT", False, None, False)
            ],
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[], unique_constraints=[], check_constraints=[]
        )
        
        result = self.data_comparator.compare_table_data(
            "empty_table", conn1, conn2
        )
        
        self.assertEqual(result.row_count_db1, 0)
        self.assertEqual(result.row_count_db2, 0)
        self.assertEqual(result.matching_rows, 0)
        self.assertEqual(len(result.rows_only_in_db1), 0)
        self.assertEqual(len(result.rows_only_in_db2), 0)
        self.assertEqual(len(result.rows_with_differences), 0)
    
    def test_compare_with_special_characters(self):
        """Test comparing data with special characters"""
        # Create databases with special character data
        conn = sqlite3.connect(self.db1_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE special_chars (
                id INTEGER PRIMARY KEY,
                text_field TEXT
            )
        ''')
        cursor.execute("INSERT INTO special_chars VALUES (1, ?)", 
                      ("Test with quotes: \"hello\" and 'world'",))
        cursor.execute("INSERT INTO special_chars VALUES (2, ?)", 
                      ("Unicode: café naïve 北京",))
        cursor.execute("INSERT INTO special_chars VALUES (3, ?)", 
                      ("Newlines:\nLine1\nLine2",))
        conn.commit()
        conn.close()
        
        # Create identical second database
        conn = sqlite3.connect(self.db2_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE special_chars (
                id INTEGER PRIMARY KEY,
                text_field TEXT
            )
        ''')
        cursor.execute("INSERT INTO special_chars VALUES (1, ?)", 
                      ("Test with quotes: \"hello\" and 'world'",))
        cursor.execute("INSERT INTO special_chars VALUES (2, ?)", 
                      ("Unicode: café naïve 北京",))
        cursor.execute("INSERT INTO special_chars VALUES (3, ?)", 
                      ("Newlines:\nLine1\nLine2",))
        conn.commit()
        conn.close()
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        table_structure = TableStructure(
            name="special_chars",
            columns=[
                Column("id", "INTEGER", False, None, True),
                Column("text_field", "TEXT", False, None, False)
            ],
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[], unique_constraints=[], check_constraints=[]
        )
        
        result = self.data_comparator.compare_table_data(
            "special_chars", conn1, conn2
        )
        
        # Should handle special characters correctly
        self.assertEqual(result.matching_rows, 3)
        self.assertEqual(len(result.rows_with_differences), 0)


if __name__ == '__main__':
    unittest.main()
