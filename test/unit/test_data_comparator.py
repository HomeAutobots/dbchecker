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
    
    def test_compare_with_uuid_tracking_mode_different_uuids(self):
        """Test comparing data with UUID tracking mode and different UUIDs"""
        options = ComparisonOptions(
            uuid_comparison_mode='include_with_tracking',
            auto_detect_uuids=True
        )
        data_comparator = DataComparator(self.uuid_handler, options)
        
        # Create databases with different UUIDs
        conn = sqlite3.connect(self.db1_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                user_uuid TEXT
            )
        ''')
        cursor.execute("INSERT INTO users VALUES (1, 'John', 'uuid-1')")
        conn.commit()
        conn.close()
        
        conn = sqlite3.connect(self.db2_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                user_uuid TEXT
            )
        ''')
        cursor.execute("INSERT INTO users VALUES (1, 'John', 'uuid-different')")  # Different UUID
        conn.commit()
        conn.close()
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        result = data_comparator.compare_table_data(
            "users", conn1, conn2
        )
        
        # Should include UUID statistics with differences counted
        self.assertIsNotNone(result.uuid_statistics)
        if result.uuid_statistics:
            self.assertGreater(result.uuid_statistics.uuid_value_differences, 0)
    
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
    
    def test_get_excluded_columns_info_include_normal_mode(self):
        """Test getting excluded columns with include_normal UUID mode"""
        options = ComparisonOptions(uuid_comparison_mode='include_normal')
        data_comparator = DataComparator(self.uuid_handler, options)
        
        sample_data = [
            {"id": 1, "name": "John", "user_uuid": "uuid-1"},
        ]
        
        excluded_info = data_comparator.get_excluded_columns_info(
            self.test_table_structure, sample_data
        )
        
        self.assertIn('uuid_columns', excluded_info)
        self.assertIn('uuid_included_normal', excluded_info)
        # UUIDs should not be in all_excluded for include_normal mode
        self.assertNotIn('user_uuid', excluded_info['all_excluded'])
    
    def test_get_excluded_columns_info_include_with_tracking_mode(self):
        """Test getting excluded columns with include_with_tracking UUID mode"""
        options = ComparisonOptions(uuid_comparison_mode='include_with_tracking')
        data_comparator = DataComparator(self.uuid_handler, options)
        
        sample_data = [
            {"id": 1, "name": "John", "user_uuid": "uuid-1"},
        ]
        
        excluded_info = data_comparator.get_excluded_columns_info(
            self.test_table_structure, sample_data
        )
        
        self.assertIn('uuid_columns', excluded_info)
        self.assertIn('uuid_included_for_tracking', excluded_info)
        # UUIDs should not be in all_excluded for include_with_tracking mode
        self.assertNotIn('user_uuid', excluded_info['all_excluded'])
    
    def test_compare_all_tables_with_error(self):
        """Test compare_all_tables when an error occurs"""
        from dbchecker.exceptions import DataComparisonError
        
        # Create mock connections that will raise an error
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        
        # Make get_table_structure raise an exception
        mock_conn1.get_table_structure.side_effect = Exception("Database error")
        
        with self.assertRaises(DataComparisonError) as cm:
            self.data_comparator.compare_all_tables(mock_conn1, mock_conn2, ["test_table"])
        
        self.assertIn("Failed to compare table test_table", str(cm.exception))
    
    def test_values_equal_string_comparison(self):
        """Test _values_equal method with string comparison"""
        # Test string trimming
        self.assertTrue(self.data_comparator._values_equal("  hello  ", "hello"))
        self.assertTrue(self.data_comparator._values_equal("hello", "hello"))
        self.assertFalse(self.data_comparator._values_equal("hello", "world"))
    
    def test_values_equal_numeric_comparison(self):
        """Test _values_equal method with numeric comparison"""
        # Test integer comparison
        self.assertTrue(self.data_comparator._values_equal(42, 42))
        self.assertFalse(self.data_comparator._values_equal(42, 43))
        
        # Test float comparison with precision (tolerance is 1e-10)
        self.assertTrue(self.data_comparator._values_equal(1.0, 1.0 + 5e-11))  # Within tolerance
        self.assertFalse(self.data_comparator._values_equal(1.0, 2.0))
        
        # Test mixed int/float comparison
        self.assertTrue(self.data_comparator._values_equal(42, 42.0))
    
    def test_values_equal_none_comparison(self):
        """Test _values_equal method with None values"""
        self.assertTrue(self.data_comparator._values_equal(None, None))
        self.assertFalse(self.data_comparator._values_equal(None, "hello"))
        self.assertFalse(self.data_comparator._values_equal("hello", None))
    
    def test_values_equal_default_comparison(self):
        """Test _values_equal method with other types"""
        # Test boolean comparison
        self.assertTrue(self.data_comparator._values_equal(True, True))
        self.assertFalse(self.data_comparator._values_equal(True, False))
        
        # Test list comparison
        self.assertTrue(self.data_comparator._values_equal([1, 2, 3], [1, 2, 3]))
        self.assertFalse(self.data_comparator._values_equal([1, 2, 3], [1, 2, 4]))
    
    def test_create_row_identifier(self):
        """Test _create_row_identifier method"""
        row = {"id": 1, "name": "John", "email": "john@test.com", "created_at": "2024-01-01"}
        exclude_columns = ["created_at"]
        
        identifier = self.data_comparator._create_row_identifier(row, exclude_columns)
        
        self.assertIsInstance(identifier, str)
        self.assertIn("id=1", identifier)
        self.assertIn("name=John", identifier)
        self.assertIn("email=john@test.com", identifier)
        self.assertNotIn("created_at", identifier)
    
    def test_create_row_identifier_all_excluded(self):
        """Test _create_row_identifier when all columns are excluded"""
        row = {"id": 1, "name": "John"}
        exclude_columns = ["id", "name"]
        
        identifier = self.data_comparator._create_row_identifier(row, exclude_columns)
        
        self.assertIsInstance(identifier, str)
        self.assertTrue(identifier.startswith("row_"))
    
    def test_create_row_identifier_empty_row(self):
        """Test _create_row_identifier with empty row"""
        row = {}
        exclude_columns = []
        
        identifier = self.data_comparator._create_row_identifier(row, exclude_columns)
        
        self.assertIsInstance(identifier, str)
        self.assertTrue(identifier.startswith("row_"))
    
    def test_get_row_hash_with_primary_key(self):
        """Test get_row_hash using primary key field"""
        row = {"id": 1, "name": "John", "email": "john@test.com"}
        exclude_columns = []
        
        row_hash = self.data_comparator.get_row_hash(row, exclude_columns)
        
        # Should use 'id' field for hashing since it's a primary key pattern
        self.assertIsInstance(row_hash, str)
        
        # Same row should produce same hash
        row_hash2 = self.data_comparator.get_row_hash(row, exclude_columns)
        self.assertEqual(row_hash, row_hash2)
    
    def test_get_row_hash_with_id_suffix(self):
        """Test get_row_hash using field ending with '_id'"""
        row = {"user_id": 123, "name": "John", "email": "john@test.com"}
        exclude_columns = []
        
        row_hash = self.data_comparator.get_row_hash(row, exclude_columns)
        
        # Should use 'user_id' field for hashing
        self.assertIsInstance(row_hash, str)
    
    def test_get_row_hash_fallback_to_all_fields(self):
        """Test get_row_hash falling back to all fields when no key field found"""
        row = {"name": "John", "email": "john@test.com"}
        exclude_columns = []
        
        row_hash = self.data_comparator.get_row_hash(row, exclude_columns)
        
        # Should use all fields for hashing
        self.assertIsInstance(row_hash, str)
    
    def test_get_row_hash_with_excluded_id_field(self):
        """Test get_row_hash when ID field is excluded"""
        row = {"user_id": 123, "name": "John", "email": "john@test.com"}
        exclude_columns = ["user_id"]
        
        row_hash = self.data_comparator.get_row_hash(row, exclude_columns)
        
        # Should fall back to all non-excluded fields
        self.assertIsInstance(row_hash, str)
    
    def test_find_matching_rows_with_duplicates(self):
        """Test find_matching_rows with duplicate rows"""
        rows1 = [
            {"id": 1, "name": "John"},
            {"id": 1, "name": "John"},  # Duplicate
            {"id": 2, "name": "Jane"}
        ]
        rows2 = [
            {"id": 1, "name": "John"},
            {"id": 1, "name": "John"},  # Duplicate
            {"id": 1, "name": "John"},  # Extra duplicate
            {"id": 3, "name": "Bob"}
        ]
        
        result = self.data_comparator.find_matching_rows(rows1, rows2, [])
        
        # Should match duplicates properly
        self.assertEqual(len(result['matched_pairs']), 2)  # Two John pairs matched
        self.assertEqual(len(result['only_in_db1']), 1)    # Jane only in db1
        self.assertEqual(len(result['only_in_db2']), 2)    # Extra John and Bob in db2
    
    def test_find_matching_rows_more_duplicates_in_db1(self):
        """Test find_matching_rows with more duplicates in db1"""
        rows1 = [
            {"id": 1, "name": "John"},
            {"id": 1, "name": "John"},  # Duplicate
            {"id": 1, "name": "John"},  # Extra duplicate
        ]
        rows2 = [
            {"id": 1, "name": "John"},
            {"id": 1, "name": "John"},  # Duplicate
        ]
        
        result = self.data_comparator.find_matching_rows(rows1, rows2, [])
        
        # Should match 2 pairs and leave 1 extra in db1
        self.assertEqual(len(result['matched_pairs']), 2)
        self.assertEqual(len(result['only_in_db1']), 1)
        self.assertEqual(len(result['only_in_db2']), 0)
    
    def test_identify_differences_column_only_in_row1(self):
        """Test identify_differences when column exists only in row1"""
        row1 = {"id": 1, "name": "John", "extra_field": "value"}
        row2 = {"id": 1, "name": "John"}
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        self.assertEqual(len(differences), 1)
        diff = differences[0]
        self.assertEqual(diff.field_name, "extra_field")
        self.assertEqual(diff.value_db1, "value")
        self.assertIsNone(diff.value_db2)
    
    def test_identify_differences_column_only_in_row2(self):
        """Test identify_differences when column exists only in row2"""
        row1 = {"id": 1, "name": "John"}
        row2 = {"id": 1, "name": "John", "extra_field": "value"}
        
        differences = self.data_comparator.identify_differences(row1, row2, [])
        
        self.assertEqual(len(differences), 1)
        diff = differences[0]
        self.assertEqual(diff.field_name, "extra_field")
        self.assertIsNone(diff.value_db1)
        self.assertEqual(diff.value_db2, "value")
    
    def test_get_statistics(self):
        """Test get_statistics method"""
        # Create mock comparison result
        table_results = {
            "users": TableDataComparison(
                table_name="users",
                row_count_db1=10,
                row_count_db2=12,
                matching_rows=8,
                rows_only_in_db1=[{"id": 1}],
                rows_only_in_db2=[{"id": 2}, {"id": 3}],
                rows_with_differences=[
                    RowDifference("row1", [FieldDifference("name", "John", "Jane")])
                ],
                uuid_statistics=None
            ),
            "products": TableDataComparison(
                table_name="products",
                row_count_db1=5,
                row_count_db2=5,
                matching_rows=5,
                rows_only_in_db1=[],
                rows_only_in_db2=[],
                rows_with_differences=[],
                uuid_statistics=None
            )
        }
        
        comparison_result = DataComparisonResult(
            table_results=table_results,
            total_differences=4  # 1 only_in_db1 + 2 only_in_db2 + 1 differences
        )
        
        stats = self.data_comparator.get_statistics(comparison_result)
        
        self.assertEqual(stats['total_tables'], 2)
        self.assertEqual(stats['tables_with_differences'], 1)  # Only users table has differences
        self.assertEqual(stats['tables_identical'], 1)  # Products table is identical
        self.assertEqual(stats['total_rows_compared'], 32)  # 10+12+5+5
        self.assertEqual(stats['total_differences'], 4)
    
    def test_options_verbose_mode(self):
        """Test data comparator with verbose mode enabled"""
        options = ComparisonOptions(verbose=True)
        data_comparator = DataComparator(self.uuid_handler, options)
        
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=1)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        # Capture print output
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            result = data_comparator.compare_table_data("users", conn1, conn2)
        finally:
            sys.stdout = sys.__stdout__
        
        output = captured_output.getvalue()
        self.assertIn("Table users:", output)
    
    def test_options_verbose_mode_with_uuid_tracking(self):
        """Test data comparator with verbose mode and UUID tracking enabled"""
        options = ComparisonOptions(
            verbose=True,
            uuid_comparison_mode='include_with_tracking',
            auto_detect_uuids=True
        )
        data_comparator = DataComparator(self.uuid_handler, options)
        
        self._create_test_database(self.db1_path, data_set=1)
        self._create_test_database(self.db2_path, data_set=1)
        
        conn1 = DatabaseConnector(self.db1_path)
        conn2 = DatabaseConnector(self.db2_path)
        
        # Capture print output
        import io
        import sys
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            result = data_comparator.compare_table_data("users", conn1, conn2)
        finally:
            sys.stdout = sys.__stdout__
        
        output = captured_output.getvalue()
        self.assertIn("Table users:", output)
        if result.uuid_statistics and result.uuid_statistics.uuid_columns:
            self.assertIn("UUID tracking enabled", output)


if __name__ == '__main__':
    unittest.main()
