"""
Comprehensive unit tests for the DatabaseConnector class.
"""

import unittest
import tempfile
import os
import sqlite3
from unittest.mock import patch, MagicMock

from dbchecker.database_connector import DatabaseConnector
from dbchecker.models import Column, TableStructure, DatabaseSchema
from dbchecker.exceptions import DatabaseConnectionError, SchemaExtractionError


class TestDatabaseConnector(unittest.TestCase):
    """Test cases for DatabaseConnector class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self._create_test_database()
    
    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)
    
    def _create_test_database(self):
        """Create a test database with various table structures"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table with various column types and constraints
        cursor.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT,
                age INTEGER DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT check_age CHECK (age >= 0)
            )
        ''')
        
        # Create table with foreign key
        cursor.execute('''
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ''')
        
        # Create index
        cursor.execute('CREATE INDEX idx_users_username ON users(username)')
        cursor.execute('CREATE UNIQUE INDEX idx_posts_title ON posts(title)')
        
        # Create view
        cursor.execute('''
            CREATE VIEW active_users AS 
            SELECT id, username, email FROM users WHERE is_active = 1
        ''')
        
        # Create trigger
        cursor.execute('''
            CREATE TRIGGER update_timestamp 
            AFTER UPDATE ON users
            BEGIN
                UPDATE users SET created_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
            END
        ''')
        
        # Insert test data
        cursor.execute("INSERT INTO users (username, email, age) VALUES ('john', 'john@test.com', 25)")
        cursor.execute("INSERT INTO users (username, email, age) VALUES ('jane', 'jane@test.com', 30)")
        cursor.execute("INSERT INTO posts (title, content, user_id) VALUES ('Test Post', 'Content', 1)")
        
        conn.commit()
        conn.close()
    
    def test_init_with_valid_path(self):
        """Test initialization with valid database path"""
        connector = DatabaseConnector(self.db_path)
        self.assertEqual(connector.db_path, self.db_path)
        self.assertIsNotNone(connector.connection)
    
    def test_init_with_invalid_path(self):
        """Test initialization with invalid database path"""
        invalid_path = "/nonexistent/path/db.sqlite"
        with self.assertRaises(DatabaseConnectionError):
            DatabaseConnector(invalid_path)
    
    def test_close_connection(self):
        """Test closing database connection"""
        connector = DatabaseConnector(self.db_path)
        self.assertIsNotNone(connector.connection)
        connector.close()
        self.assertIsNone(connector.connection)
    
    def test_execute_query(self):
        """Test executing SQL queries"""
        connector = DatabaseConnector(self.db_path)
        results = connector.execute_query("SELECT COUNT(*) as count FROM users")
        self.assertEqual(len(results), 1)
        self.assertIn('count', results[0])
        self.assertEqual(results[0]['count'], 2)
    
    def test_get_table_names(self):
        """Test retrieving table names"""
        connector = DatabaseConnector(self.db_path)
        table_names = connector.get_table_names()
        expected_tables = {'users', 'posts'}
        self.assertEqual(set(table_names), expected_tables)
    
    def test_get_table_structure_users(self):
        """Test retrieving table structure for users table"""
        connector = DatabaseConnector(self.db_path)
        structure = connector.get_table_structure('users')
        
        self.assertIsInstance(structure, TableStructure)
        self.assertEqual(structure.name, 'users')
        
        # Check columns
        column_names = [col.name for col in structure.columns]
        expected_columns = ['id', 'username', 'email', 'age', 'is_active', 'created_at']
        self.assertEqual(column_names, expected_columns)
        
        # Check primary key
        self.assertIsNotNone(structure.primary_key)
        if structure.primary_key:
            self.assertEqual(structure.primary_key.columns, ['id'])
        
        # Check unique constraints
        unique_constraint_columns = [uc.columns for uc in structure.unique_constraints]
        self.assertIn(['username'], unique_constraint_columns)
    
    def test_get_table_structure_posts(self):
        """Test retrieving table structure for posts table"""
        connector = DatabaseConnector(self.db_path)
        structure = connector.get_table_structure('posts')
        
        self.assertEqual(structure.name, 'posts')
        
        # Check foreign keys
        self.assertEqual(len(structure.foreign_keys), 1)
        fk = structure.foreign_keys[0]
        self.assertEqual(fk.columns, ['user_id'])
        self.assertEqual(fk.referenced_table, 'users')
        self.assertEqual(fk.referenced_columns, ['id'])
    
    def test_get_table_structure_nonexistent(self):
        """Test retrieving structure for nonexistent table"""
        connector = DatabaseConnector(self.db_path)
        with self.assertRaises(SchemaExtractionError):
            connector.get_table_structure('nonexistent_table')
    
    def test_get_database_schema(self):
        """Test retrieving complete database schema"""
        connector = DatabaseConnector(self.db_path)
        schema = connector.get_schema()
        
        self.assertIsInstance(schema, DatabaseSchema)
        
        # Check tables
        table_names = set(schema.tables.keys())
        expected_tables = {'users', 'posts'}
        self.assertEqual(table_names, expected_tables)
        
        # Check views
        view_names = [view.name for view in schema.views]
        self.assertIn('active_users', view_names)
        
        # Check triggers
        trigger_names = [trigger.name for trigger in schema.triggers]
        self.assertIn('update_timestamp', trigger_names)
        
        # Check indexes
        index_names = [index.name for index in schema.indexes]
        self.assertIn('idx_users_username', index_names)
        self.assertIn('idx_posts_title', index_names)
    
    def test_get_table_data(self):
        """Test retrieving table data"""
        connector = DatabaseConnector(self.db_path)
        data = connector.get_table_data('users')
        
        self.assertEqual(len(data), 2)
        self.assertIn('username', data[0])
        self.assertIn('email', data[0])
        
        usernames = [row['username'] for row in data]
        self.assertIn('john', usernames)
        self.assertIn('jane', usernames)
    
    def test_get_table_data_with_limit(self):
        """Test retrieving table data with limit"""
        connector = DatabaseConnector(self.db_path)
        data = connector.get_table_data('users', limit=1)
        
        self.assertEqual(len(data), 1)
    
    def test_get_table_data_nonexistent(self):
        """Test retrieving data from nonexistent table"""
        connector = DatabaseConnector(self.db_path)
        with self.assertRaises(Exception):  # Should raise some database error
            connector.get_table_data('nonexistent_table')
    
    def test_get_table_row_count(self):
        """Test getting table row count"""
        connector = DatabaseConnector(self.db_path)
        count = connector.get_row_count('users')
        self.assertEqual(count, 2)
        
        count = connector.get_row_count('posts')
        self.assertEqual(count, 1)
    
    def test_get_table_row_count_nonexistent(self):
        """Test getting row count for nonexistent table"""
        connector = DatabaseConnector(self.db_path)
        with self.assertRaises(Exception):
            connector.get_row_count('nonexistent_table')
    
    @patch('sqlite3.connect')
    def test_connection_error_handling(self, mock_connect):
        """Test handling of connection errors"""
        mock_connect.side_effect = sqlite3.Error("Connection failed")
        
        with self.assertRaises(DatabaseConnectionError):
            DatabaseConnector("test.db")
    
    def test_column_type_parsing(self):
        """Test parsing of various column types"""
        # Create database with various column types
        test_db = os.path.join(self.temp_dir, "types_test.db")
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE type_test (
                int_col INTEGER,
                text_col TEXT,
                real_col REAL,
                blob_col BLOB,
                varchar_col VARCHAR(255),
                decimal_col DECIMAL(10,2),
                timestamp_col TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()
        
        connector = DatabaseConnector(test_db)
        structure = connector.get_table_structure('type_test')
        
        column_types = {col.name: col.type for col in structure.columns}
        
        self.assertIn('INTEGER', column_types['int_col'])
        self.assertIn('TEXT', column_types['text_col'])
        self.assertIn('REAL', column_types['real_col'])
        self.assertIn('BLOB', column_types['blob_col'])
        self.assertIn('VARCHAR', column_types['varchar_col'])
        self.assertIn('DECIMAL', column_types['decimal_col'])
        self.assertIn('TIMESTAMP', column_types['timestamp_col'])
        
        os.remove(test_db)
    
    def test_constraint_parsing(self):
        """Test parsing of various constraint types"""
        # Create database with various constraints
        test_db = os.path.join(self.temp_dir, "constraints_test.db")
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE constraint_test (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER CHECK (age > 0),
                status TEXT DEFAULT 'active'
            )
        ''')
        conn.commit()
        conn.close()
        
        connector = DatabaseConnector(test_db)
        structure = connector.get_table_structure('constraint_test')
        
        # Check primary key
        self.assertIsNotNone(structure.primary_key)
        if structure.primary_key:
            self.assertEqual(structure.primary_key.columns, ['id'])
        
        # Check unique constraints
        unique_columns = [uc.columns for uc in structure.unique_constraints]
        self.assertIn(['email'], unique_columns)
        
        # Check check constraints
        check_constraint_names = [cc.name for cc in structure.check_constraints]
        self.assertTrue(any('age' in name.lower() for name in check_constraint_names) or 
                       any('age > 0' in cc.expression for cc in structure.check_constraints))
        
        os.remove(test_db)
    
    def test_empty_database(self):
        """Test handling of empty database"""
        empty_db = os.path.join(self.temp_dir, "empty.db")
        conn = sqlite3.connect(empty_db)
        conn.close()
        
        connector = DatabaseConnector(empty_db)
        table_names = connector.get_table_names()
        self.assertEqual(len(table_names), 0)
        
        schema = connector.get_schema()
        self.assertEqual(len(schema.tables), 0)
        self.assertEqual(len(schema.views), 0)
        self.assertEqual(len(schema.triggers), 0)
        
        os.remove(empty_db)


if __name__ == '__main__':
    unittest.main()
