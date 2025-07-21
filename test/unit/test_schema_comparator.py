"""
Comprehensive unit tests for the SchemaComparator class.
"""

import unittest
from dbchecker.schema_comparator import SchemaComparator
from dbchecker.models import (
    DatabaseSchema, TableStructure, Column, Index, Trigger, View,
    PrimaryKey, ForeignKey, UniqueConstraint, CheckConstraint,
    SchemaComparisonResult, TableComparisonResult, FieldDifference
)


class TestSchemaComparator(unittest.TestCase):
    """Test cases for SchemaComparator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.comparator = SchemaComparator()
        
        # Create test table structures
        self.users_columns = [
            Column("id", "INTEGER", False, None, True),
            Column("username", "TEXT", False, None, False),
            Column("email", "TEXT", True, None, False),
            Column("age", "INTEGER", True, "0", False)
        ]
        
        self.posts_columns = [
            Column("id", "INTEGER", False, None, True),
            Column("title", "TEXT", False, None, False),
            Column("content", "TEXT", True, None, False),
            Column("user_id", "INTEGER", True, None, False)
        ]
        
        self.users_table = TableStructure(
            name="users",
            columns=self.users_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        self.posts_table = TableStructure(
            name="posts",
            columns=self.posts_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[ForeignKey(["user_id"], "users", ["id"])],
            unique_constraints=[],
            check_constraints=[]
        )
    
    def test_compare_identical_schemas(self):
        """Test comparing two identical schemas"""
        schema1 = DatabaseSchema(
            tables={"users": self.users_table, "posts": self.posts_table},
            views=[View("user_view", "SELECT * FROM users")],
            triggers=[Trigger("user_trigger", "users", "INSERT", "AFTER", "BEGIN END")],
            indexes=[Index("idx_username", "users", ["username"], True)]
        )
        
        schema2 = DatabaseSchema(
            tables={"users": self.users_table, "posts": self.posts_table},
            views=[View("user_view", "SELECT * FROM users")],
            triggers=[Trigger("user_trigger", "users", "INSERT", "AFTER", "BEGIN END")],
            indexes=[Index("idx_username", "users", ["username"], True)]
        )
        
        result = self.comparator.compare_schemas(schema1, schema2)
        
        self.assertIsInstance(result, SchemaComparisonResult)
        self.assertTrue(result.identical)
        self.assertEqual(len(result.missing_in_db1), 0)
        self.assertEqual(len(result.missing_in_db2), 0)
        self.assertEqual(len(result.table_differences), 0)
    
    def test_compare_schemas_missing_tables(self):
        """Test comparing schemas with missing tables"""
        schema1 = DatabaseSchema(
            tables={"users": self.users_table},
            views=[], triggers=[], indexes=[]
        )
        
        schema2 = DatabaseSchema(
            tables={"users": self.users_table, "posts": self.posts_table},
            views=[], triggers=[], indexes=[]
        )
        
        result = self.comparator.compare_schemas(schema1, schema2)
        
        self.assertFalse(result.identical)
        self.assertEqual(result.missing_in_db1, ["posts"])
        self.assertEqual(len(result.missing_in_db2), 0)
        self.assertEqual(len(result.table_differences), 0)
    
    def test_compare_schemas_extra_tables(self):
        """Test comparing schemas with extra tables in first database"""
        schema1 = DatabaseSchema(
            tables={"users": self.users_table, "posts": self.posts_table},
            views=[], triggers=[], indexes=[]
        )
        
        schema2 = DatabaseSchema(
            tables={"users": self.users_table},
            views=[], triggers=[], indexes=[]
        )
        
        result = self.comparator.compare_schemas(schema1, schema2)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.missing_in_db1), 0)
        self.assertEqual(result.missing_in_db2, ["posts"])
        self.assertEqual(len(result.table_differences), 0)
    
    def test_compare_identical_tables(self):
        """Test comparing two identical tables"""
        result = self.comparator.compare_tables(self.users_table, self.users_table)
        
        self.assertIsInstance(result, TableComparisonResult)
        self.assertTrue(result.identical)
        self.assertEqual(result.table_name, "users")
        self.assertEqual(len(result.missing_columns_db1), 0)
        self.assertEqual(len(result.missing_columns_db2), 0)
        self.assertEqual(len(result.column_differences), 0)
    
    def test_compare_tables_missing_columns(self):
        """Test comparing tables with missing columns"""
        # Create modified users table with missing email column
        modified_columns = [col for col in self.users_columns if col.name != "email"]
        modified_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(result.missing_columns_db1, [])
        self.assertEqual(result.missing_columns_db2, ["email"])
        self.assertEqual(len(result.column_differences), 0)
    
    def test_compare_tables_extra_columns(self):
        """Test comparing tables with extra columns"""
        # Create modified users table with extra column
        extra_columns = self.users_columns + [Column("status", "TEXT", True, "active", False)]
        modified_table = TableStructure(
            name="users",
            columns=extra_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(result.missing_columns_db1, ["status"])
        self.assertEqual(result.missing_columns_db2, [])
        self.assertEqual(len(result.column_differences), 0)
    
    def test_compare_tables_different_column_types(self):
        """Test comparing tables with different column types"""
        # Create modified users table with different age column type
        modified_columns = []
        for col in self.users_columns:
            if col.name == "age":
                modified_col = Column("age", "TEXT", True, "0", False)
                modified_columns.append(modified_col)
            else:
                modified_columns.append(col)
        
        modified_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.missing_columns_db1), 0)
        self.assertEqual(len(result.missing_columns_db2), 0)
        self.assertEqual(len(result.column_differences), 1)
        
        diff = result.column_differences[0]
        self.assertEqual(diff.field_name, "age.type")
        self.assertIn("INTEGER", str(diff.value_db1))
        self.assertIn("TEXT", str(diff.value_db2))
    
    def test_compare_tables_different_nullability(self):
        """Test comparing tables with different column nullability"""
        # Create modified users table with different email nullability
        modified_columns = []
        for col in self.users_columns:
            if col.name == "email":
                modified_col = Column("email", "TEXT", False, None, False)  # Changed to NOT NULL
                modified_columns.append(modified_col)
            else:
                modified_columns.append(col)
        
        modified_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.column_differences), 1)
        
        diff = result.column_differences[0]
        self.assertEqual(diff.field_name, "email.nullable")
    
    def test_compare_tables_different_defaults(self):
        """Test comparing tables with different column defaults"""
        # Create modified users table with different age default
        modified_columns = []
        for col in self.users_columns:
            if col.name == "age":
                modified_col = Column("age", "INTEGER", True, "18", False)  # Changed default
                modified_columns.append(modified_col)
            else:
                modified_columns.append(col)
        
        modified_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.column_differences), 1)
        
        diff = result.column_differences[0]
        self.assertEqual(diff.field_name, "age.default")
    
    def test_compare_tables_different_primary_keys(self):
        """Test comparing tables with different primary keys"""
        # Create modified users table with compound primary key
        modified_table = TableStructure(
            name="users",
            columns=self.users_columns,
            primary_key=PrimaryKey(columns=["id", "username"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.column_differences), 1)
        
        # Should detect difference in primary key structure
        pk_diff = next((diff for diff in result.column_differences 
                       if "primary_key" in diff.field_name.lower()), None)
        self.assertIsNotNone(pk_diff)
    
    def test_compare_tables_different_foreign_keys(self):
        """Test comparing tables with different foreign keys"""
        # Create modified posts table with additional foreign key
        modified_fks = self.posts_table.foreign_keys + [
            ForeignKey(["title"], "categories", ["name"])
        ]
        
        modified_table = TableStructure(
            name="posts",
            columns=self.posts_columns,
            primary_key=self.posts_table.primary_key,
            foreign_keys=modified_fks,
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.comparator.compare_tables(self.posts_table, modified_table)
        
        self.assertFalse(result.identical)
        # Foreign key differences should be detected in the comparison
        self.assertTrue(len(result.column_differences) > 0 or 
                       not result.identical)
    
    def test_compare_empty_schemas(self):
        """Test comparing empty schemas"""
        schema1 = DatabaseSchema(tables={}, views=[], triggers=[], indexes=[])
        schema2 = DatabaseSchema(tables={}, views=[], triggers=[], indexes=[])
        
        result = self.comparator.compare_schemas(schema1, schema2)
        
        self.assertTrue(result.identical)
        self.assertEqual(len(result.missing_in_db1), 0)
        self.assertEqual(len(result.missing_in_db2), 0)
        self.assertEqual(len(result.table_differences), 0)
    
    def test_compare_schemas_with_table_differences(self):
        """Test comparing schemas where common tables have differences"""
        # Create modified schema with different users table
        modified_columns = [col for col in self.users_columns if col.name != "email"]
        modified_users_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        schema1 = DatabaseSchema(
            tables={"users": self.users_table},
            views=[], triggers=[], indexes=[]
        )
        
        schema2 = DatabaseSchema(
            tables={"users": modified_users_table},
            views=[], triggers=[], indexes=[]
        )
        
        result = self.comparator.compare_schemas(schema1, schema2)
        
        self.assertFalse(result.identical)
        self.assertEqual(len(result.missing_in_db1), 0)
        self.assertEqual(len(result.missing_in_db2), 0)
        self.assertEqual(len(result.table_differences), 1)
        self.assertIn("users", result.table_differences)
        
        table_diff = result.table_differences["users"]
        self.assertFalse(table_diff.identical)
    
    def test_compare_column_order_independence(self):
        """Test that column order doesn't affect comparison"""
        # Create users table with columns in different order
        reordered_columns = [
            self.users_columns[1],  # username
            self.users_columns[0],  # id
            self.users_columns[3],  # age
            self.users_columns[2],  # email
        ]
        
        reordered_table = TableStructure(
            name="users",
            columns=reordered_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[UniqueConstraint("uk_username", ["username"])],
            check_constraints=[CheckConstraint("ck_age", "age >= 0")]
        )
        
        result = self.comparator.compare_tables(self.users_table, reordered_table)
        
        # Should be identical despite different column order
        self.assertTrue(result.identical)
        self.assertEqual(len(result.missing_columns_db1), 0)
        self.assertEqual(len(result.missing_columns_db2), 0)
        self.assertEqual(len(result.column_differences), 0)
    
    def test_compare_tables_case_sensitivity(self):
        """Test case sensitivity in table and column names"""
        # Create table with different case column name
        modified_columns = []
        for col in self.users_columns:
            if col.name == "username":
                modified_col = Column("USERNAME", col.type, col.nullable, col.default, col.is_primary_key)
                modified_columns.append(modified_col)
            else:
                modified_columns.append(col)
        
        modified_table = TableStructure(
            name="users",
            columns=modified_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.comparator.compare_tables(self.users_table, modified_table)
        
        # Should detect case difference
        self.assertFalse(result.identical)
        self.assertEqual(result.missing_columns_db1, ["USERNAME"])
        self.assertEqual(result.missing_columns_db2, ["username"])


if __name__ == '__main__':
    unittest.main()
