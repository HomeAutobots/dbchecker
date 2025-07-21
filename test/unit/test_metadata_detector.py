"""
Comprehensive unit tests for the MetadataDetector class.
"""

import unittest
import re
from unittest.mock import MagicMock

from dbchecker.metadata_detector import MetadataDetector
from dbchecker.models import ComparisonOptions, Column, TableStructure, PrimaryKey


class TestMetadataDetector(unittest.TestCase):
    """Test cases for MetadataDetector class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.default_options = ComparisonOptions()
        self.detector = MetadataDetector(self.default_options)
        
        # Create test table structure with various column types
        self.test_columns = [
            Column("id", "INTEGER", False, None, True),
            Column("name", "TEXT", False, None, False),
            Column("email", "TEXT", True, None, False),
            Column("created_at", "DATETIME", True, None, False),
            Column("updated_at", "TIMESTAMP", True, None, False),
            Column("created_by", "TEXT", True, None, False),
            Column("session_id", "TEXT", True, None, False),
            Column("user_uuid", "TEXT", True, None, False),
            Column("record_seq", "INTEGER", False, None, False),
            Column("version_number", "INTEGER", True, None, False),
            Column("custom_field", "TEXT", True, None, False)
        ]
        
        self.test_table_structure = TableStructure(
            name="test_table",
            columns=self.test_columns,
            primary_key=PrimaryKey(columns=["id"]),
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
    
    def test_init_with_default_options(self):
        """Test initialization with default options"""
        detector = MetadataDetector(ComparisonOptions())
        
        self.assertIsInstance(detector.options, ComparisonOptions)
        self.assertIsInstance(detector.default_timestamp_patterns, list)
        self.assertIsInstance(detector.default_metadata_patterns, list)
        self.assertIsInstance(detector.default_sequence_patterns, list)
        self.assertIsInstance(detector.timestamp_data_types, set)
        self.assertIsInstance(detector.sequence_data_types, set)
        
        # Check that default patterns are populated
        self.assertGreater(len(detector.default_timestamp_patterns), 0)
        self.assertGreater(len(detector.default_metadata_patterns), 0)
        self.assertGreater(len(detector.default_sequence_patterns), 0)
    
    def test_detect_timestamp_columns_auto_detect_disabled(self):
        """Test timestamp detection when auto-detection is disabled"""
        options = ComparisonOptions(
            auto_detect_timestamps=False,
            explicit_timestamp_columns=["explicit_timestamp"]
        )
        detector = MetadataDetector(options)
        
        result = detector.detect_timestamp_columns(self.test_table_structure)
        
        self.assertEqual(result, ["explicit_timestamp"])
    
    def test_detect_timestamp_columns_by_data_type(self):
        """Test timestamp detection by data type"""
        result = self.detector.detect_timestamp_columns(self.test_table_structure)
        
        # Should detect created_at and updated_at by data type
        self.assertIn("created_at", result)
        self.assertIn("updated_at", result)
    
    def test_detect_timestamp_columns_by_name_pattern(self):
        """Test timestamp detection by name patterns"""
        columns = [
            Column("created", "TEXT", False, None, False),
            Column("modified", "TEXT", False, None, False),
            Column("deleted", "TEXT", False, None, False),
            Column("some_time", "TEXT", False, None, False),
            Column("end_date", "TEXT", False, None, False),
            Column("created_time", "TEXT", False, None, False),
            Column("regular_field", "TEXT", False, None, False)  # Changed from not_timestamp
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_timestamp_columns(table_structure)
        
        # Should detect all timestamp-like columns except regular_field
        expected = ["created", "modified", "deleted", "some_time", "end_date", "created_time"]
        for col in expected:
            self.assertIn(col, result)
        self.assertNotIn("regular_field", result)
    
    def test_detect_timestamp_columns_with_custom_patterns(self):
        """Test timestamp detection with custom patterns"""
        options = ComparisonOptions(
            auto_detect_timestamps=True,
            timestamp_patterns=[r".*custom_time.*", r"^special$"]
        )
        detector = MetadataDetector(options)
        
        columns = [
            Column("custom_time_field", "TEXT", False, None, False),
            Column("special", "TEXT", False, None, False),
            Column("created_at", "DATETIME", False, None, False),  # Will match by data type
            Column("other_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = detector.detect_timestamp_columns(table_structure)
        
        # Should match custom patterns AND data types
        self.assertIn("custom_time_field", result)
        self.assertIn("special", result)
        self.assertIn("created_at", result)  # Matches by DATETIME data type
        self.assertNotIn("other_field", result)
    
    def test_detect_timestamp_columns_with_explicit_columns(self):
        """Test timestamp detection with explicit columns"""
        options = ComparisonOptions(
            auto_detect_timestamps=True,
            explicit_timestamp_columns=["explicit_col1", "explicit_col2"]
        )
        detector = MetadataDetector(options)
        
        result = detector.detect_timestamp_columns(self.test_table_structure)
        
        # Should include both auto-detected and explicit columns
        self.assertIn("created_at", result)  # Auto-detected
        self.assertIn("updated_at", result)  # Auto-detected
        self.assertIn("explicit_col1", result)  # Explicit
        self.assertIn("explicit_col2", result)  # Explicit
    
    def test_detect_metadata_columns_auto_detect_disabled(self):
        """Test metadata detection when auto-detection is disabled"""
        options = ComparisonOptions(
            auto_detect_metadata=False,
            explicit_metadata_columns=["explicit_metadata"]
        )
        detector = MetadataDetector(options)
        
        result = detector.detect_metadata_columns(self.test_table_structure)
        
        self.assertEqual(result, ["explicit_metadata"])
    
    def test_detect_metadata_columns_by_default_patterns(self):
        """Test metadata detection by default patterns"""
        result = self.detector.detect_metadata_columns(self.test_table_structure)
        
        # Should detect created_by, session_id, and version_number
        self.assertIn("created_by", result)
        self.assertIn("session_id", result)
        self.assertIn("version_number", result)
    
    def test_detect_metadata_columns_by_audit_patterns(self):
        """Test metadata detection by audit patterns"""
        columns = [
            Column("author_user", "TEXT", False, None, False),
            Column("editor_user", "TEXT", False, None, False),
            Column("modified_by", "TEXT", False, None, False),
            Column("created_user", "TEXT", False, None, False),
            Column("data_source", "TEXT", False, None, False),
            Column("source_system", "TEXT", False, None, False),
            Column("system_id", "TEXT", False, None, False),
            Column("regular_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_metadata_columns(table_structure)
        
        # Should detect all audit-related columns except regular_field
        expected = ["author_user", "editor_user", "modified_by", "created_user", 
                   "data_source", "source_system", "system_id"]
        for col in expected:
            self.assertIn(col, result)
        self.assertNotIn("regular_field", result)
    
    def test_detect_metadata_columns_with_custom_patterns(self):
        """Test metadata detection with custom patterns"""
        options = ComparisonOptions(
            auto_detect_metadata=True,
            metadata_patterns=[r".*custom_meta.*", r"^special_field$"]
        )
        detector = MetadataDetector(options)
        
        columns = [
            Column("custom_meta_field", "TEXT", False, None, False),
            Column("special_field", "TEXT", False, None, False),
            Column("created_by", "TEXT", False, None, False),  # Will match audit patterns
            Column("other_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = detector.detect_metadata_columns(table_structure)
        
        # Should match custom patterns AND audit patterns
        self.assertIn("custom_meta_field", result)
        self.assertIn("special_field", result)
        self.assertIn("created_by", result)  # Matches audit pattern .*_by$
        self.assertNotIn("other_field", result)
    
    def test_detect_sequence_columns_auto_detect_disabled(self):
        """Test sequence detection when auto-detection is disabled"""
        options = ComparisonOptions(
            auto_detect_sequences=False,
            explicit_sequence_columns=["explicit_sequence"]
        )
        detector = MetadataDetector(options)
        
        result = detector.detect_sequence_columns(self.test_table_structure)
        
        self.assertEqual(result, ["explicit_sequence"])
    
    def test_detect_sequence_columns_by_data_type(self):
        """Test sequence detection by data type"""
        columns = [
            Column("serial_id", "SERIAL", False, None, False),
            Column("bigserial_id", "BIGSERIAL", False, None, False),
            Column("identity_id", "IDENTITY", False, None, False),
            Column("regular_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        detector = MetadataDetector(ComparisonOptions())
        result = detector.detect_sequence_columns(table_structure)
        
        # Should detect all sequence data types
        self.assertIn("serial_id", result)
        self.assertIn("bigserial_id", result)
        self.assertIn("identity_id", result)
        self.assertNotIn("regular_field", result)
    
    def test_detect_sequence_columns_by_primary_key_integer(self):
        """Test sequence detection by primary key with integer type"""
        result = self.detector.detect_sequence_columns(self.test_table_structure)
        
        # Should detect 'id' as it's a primary key with INTEGER type
        self.assertIn("id", result)
    
    def test_detect_sequence_columns_by_name_pattern(self):
        """Test sequence detection by name patterns"""
        columns = [
            Column("item_seq", "INTEGER", False, None, False),
            Column("record_sequence", "INTEGER", False, None, False),
            Column("row_number", "INTEGER", False, None, False),
            Column("user_rowid", "INTEGER", False, None, False),
            Column("regular_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_sequence_columns(table_structure)
        
        # Should detect all sequence-named columns
        expected = ["item_seq", "record_sequence", "row_number", "user_rowid"]
        for col in expected:
            self.assertIn(col, result)
        self.assertNotIn("regular_field", result)
    
    def test_detect_sequence_columns_with_sequential_sample_data(self):
        """Test sequence detection with sequential sample data"""
        columns = [
            Column("auto_id", "INTEGER", False, None, False),
            Column("not_sequential", "INTEGER", False, None, False),
            Column("text_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        # Sample data with sequential pattern for auto_id
        sample_data = [
            {"auto_id": 1, "not_sequential": 100, "text_field": "test1"},
            {"auto_id": 2, "not_sequential": 50, "text_field": "test2"},
            {"auto_id": 3, "not_sequential": 200, "text_field": "test3"},
            {"auto_id": 4, "not_sequential": 75, "text_field": "test4"}
        ]
        
        result = self.detector.detect_sequence_columns(table_structure, sample_data)
        
        # Should detect auto_id as sequential
        self.assertIn("auto_id", result)
        self.assertNotIn("not_sequential", result)
        self.assertNotIn("text_field", result)
    
    def test_appears_sequential_perfect_sequence(self):
        """Test _appears_sequential with perfect sequential data"""
        sample_data = [
            {"col": 1}, {"col": 2}, {"col": 3}, {"col": 4}, {"col": 5}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        self.assertTrue(result)
    
    def test_appears_sequential_mostly_sequential(self):
        """Test _appears_sequential with mostly sequential data"""
        sample_data = [
            {"col": 1}, {"col": 2}, {"col": 3}, {"col": 5}, {"col": 6}  # Missing 4
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        # 3 out of 4 differences are 1, which is 75% > 70% threshold
        self.assertTrue(result)
    
    def test_appears_sequential_non_sequential(self):
        """Test _appears_sequential with non-sequential data"""
        sample_data = [
            {"col": 1}, {"col": 10}, {"col": 100}, {"col": 1000}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        self.assertFalse(result)
    
    def test_appears_sequential_insufficient_data(self):
        """Test _appears_sequential with insufficient data"""
        sample_data = [{"col": 1}]
        
        result = self.detector._appears_sequential(sample_data, "col")
        self.assertFalse(result)
    
    def test_appears_sequential_with_none_values(self):
        """Test _appears_sequential with None values"""
        sample_data = [
            {"col": 1}, {"col": None}, {"col": 3}, {"col": 4}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        # Values after filtering None: [1, 3, 4]
        # Differences: [2, 1] - only 1 out of 2 differences is 1, which is 50% < 70% threshold
        self.assertFalse(result)
    
    def test_appears_sequential_with_non_integer_values(self):
        """Test _appears_sequential with non-integer values that can be converted"""
        sample_data = [
            {"col": "1"}, {"col": "2"}, {"col": "3"}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        # String values that can be converted to int should be treated as sequential
        self.assertTrue(result)
    
    def test_appears_sequential_with_non_convertible_values(self):
        """Test _appears_sequential with non-convertible values"""
        sample_data = [
            {"col": "abc"}, {"col": "def"}, {"col": "ghi"}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        # String values that cannot be converted to int should return False
        self.assertFalse(result)
    
    def test_appears_sequential_with_exception(self):
        """Test _appears_sequential handles exceptions gracefully"""
        sample_data = [
            {"col": 1}, {"col": "invalid"}, {"col": 3}
        ]
        
        result = self.detector._appears_sequential(sample_data, "col")
        self.assertFalse(result)
    
    def test_appears_sequential_division_by_zero(self):
        """Test _appears_sequential handles edge case causing exception"""
        # Create a sample data that will trigger the exception handler
        # by creating a scenario where the list comprehension fails
        
        # Create data that looks normal but will cause an exception during processing
        class ExceptionDict(dict):
            def get(self, key, default=None):
                if key == "col":
                    raise Exception("Mocked exception during processing")
                return super().get(key, default)
        
        sample_data = [ExceptionDict({"col": 1}), ExceptionDict({"col": 2})]
        
        # Type ignore for testing purposes - we're intentionally passing malformed data
        result = self.detector._appears_sequential(sample_data, "col")  # type: ignore
        self.assertFalse(result)
    
    def test_appears_sequential_missing_column(self):
        """Test _appears_sequential with missing column"""
        sample_data = [
            {"other_col": 1}, {"other_col": 2}
        ]
        
        result = self.detector._appears_sequential(sample_data, "missing_col")
        self.assertFalse(result)
    
    def test_get_excluded_columns_explicit_columns(self):
        """Test _get_excluded_columns with explicit columns"""
        options = ComparisonOptions(
            excluded_columns=["exclude1", "exclude2"]
        )
        detector = MetadataDetector(options)
        
        result = detector._get_excluded_columns(self.test_table_structure)
        
        self.assertIn("exclude1", result)
        self.assertIn("exclude2", result)
    
    def test_get_excluded_columns_pattern_matching(self):
        """Test _get_excluded_columns with pattern matching"""
        options = ComparisonOptions(
            excluded_column_patterns=[r".*_temp$", r"^test_.*"]
        )
        detector = MetadataDetector(options)
        
        columns = [
            Column("data_temp", "TEXT", False, None, False),
            Column("test_field", "TEXT", False, None, False),
            Column("test_another", "TEXT", False, None, False),
            Column("regular_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = detector._get_excluded_columns(table_structure)
        
        self.assertIn("data_temp", result)
        self.assertIn("test_field", result)
        self.assertIn("test_another", result)
        self.assertNotIn("regular_field", result)
    
    def test_get_excluded_columns_invalid_regex(self):
        """Test _get_excluded_columns with invalid regex patterns"""
        options = ComparisonOptions(
            excluded_column_patterns=["[invalid_regex", "valid_pattern"]
        )
        detector = MetadataDetector(options)
        
        columns = [
            Column("valid_pattern", "TEXT", False, None, False),
            Column("other_field", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = detector._get_excluded_columns(table_structure)
        
        # Should handle invalid regex gracefully and still process valid patterns
        self.assertIn("valid_pattern", result)
        self.assertNotIn("other_field", result)
    
    def test_get_all_excluded_columns_comprehensive(self):
        """Test get_all_excluded_columns with all types of exclusions"""
        options = ComparisonOptions(
            auto_detect_timestamps=True,
            auto_detect_metadata=True,
            auto_detect_sequences=True,
            excluded_columns=["manual_exclude"]
        )
        detector = MetadataDetector(options)
        
        uuid_columns = ["user_uuid"]
        
        result = detector.get_all_excluded_columns(
            self.test_table_structure, uuid_columns
        )
        
        # Verify return structure
        expected_keys = [
            'uuid_columns', 'timestamp_columns', 'metadata_columns', 
            'sequence_columns', 'excluded_columns', 'all_excluded'
        ]
        for key in expected_keys:
            self.assertIn(key, result)
        
        # Verify specific detections
        self.assertEqual(result['uuid_columns'], uuid_columns)
        self.assertIn("created_at", result['timestamp_columns'])
        self.assertIn("created_by", result['metadata_columns'])
        self.assertIn("id", result['sequence_columns'])
        self.assertIn("manual_exclude", result['excluded_columns'])
        
        # Verify all_excluded contains all types
        all_excluded = result['all_excluded']
        self.assertIn("user_uuid", all_excluded)
        self.assertIn("created_at", all_excluded)
        self.assertIn("created_by", all_excluded)
        self.assertIn("id", all_excluded)
        self.assertIn("manual_exclude", all_excluded)
        
        # Verify no duplicates in all_excluded
        self.assertEqual(len(all_excluded), len(set(all_excluded)))
    
    def test_get_all_excluded_columns_no_duplicates(self):
        """Test get_all_excluded_columns removes duplicates properly"""
        options = ComparisonOptions(
            explicit_timestamp_columns=["created_at"],  # Duplicate with auto-detected
            explicit_metadata_columns=["created_by"],   # Duplicate with auto-detected
            explicit_sequence_columns=["id"]            # Duplicate with auto-detected
        )
        detector = MetadataDetector(options)
        
        uuid_columns = ["user_uuid"]
        
        result = detector.get_all_excluded_columns(
            self.test_table_structure, uuid_columns
        )
        
        # Check that duplicates are removed
        all_excluded = result['all_excluded']
        self.assertEqual(len(all_excluded), len(set(all_excluded)))
        
        # Verify items appear only once
        self.assertEqual(all_excluded.count("created_at"), 1)
        self.assertEqual(all_excluded.count("created_by"), 1)
        self.assertEqual(all_excluded.count("id"), 1)
    
    def test_get_exclusion_summary_all_types(self):
        """Test get_exclusion_summary with all exclusion types"""
        exclusions = {
            'uuid_columns': ['user_uuid', 'session_uuid'],
            'timestamp_columns': ['created_at', 'updated_at'],
            'metadata_columns': ['created_by', 'session_id'],
            'sequence_columns': ['id', 'record_seq'],
            'excluded_columns': ['manual_exclude'],
            'all_excluded': ['user_uuid', 'session_uuid', 'created_at', 'updated_at', 
                           'created_by', 'session_id', 'id', 'record_seq', 'manual_exclude']
        }
        
        result = self.detector.get_exclusion_summary(exclusions)
        
        self.assertIn("UUID columns: user_uuid, session_uuid", result)
        self.assertIn("Timestamp columns: created_at, updated_at", result)
        self.assertIn("Metadata columns: created_by, session_id", result)
        self.assertIn("Sequence columns: id, record_seq", result)
        self.assertIn("User-excluded columns: manual_exclude", result)
        self.assertIn("Excluded from comparison", result)
    
    def test_get_exclusion_summary_some_types(self):
        """Test get_exclusion_summary with only some exclusion types"""
        exclusions = {
            'uuid_columns': ['user_uuid'],
            'timestamp_columns': [],
            'metadata_columns': ['created_by'],
            'sequence_columns': [],
            'excluded_columns': [],
            'all_excluded': ['user_uuid', 'created_by']
        }
        
        result = self.detector.get_exclusion_summary(exclusions)
        
        self.assertIn("UUID columns: user_uuid", result)
        self.assertIn("Metadata columns: created_by", result)
        self.assertNotIn("Timestamp columns", result)
        self.assertNotIn("Sequence columns", result)
        self.assertNotIn("User-excluded columns", result)
    
    def test_get_exclusion_summary_no_exclusions(self):
        """Test get_exclusion_summary with no exclusions"""
        exclusions = {
            'uuid_columns': [],
            'timestamp_columns': [],
            'metadata_columns': [],
            'sequence_columns': [],
            'excluded_columns': [],
            'all_excluded': []
        }
        
        result = self.detector.get_exclusion_summary(exclusions)
        
        self.assertEqual(result, "No columns excluded from comparison")
    
    def test_get_exclusion_summary_missing_excluded_columns_key(self):
        """Test get_exclusion_summary with missing excluded_columns key"""
        exclusions = {
            'uuid_columns': ['user_uuid'],
            'timestamp_columns': [],
            'metadata_columns': [],
            'sequence_columns': [],
            # 'excluded_columns' key is missing
            'all_excluded': ['user_uuid']
        }
        
        result = self.detector.get_exclusion_summary(exclusions)
        
        self.assertIn("UUID columns: user_uuid", result)
        self.assertNotIn("User-excluded columns", result)
    
    def test_detect_timestamp_columns_case_insensitive(self):
        """Test timestamp detection is case insensitive"""
        columns = [
            Column("Created_At", "TEXT", False, None, False),
            Column("UPDATED_TIME", "TEXT", False, None, False),
            Column("Modified", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_timestamp_columns(table_structure)
        
        # Should detect all regardless of case
        self.assertIn("Created_At", result)
        self.assertIn("UPDATED_TIME", result)
        self.assertIn("Modified", result)
    
    def test_detect_metadata_columns_case_insensitive(self):
        """Test metadata detection is case insensitive"""
        columns = [
            Column("Created_By", "TEXT", False, None, False),
            Column("SESSION_ID", "TEXT", False, None, False),
            Column("Version_Number", "TEXT", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=None,
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_metadata_columns(table_structure)
        
        # Should detect all regardless of case
        self.assertIn("Created_By", result)
        self.assertIn("SESSION_ID", result)
        self.assertIn("Version_Number", result)
    
    def test_detect_sequence_columns_case_insensitive(self):
        """Test sequence detection is case insensitive"""
        columns = [
            Column("Record_Seq", "INTEGER", False, None, False),
            Column("ID", "INTEGER", False, None, True),  # Primary key
            Column("User_Number", "INTEGER", False, None, False)
        ]
        
        table_structure = TableStructure(
            name="test_table",
            columns=columns,
            primary_key=PrimaryKey(columns=["ID"]),
            foreign_keys=[],
            unique_constraints=[],
            check_constraints=[]
        )
        
        result = self.detector.detect_sequence_columns(table_structure)
        
        # Should detect all regardless of case
        self.assertIn("Record_Seq", result)
        self.assertIn("ID", result)
        self.assertIn("User_Number", result)
    
    def test_detect_columns_with_sample_data_none(self):
        """Test detection methods handle None sample_data gracefully"""
        # All detection methods should work with sample_data=None
        timestamps = self.detector.detect_timestamp_columns(self.test_table_structure, None)
        metadata = self.detector.detect_metadata_columns(self.test_table_structure, None)
        sequences = self.detector.detect_sequence_columns(self.test_table_structure, None)
        
        self.assertIsInstance(timestamps, list)
        self.assertIsInstance(metadata, list)
        self.assertIsInstance(sequences, list)
    
    def test_detect_columns_with_empty_sample_data(self):
        """Test detection methods handle empty sample_data gracefully"""
        sample_data = []
        
        timestamps = self.detector.detect_timestamp_columns(self.test_table_structure, sample_data)
        metadata = self.detector.detect_metadata_columns(self.test_table_structure, sample_data)
        sequences = self.detector.detect_sequence_columns(self.test_table_structure, sample_data)
        
        self.assertIsInstance(timestamps, list)
        self.assertIsInstance(metadata, list)
        self.assertIsInstance(sequences, list)


if __name__ == '__main__':
    unittest.main()
