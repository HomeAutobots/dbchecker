"""
Unit tests for CLI module in cli.py to achieve 100% coverage.
"""

import unittest
from unittest.mock import MagicMock, patch, call, mock_open
import sys
import os
import argparse
from io import StringIO

from dbchecker.cli import main
from dbchecker.exceptions import DatabaseComparisonError


class TestCLI(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.test_db1 = 'test_db1.sqlite'
        self.test_db2 = 'test_db2.sqlite'
        
        # Patch sys.argv to avoid interference from actual command line arguments
        self.argv_patcher = patch('sys.argv', ['dbchecker'])
        self.mock_argv = self.argv_patcher.start()
        
    def tearDown(self):
        """Clean up after tests"""
        self.argv_patcher.stop()

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_basic_execution_success(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test basic successful execution with minimal arguments"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify comparator was created and used correctly
        mock_comparator_class.assert_called_once_with(
            db1_path='db1.sqlite',
            db2_path='db2.sqlite',
            uuid_columns=[]
        )
        mock_comparator.set_comparison_options.assert_called_once()
        mock_comparator.compare.assert_called_once()
        mock_comparator.generate_reports.assert_called_once()
        
        # Verify successful exit
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_execution_with_differences(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test execution when differences are found"""
        # Mock the comparison result with differences
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 1
        mock_result.summary.tables_with_differences = 1
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 5
        mock_result.schema_comparison = MagicMock()
        mock_result.schema_comparison.identical = False
        mock_result.data_comparison = MagicMock()
        mock_result.data_comparison.total_differences = 5
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify exit code indicates differences found
        mock_exit.assert_called_once_with(1)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--schema-only', '--data-only'])
    @patch('sys.exit')
    @patch('builtins.print')
    def test_main_conflicting_arguments_error(self, mock_print, mock_exit):
        """Test error when both --schema-only and --data-only are specified"""
        main()
        
        # Verify error was printed and exit was called
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Cannot specify both --schema-only and --data-only" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Error message not found in stderr calls: {stderr_calls}")
        
        # Mock exit should be called with 1 - check all calls
        exit_calls = mock_exit.call_args_list
        self.assertTrue(any(call == call(1) for call in exit_calls), f"sys.exit(1) not found in calls: {exit_calls}")

    @patch('sys.argv', ['dbchecker', 'nonexistent.db', 'db2.sqlite'])
    @patch('os.path.exists', side_effect=lambda path: path == 'db2.sqlite')
    @patch('sys.exit')
    @patch('builtins.print')
    def test_main_missing_database1_error(self, mock_print, mock_exit, mock_exists):
        """Test error when first database file doesn't exist"""
        main()
        
        # Verify error was printed and exit was called
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Database file not found: nonexistent.db" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Error message not found in stderr calls: {stderr_calls}")
        
        # Check that sys.exit(1) was called
        exit_calls = mock_exit.call_args_list
        self.assertTrue(any(call == call(1) for call in exit_calls), f"sys.exit(1) not found in calls: {exit_calls}")

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'nonexistent.db'])
    @patch('os.path.exists', side_effect=lambda path: path == 'db1.sqlite')
    @patch('sys.exit')
    @patch('builtins.print')
    def test_main_missing_database2_error(self, mock_print, mock_exit, mock_exists):
        """Test error when second database file doesn't exist"""
        main()
        
        # Verify error was printed and exit was called
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Database file not found: nonexistent.db" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Error message not found in stderr calls: {stderr_calls}")
        
        # Check that sys.exit(1) was called
        exit_calls = mock_exit.call_args_list
        self.assertTrue(any(call == call(1) for call in exit_calls), f"sys.exit(1) not found in calls: {exit_calls}")

    @patch('sys.argv', [
        'dbchecker', 'db1.sqlite', 'db2.sqlite',
        '--uuid-columns', 'id', 'user_id',
        '--no-auto-detect-uuids',
        '--uuid-patterns', '^uuid_', '^id_',
        '--uuid-comparison-mode', 'include_with_tracking',
        '--unique-id-patterns', '^report-\\d+$',
        '--normalize-patterns', '^(report|record)-(.+)$:id-\\2',
        '--exclude-columns', 'created_at', 'updated_at',
        '--exclude-column-patterns', '_timestamp$', '_date$',
        '--schema-only',
        '--case-insensitive',
        '--ignore-whitespace',
        '--batch-size', '2000',
        '--no-parallel',
        '--max-workers', '8',
        '--output-dir', '/tmp/reports',
        '--output-format', 'json', 'csv',
        '--filename-prefix', 'test_report',
        '--max-differences', '50',
        '--verbose'
    ])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_all_arguments(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with all possible arguments"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = MagicMock()
        mock_result.schema_comparison.identical = True
        mock_result.data_comparison = None  # Schema only
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify comparator was created with correct arguments
        mock_comparator_class.assert_called_once_with(
            db1_path='db1.sqlite',
            db2_path='db2.sqlite',
            uuid_columns=['id', 'user_id']
        )
        
        # Verify options were set correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(call_args.explicit_uuid_columns, ['id', 'user_id'])
        self.assertFalse(call_args.auto_detect_uuids)
        self.assertEqual(call_args.uuid_patterns, ['^uuid_', '^id_'])
        self.assertEqual(call_args.uuid_comparison_mode, 'include_with_tracking')
        self.assertEqual(call_args.unique_id_patterns, ['^report-\\d+$'])
        self.assertEqual(len(call_args.unique_id_normalize_patterns), 1)
        self.assertEqual(call_args.unique_id_normalize_patterns[0]['pattern'], '^(report|record)-(.+)$')
        self.assertEqual(call_args.unique_id_normalize_patterns[0]['replacement'], 'id-\\2')
        self.assertEqual(call_args.excluded_columns, ['created_at', 'updated_at'])
        self.assertEqual(call_args.excluded_column_patterns, ['_timestamp$', '_date$'])
        self.assertTrue(call_args.compare_schema)
        self.assertFalse(call_args.compare_data)  # Schema only
        self.assertFalse(call_args.case_sensitive)  # Case insensitive
        self.assertTrue(call_args.ignore_whitespace)
        self.assertEqual(call_args.batch_size, 2000)
        self.assertFalse(call_args.parallel_tables)  # No parallel
        self.assertEqual(call_args.max_workers, 8)
        self.assertEqual(call_args.output_format, ['json', 'csv'])
        self.assertTrue(call_args.verbose)
        self.assertEqual(call_args.max_differences_per_table, 50)
        
        # Verify reports were generated with correct parameters
        mock_comparator.generate_reports.assert_called_once_with(
            mock_result,
            output_dir='/tmp/reports',
            filename_prefix='test_report'
        )
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--quiet'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_quiet_mode(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function in quiet mode"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify no print calls were made (quiet mode)
        mock_print.assert_not_called()
        
        # Verify options were set correctly (verbose should be False in quiet mode)
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertFalse(call_args.verbose)
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--verbose', '--quiet'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_verbose_and_quiet_mode(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test that quiet overrides verbose"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify options were set correctly (verbose should be False when quiet is also specified)
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertFalse(call_args.verbose)
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--normalize-patterns', 'invalid_pattern'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_invalid_normalize_pattern(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test warning for invalid normalize pattern format"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify warning was printed to stderr (captured via mock_print calls)
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        warning_found = any("Warning: Invalid normalize pattern format 'invalid_pattern'" in str(call) for call in stderr_calls)
        self.assertTrue(warning_found, f"Warning not found in stderr calls: {stderr_calls}")
        
        # Verify options were set with empty normalize patterns
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(call_args.unique_id_normalize_patterns, [])
        
        mock_exit.assert_called_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_database_comparison_error(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test handling of DatabaseComparisonError"""
        # Mock the comparator to raise an exception
        mock_comparator = MagicMock()
        mock_comparator.compare.side_effect = DatabaseComparisonError("Test error")
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify error was printed to stderr (captured via mock_print calls)
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Comparison error: Test error" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Error message not found in stderr calls: {stderr_calls}")
        
        # Verify exit code 2 for comparison error
        mock_exit.assert_called_with(2)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_keyboard_interrupt(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test handling of KeyboardInterrupt"""
        # Mock the comparator to raise KeyboardInterrupt
        mock_comparator = MagicMock()
        mock_comparator.compare.side_effect = KeyboardInterrupt()
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify error was printed to stderr (captured via mock_print calls)
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Comparison interrupted by user" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Interrupt message not found in stderr calls: {stderr_calls}")
        
        # Verify exit code 130 for keyboard interrupt
        mock_exit.assert_called_with(130)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_unexpected_error(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test handling of unexpected error"""
        # Mock the comparator to raise an unexpected exception
        mock_comparator = MagicMock()
        mock_comparator.compare.side_effect = ValueError("Unexpected error")
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify error was printed to stderr (captured via mock_print calls)
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Unexpected error: Unexpected error" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Unexpected error message not found in stderr calls: {stderr_calls}")
        
        # Verify exit code 3 for unexpected error
        mock_exit.assert_called_with(3)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--verbose'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_unexpected_error_verbose(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test handling of unexpected error in verbose mode"""
        # Mock the comparator to raise an unexpected exception
        mock_comparator = MagicMock()
        mock_comparator.compare.side_effect = ValueError("Unexpected error")
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print, \
             patch('traceback.print_exc') as mock_traceback:
            main()
        
        # Verify error was printed to stderr (captured via mock_print calls)
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        error_found = any("Unexpected error: Unexpected error" in str(call) for call in stderr_calls)
        self.assertTrue(error_found, f"Unexpected error message not found in stderr calls: {stderr_calls}")
        
        # Verify traceback was printed in verbose mode
        mock_traceback.assert_called_once()
        
        # Verify exit code 3 for unexpected error
        mock_exit.assert_called_with(3)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--data-only'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_data_only_mode(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with --data-only flag"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None  # Data only
        mock_result.data_comparison = MagicMock()
        mock_result.data_comparison.total_differences = 0
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify options were set correctly for data-only mode
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertFalse(call_args.compare_schema)
        self.assertTrue(call_args.compare_data)
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--normalize-patterns', 'valid:pattern', 'another:valid'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_valid_normalize_patterns(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with valid normalize patterns"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify normalize patterns were parsed correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(len(call_args.unique_id_normalize_patterns), 2)
        self.assertEqual(call_args.unique_id_normalize_patterns[0]['pattern'], 'valid')
        self.assertEqual(call_args.unique_id_normalize_patterns[0]['replacement'], 'pattern')
        self.assertEqual(call_args.unique_id_normalize_patterns[1]['pattern'], 'another')
        self.assertEqual(call_args.unique_id_normalize_patterns[1]['replacement'], 'valid')
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--output-format', 'html', 'markdown'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_custom_output_formats(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with custom output formats"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify output formats were set correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(call_args.output_format, ['html', 'markdown'])
        
        mock_exit.assert_called_once_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('sys.exit')
    def test_main_path_mkdir_called(self, mock_exit, mock_path, mock_exists):
        """Test that output directory is created"""
        # Mock Path.mkdir to verify it's called
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        
        with patch('dbchecker.cli.DatabaseComparator') as mock_comparator_class:
            # Mock the comparison result
            mock_result = MagicMock()
            mock_result.summary.total_tables = 2
            mock_result.summary.identical_tables = 2
            mock_result.summary.tables_with_differences = 0
            mock_result.summary.total_rows_compared = 100
            mock_result.summary.total_differences_found = 0
            mock_result.schema_comparison = None
            mock_result.data_comparison = None
            
            # Mock the comparator instance
            mock_comparator = MagicMock()
            mock_comparator.compare.return_value = mock_result
            mock_comparator_class.return_value = mock_comparator
            
            with patch('builtins.print') as mock_print:
                main()
        
        # Verify Path was called with the default output directory "."
        mock_path.assert_called_once_with('.')
        mock_path_instance.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        mock_exit.assert_called_once_with(0)

    def test_script_execution_block(self):
        """Test the if __name__ == '__main__' block coverage"""
        import subprocess
        import sys
        import tempfile
        import os
        
        # Create temporary database files for the test
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp1, \
             tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp2:
            
            try:
                # Execute the CLI module directly to cover the __name__ == '__main__' block
                result = subprocess.run([
                    sys.executable, '-m', 'dbchecker.cli', 
                    tmp1.name, tmp2.name, '--quiet'
                ], capture_output=True, text=True, timeout=10)
                
                # The command should execute successfully
                self.assertEqual(result.returncode, 0, f"CLI execution failed: {result.stderr}")
                
            except subprocess.TimeoutExpired:
                self.fail("CLI execution timed out")
            finally:
                # Clean up temporary files
                try:
                    os.unlink(tmp1.name)
                    os.unlink(tmp2.name)
                except:
                    pass

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--normalize-patterns', 'pattern1:replacement1', 'pattern2:replacement2', 'invalid'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_mixed_normalize_patterns(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with mix of valid and invalid normalize patterns"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify warning was printed for invalid pattern
        stderr_calls = [call for call in mock_print.call_args_list if len(call[1]) > 0 and call[1].get('file') == sys.stderr]
        warning_found = any("Warning: Invalid normalize pattern format 'invalid'" in str(call) for call in stderr_calls)
        self.assertTrue(warning_found, f"Warning not found in stderr calls: {stderr_calls}")
        
        # Verify valid patterns were processed correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(len(call_args.unique_id_normalize_patterns), 2)
        
        mock_exit.assert_called_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--uuid-comparison-mode', 'include_normal'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_include_normal_uuid_mode(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with include_normal UUID comparison mode"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify UUID comparison mode was set correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(call_args.uuid_comparison_mode, 'include_normal')
        
        mock_exit.assert_called_with(0)

    @patch('sys.argv', ['dbchecker', 'db1.sqlite', 'db2.sqlite', '--max-differences', '200'])
    @patch('os.path.exists', return_value=True)
    @patch('dbchecker.cli.Path')
    @patch('dbchecker.cli.DatabaseComparator')
    @patch('sys.exit')
    def test_main_custom_max_differences(self, mock_exit, mock_comparator_class, mock_path, mock_exists):
        """Test main function with custom max differences setting"""
        # Mock the comparison result
        mock_result = MagicMock()
        mock_result.summary.total_tables = 2
        mock_result.summary.identical_tables = 2
        mock_result.summary.tables_with_differences = 0
        mock_result.summary.total_rows_compared = 100
        mock_result.summary.total_differences_found = 0
        mock_result.schema_comparison = None
        mock_result.data_comparison = None
        
        # Mock the comparator instance
        mock_comparator = MagicMock()
        mock_comparator.compare.return_value = mock_result
        mock_comparator_class.return_value = mock_comparator
        
        # Mock Path.mkdir
        mock_path.return_value.mkdir = MagicMock()
        
        with patch('builtins.print') as mock_print:
            main()
        
        # Verify max differences was set correctly
        call_args = mock_comparator.set_comparison_options.call_args[0][0]
        self.assertEqual(call_args.max_differences_per_table, 200)
        
        mock_exit.assert_called_with(0)


if __name__ == '__main__':
    unittest.main()
