"""
Unit tests for DatabaseComparator in comparator.py to achieve 100% coverage.
"""

import unittest
from unittest.mock import MagicMock, patch, call, mock_open
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions, ComparisonResult, ComparisonSummary
from dbchecker.exceptions import DatabaseComparisonError, InvalidConfigurationError

class TestDatabaseComparator(unittest.TestCase):
    def setUp(self):
        self.db1_path = 'db1.sqlite'
        self.db2_path = 'db2.sqlite'
        self.comparator = DatabaseComparator(self.db1_path, self.db2_path)

    def test_init_sets_paths_and_components(self):
        self.assertEqual(self.comparator.db1_path, self.db1_path)
        self.assertEqual(self.comparator.db2_path, self.db2_path)
        self.assertIsNotNone(self.comparator.uuid_handler)
        self.assertIsNotNone(self.comparator.schema_comparator)
        self.assertIsNotNone(self.comparator.data_comparator)
        self.assertIsNotNone(self.comparator.report_generator)

    def test_set_comparison_options_updates_options_and_handlers(self):
        options = ComparisonOptions(explicit_uuid_columns=['col1'], uuid_patterns=[r'^uuid_'])
        self.comparator.set_comparison_options(options)
        self.assertEqual(self.comparator.options, options)
        # UUIDHandler should have received the explicit column and pattern
        self.assertIn('col1', self.comparator.uuid_handler.explicit_uuid_columns)
        self.assertIn(r'^uuid_', self.comparator.uuid_handler.custom_patterns)

    @patch('dbchecker.comparator.DatabaseComparator._validate_configuration')
    @patch('dbchecker.comparator.DatabaseComparator._initialize_connections')
    @patch('dbchecker.comparator.DatabaseComparator._cleanup_connections')
    @patch('dbchecker.comparator.DatabaseComparator._compare_schemas')
    @patch('dbchecker.comparator.DatabaseComparator._compare_data')
    @patch('dbchecker.comparator.DatabaseComparator._generate_summary')
    def test_compare_runs_all_phases_and_returns_result(self, mock_summary, mock_data, mock_schema, mock_cleanup, mock_init, mock_validate):
        mock_schema.return_value = 'schema_result'
        mock_data.return_value = 'data_result'
        mock_summary.return_value = 'summary'
        result = self.comparator.compare()
        self.assertIsInstance(result, ComparisonResult)
        self.assertEqual(result.schema_comparison, 'schema_result')
        self.assertEqual(result.data_comparison, 'data_result')
        self.assertEqual(result.summary, 'summary')
        mock_cleanup.assert_called()

    @patch('dbchecker.comparator.DatabaseComparator._validate_configuration', side_effect=Exception('fail'))
    @patch('dbchecker.comparator.DatabaseComparator._cleanup_connections')
    def test_compare_raises_and_wraps_exception(self, mock_cleanup, mock_validate):
        with self.assertRaises(DatabaseComparisonError):
            self.comparator.compare()
        mock_cleanup.assert_called()

    def test_validate_configuration_raises(self):
        self.comparator.db1_path = ''
        with self.assertRaises(InvalidConfigurationError):
            self.comparator._validate_configuration()
        self.comparator.db1_path = 'db1.sqlite'
        self.comparator.options.batch_size = 0
        with self.assertRaises(InvalidConfigurationError):
            self.comparator._validate_configuration()
        self.comparator.options.batch_size = 1
        self.comparator.options.max_workers = 0
        with self.assertRaises(InvalidConfigurationError):
            self.comparator._validate_configuration()

    @patch('dbchecker.comparator.DatabaseConnector')
    def test_initialize_connections_success_and_failure(self, mock_connector):
        self.comparator._initialize_connections()
        self.assertIsNotNone(self.comparator.conn1)
        self.assertIsNotNone(self.comparator.conn2)
        mock_connector.side_effect = Exception('fail')
        self.comparator.conn1 = None
        self.comparator.conn2 = None
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._initialize_connections()

    def test_cleanup_connections(self):
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        self.comparator._cleanup_connections()
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        self.assertIsNone(self.comparator.conn1)
        self.assertIsNone(self.comparator.conn2)

    def test_compare_schemas_and_data_raise_if_not_initialized(self):
        self.comparator.conn1 = None
        self.comparator.conn2 = None
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._compare_schemas()
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._compare_data()

    @patch('dbchecker.comparator.DatabaseConnector')
    def test_compare_schemas_success_and_failure(self, mock_connector):
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        
        # Create proper schema objects with tables attribute
        mock_schema1 = MagicMock()
        mock_schema1.tables = ['t1', 't2']
        mock_schema2 = MagicMock()
        mock_schema2.tables = ['t1', 't2']
        
        mock_conn1.get_schema.return_value = mock_schema1
        mock_conn2.get_schema.return_value = mock_schema2
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        
        # Create a proper comparison result with table_differences
        mock_comparison_result = MagicMock()
        mock_comparison_result.table_differences = {}
        self.comparator.schema_comparator.compare_schemas = MagicMock(return_value=mock_comparison_result)
        self.comparator.options.verbose = True
        
        result = self.comparator._compare_schemas()
        self.assertIsNotNone(result)
        
        # Simulate exception
        self.comparator.schema_comparator.compare_schemas.side_effect = Exception('fail')
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._compare_schemas()

    @patch('dbchecker.comparator.DatabaseConnector')
    def test_compare_data_success_and_failure(self, mock_connector):
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_conn1.get_table_names.return_value = ['t1', 't2']
        mock_conn2.get_table_names.return_value = ['t1', 't2']
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        self.comparator.options.verbose = True
        self.comparator.options.parallel_tables = False
        self.comparator.data_comparator.compare_all_tables = MagicMock(return_value=MagicMock(table_results={'t1': MagicMock(row_count_db1=1, row_count_db2=1, rows_only_in_db1=[], rows_only_in_db2=[], rows_with_differences=[], uuid_statistics=None)}, total_differences=0))
        result = self.comparator._compare_data()
        self.assertIsNotNone(result)
        # Simulate exception
        self.comparator.data_comparator.compare_all_tables.side_effect = Exception('fail')
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._compare_data()

    def test_compare_data_parallel_success_and_failure(self):
        """Test _compare_data_parallel method with success and failure scenarios"""
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        self.comparator.options.max_workers = 2
        self.comparator.options.verbose = True
        
        # Create mock table comparison result
        mock_table_comparison = MagicMock()
        mock_table_comparison.rows_only_in_db1 = []
        mock_table_comparison.rows_only_in_db2 = []
        mock_table_comparison.rows_with_differences = []
        
        # Mock the data_comparator.compare_table_data method
        self.comparator.data_comparator.compare_table_data = MagicMock(return_value=mock_table_comparison)
        
        # Test successful execution
        with patch('dbchecker.comparator.ThreadPoolExecutor') as mock_executor, \
             patch('dbchecker.comparator.as_completed') as mock_as_completed:
            
            # Create mock future
            mock_future = MagicMock()
            mock_future.result.return_value = mock_table_comparison
            
            # Set up executor context manager
            mock_executor_instance = MagicMock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            mock_executor_instance.submit.return_value = mock_future
            
            # Set up as_completed to return the future
            mock_as_completed.return_value = [mock_future]
            
            result = self.comparator._compare_data_parallel(['t1'])
            
            self.assertIsNotNone(result)
            self.assertEqual(result.total_differences, 0)
            self.assertIn('t1', result.table_results)
        
        # Test exception handling
        with patch('dbchecker.comparator.ThreadPoolExecutor') as mock_executor, \
             patch('dbchecker.comparator.as_completed') as mock_as_completed:
            
            # Create mock future that raises exception
            mock_future_with_exception = MagicMock()
            mock_future_with_exception.result.side_effect = Exception('fail')
            
            mock_executor_instance = MagicMock()
            mock_executor.return_value.__enter__.return_value = mock_executor_instance
            mock_executor_instance.submit.return_value = mock_future_with_exception
            
            mock_as_completed.return_value = [mock_future_with_exception]
            
            with self.assertRaises(DatabaseComparisonError):
                self.comparator._compare_data_parallel(['t1'])

    def test_generate_summary_schema_and_data(self):
        # Minimal mocks for schema_result and data_result
        schema_result = MagicMock(table_differences=['t2'])
        data_result = MagicMock(table_results={'t1': MagicMock(row_count_db1=1, row_count_db2=1, rows_only_in_db1=[], rows_only_in_db2=[], rows_with_differences=[], uuid_statistics=None)}, total_differences=0)
        self.comparator.conn1 = MagicMock()
        self.comparator.conn2 = MagicMock()
        self.comparator.conn1.get_table_names.return_value = ['t1', 't2']
        self.comparator.conn2.get_table_names.return_value = ['t1', 't2']
        summary = self.comparator._generate_summary(schema_result, data_result)
        self.assertIsInstance(summary, ComparisonSummary)

    @patch('dbchecker.comparator.ReportGenerator._generate_json_report', return_value='json')
    @patch('dbchecker.comparator.ReportGenerator._generate_html_report', return_value='html')
    @patch('dbchecker.comparator.ReportGenerator._generate_markdown_report', return_value='md')
    @patch('dbchecker.comparator.ReportGenerator._generate_csv_report', return_value='csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_reports_all_formats_and_failure(self, mock_open_func, mock_csv, mock_md, mock_html, mock_json):
        comparison_result = MagicMock()
        self.comparator.options.output_format = ['json', 'html', 'markdown', 'csv', 'unknown']
        self.comparator.options.verbose = True
        self.comparator.generate_reports(comparison_result, output_dir='.', filename_prefix='test')
        # Simulate exception for JSON
        mock_json.side_effect = Exception('fail')
        self.comparator.generate_reports(comparison_result, output_dir='.', filename_prefix='test')

    def test_get_comparison_statistics(self):
        stats = self.comparator.get_comparison_statistics()
        self.assertIn('database_paths', stats)
        self.assertIn('options', stats)
        self.assertIn('uuid_detection', stats)

    def test_compare_with_schema_only(self):
        """Test comparison with only schema comparison enabled"""
        self.comparator.options.compare_schema = True
        self.comparator.options.compare_data = False
        
        with patch.object(self.comparator, '_validate_configuration'), \
             patch.object(self.comparator, '_initialize_connections'), \
             patch.object(self.comparator, '_cleanup_connections'), \
             patch.object(self.comparator, '_compare_schemas') as mock_schema, \
             patch.object(self.comparator, '_generate_summary') as mock_summary:
            
            mock_schema.return_value = 'schema_result'
            mock_summary.return_value = 'summary'
            
            result = self.comparator.compare()
            
            self.assertEqual(result.schema_comparison, 'schema_result')
            self.assertIsNone(result.data_comparison)
            mock_schema.assert_called_once()

    def test_compare_with_data_only(self):
        """Test comparison with only data comparison enabled"""
        self.comparator.options.compare_schema = False
        self.comparator.options.compare_data = True
        
        with patch.object(self.comparator, '_validate_configuration'), \
             patch.object(self.comparator, '_initialize_connections'), \
             patch.object(self.comparator, '_cleanup_connections'), \
             patch.object(self.comparator, '_compare_data') as mock_data, \
             patch.object(self.comparator, '_generate_summary') as mock_summary:
            
            mock_data.return_value = 'data_result'
            mock_summary.return_value = 'summary'
            
            result = self.comparator.compare()
            
            self.assertIsNone(result.schema_comparison)
            self.assertEqual(result.data_comparison, 'data_result')
            mock_data.assert_called_once()

    def test_compare_data_with_parallel_enabled(self):
        """Test _compare_data when parallel_tables is enabled"""
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_conn1.get_table_names.return_value = ['t1', 't2']
        mock_conn2.get_table_names.return_value = ['t1', 't2']
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        self.comparator.options.parallel_tables = True
        
        with patch.object(self.comparator, '_compare_data_parallel') as mock_parallel:
            mock_parallel.return_value = MagicMock(total_differences=5)
            result = self.comparator._compare_data()
            # Use assertIn since the order of tables from set intersection is not guaranteed
            mock_parallel.assert_called_once()
            call_args = mock_parallel.call_args[0][0]
            self.assertIn('t1', call_args)
            self.assertIn('t2', call_args)
            self.assertEqual(len(call_args), 2)
            self.assertEqual(result.total_differences, 5)

    def test_generate_summary_with_uuid_statistics(self):
        """Test _generate_summary with UUID statistics"""
        # Create mock data with UUID statistics
        mock_uuid_stats = MagicMock()
        mock_uuid_stats.uuid_columns = ['id', 'user_id']
        mock_uuid_stats.total_uuid_values_db1 = 100
        mock_uuid_stats.total_uuid_values_db2 = 95  # Different values to trigger integrity check failure
        
        mock_table_comp = MagicMock()
        mock_table_comp.row_count_db1 = 50
        mock_table_comp.row_count_db2 = 45
        mock_table_comp.rows_only_in_db1 = []
        mock_table_comp.rows_only_in_db2 = []
        mock_table_comp.rows_with_differences = []
        mock_table_comp.uuid_statistics = mock_uuid_stats
        
        data_result = MagicMock()
        data_result.table_results = {'test_table': mock_table_comp}
        data_result.total_differences = 0
        
        self.comparator.conn1 = MagicMock()
        self.comparator.conn2 = MagicMock()
        self.comparator.conn1.get_table_names.return_value = ['test_table']
        self.comparator.conn2.get_table_names.return_value = ['test_table']
        
        summary = self.comparator._generate_summary(None, data_result)
        
        self.assertEqual(summary.total_uuid_columns, 2)
        self.assertEqual(summary.total_uuid_values_db1, 100)
        self.assertEqual(summary.total_uuid_values_db2, 95)
        self.assertFalse(summary.uuid_integrity_check)

    def test_generate_summary_schema_only(self):
        """Test _generate_summary with schema result only"""
        schema_result = MagicMock()
        schema_result.table_differences = {'t1': MagicMock(identical=False)}
        
        self.comparator.conn1 = MagicMock()
        self.comparator.conn2 = MagicMock()
        self.comparator.conn1.get_table_names.return_value = ['t1', 't2']
        self.comparator.conn2.get_table_names.return_value = ['t1', 't2']
        
        summary = self.comparator._generate_summary(schema_result, None)
        
        self.assertEqual(summary.total_tables, 2)
        self.assertEqual(summary.identical_tables, 1)  # 2 total - 1 with differences
        self.assertEqual(summary.tables_with_differences, 1)

    def test_compare_data_sequential_success_and_failure(self):
        """Test _compare_data_sequential method"""
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        self.comparator.conn1 = mock_conn1
        self.comparator.conn2 = mock_conn2
        
        # Test successful execution
        mock_result = MagicMock()
        self.comparator.data_comparator.compare_all_tables = MagicMock(return_value=mock_result)
        
        result = self.comparator._compare_data_sequential(['t1', 't2'])
        self.assertEqual(result, mock_result)
        self.comparator.data_comparator.compare_all_tables.assert_called_once_with(
            mock_conn1, mock_conn2, ['t1', 't2'], self.comparator.options.batch_size
        )
        
        # Test failure when connections not initialized
        self.comparator.conn1 = None
        self.comparator.conn2 = None
        with self.assertRaises(DatabaseComparisonError):
            self.comparator._compare_data_sequential(['t1'])

    def test_compare_data_parallel_uninitialized_connections(self):
        """Test _compare_data_parallel raises error when connections not initialized"""
        self.comparator.conn1 = None
        self.comparator.conn2 = None
        
        with self.assertRaises(DatabaseComparisonError) as context:
            self.comparator._compare_data_parallel(['t1'])
        
        self.assertIn("Database connections not initialized", str(context.exception))

    def test_generate_reports_with_report_generator_methods(self):
        """Test generate_reports with actual ReportGenerator method calls"""
        comparison_result = MagicMock()
        self.comparator.options.output_format = ['json', 'html', 'markdown', 'csv']
        self.comparator.options.verbose = True
        
        # Mock the ReportGenerator.generate_report method
        with patch.object(self.comparator.report_generator, 'generate_report') as mock_generate, \
             patch('builtins.open', new_callable=mock_open) as mock_file:
            
            mock_generate.side_effect = ['json_content', 'html_content', 'md_content', 'csv_content']
            
            self.comparator.generate_reports(comparison_result, output_dir='.', filename_prefix='test')
            
            # Verify generate_report was called with correct parameters
            expected_calls = [
                call(comparison_result, "json"),
                call(comparison_result, "html"),
                call(comparison_result, "markdown"),
                call(comparison_result, "csv")
            ]
            mock_generate.assert_has_calls(expected_calls, any_order=False)

if __name__ == '__main__':
    unittest.main()
