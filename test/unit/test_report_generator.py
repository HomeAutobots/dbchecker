"""
Comprehensive unit tests for the ReportGenerator class.
"""

import unittest
import json
from datetime import datetime
from dbchecker.report_generator import ReportGenerator
from dbchecker.models import (
    ComparisonResult, ComparisonSummary, SchemaComparisonResult, DataComparisonResult,
    TableComparisonResult, TableDataComparison, FieldDifference, RowDifference,
    UUIDStatistics, Report
)


class TestReportGenerator(unittest.TestCase):
    """Test cases for ReportGenerator class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.generator = ReportGenerator()
        
        # Create test comparison result
        self.summary = ComparisonSummary(
            total_tables=2,
            identical_tables=1,
            tables_with_differences=1,
            total_rows_compared=100,
            total_differences_found=5,
            total_uuid_columns=2,
            total_uuid_values_db1=50,
            total_uuid_values_db2=50,
            uuid_integrity_check=True
        )
        
        # Create schema comparison result
        self.schema_result = SchemaComparisonResult(
            identical=False,
            missing_in_db1=["table_c"],
            missing_in_db2=["table_d"],
            table_differences={
                "users": TableComparisonResult(
                    table_name="users",
                    identical=False,
                    missing_columns_db1=["status"],
                    missing_columns_db2=["role"],
                    column_differences=[
                        FieldDifference("age", "INTEGER", "TEXT")
                    ]
                )
            }
        )
        
        # Create data comparison result
        uuid_stats = UUIDStatistics(
            uuid_columns=["id", "user_id"],
            total_uuid_values_db1=25,
            total_uuid_values_db2=25,
            unique_uuid_values_db1=25,
            unique_uuid_values_db2=25,
            uuid_value_differences=0,
            detected_patterns={"id": "standard_uuid"},
            normalized_match_count=20
        )
        
        row_diff = RowDifference(
            row_identifier="user_1",
            differences=[
                FieldDifference("name", "John", "Jane"),
                FieldDifference("email", "john@test.com", "jane@test.com")
            ]
        )
        
        table_data_comp = TableDataComparison(
            table_name="users",
            row_count_db1=50,
            row_count_db2=50,
            matching_rows=47,
            rows_only_in_db1=[{"id": "1", "name": "Only in DB1"}],
            rows_only_in_db2=[{"id": "2", "name": "Only in DB2"}],
            rows_with_differences=[row_diff],
            uuid_statistics=uuid_stats
        )
        
        self.data_result = DataComparisonResult(
            table_results={"users": table_data_comp},
            total_differences=5
        )
        
        self.comparison_result = ComparisonResult(
            schema_comparison=self.schema_result,
            data_comparison=self.data_result,
            summary=self.summary,
            timestamp=datetime.now()
        )
    
    def test_generate_json_report(self):
        """Test generating JSON report"""
        report = self.generator.generate_report(self.comparison_result, 'json')
        
        self.assertIsInstance(report, str)
        
        # Parse JSON to verify structure
        data = json.loads(report)
        
        self.assertIn('summary', data)
        self.assertIn('schema_comparison', data)
        self.assertIn('data_comparison', data)
        self.assertIn('timestamp', data)
        
        # Check summary data
        summary = data['summary']
        self.assertEqual(summary['total_tables'], 2)
        self.assertEqual(summary['identical_tables'], 1)
        self.assertEqual(summary['total_differences_found'], 5)
    
    def test_generate_html_report(self):
        """Test generating HTML report"""
        report = self.generator.generate_report(self.comparison_result, 'html')
        
        self.assertIsInstance(report, str)
        
        # Check for HTML structure
        self.assertIn('<html>', report)
        self.assertIn('<head>', report)
        self.assertIn('<body>', report)
        self.assertIn('</html>', report)
        
        # Check for CSS styles
        self.assertIn('<style>', report)
        self.assertIn('</style>', report)
        
        # Check for summary information
        self.assertIn('Database Comparison Report', report)
        self.assertIn('Total Tables', report)
        self.assertIn('Total Differences Found', report)
    
    def test_generate_markdown_report(self):
        """Test generating Markdown report"""
        report = self.generator.generate_report(self.comparison_result, 'markdown')
        
        self.assertIsInstance(report, str)
        
        # Check for Markdown structure
        self.assertIn('# Database Comparison Report', report)
        self.assertIn('## Summary', report)
        self.assertIn('- **Total Tables:**', report)
        self.assertIn('- **Total Differences Found:**', report)
    
    def test_generate_csv_report(self):
        """Test generating CSV report"""
        report = self.generator.generate_report(self.comparison_result, 'csv')
        
        self.assertIsInstance(report, str)
        
        # Check for CSV headers
        lines = report.split('\n')
        self.assertTrue(len(lines) > 0)
        
        # Should have header row
        header = lines[0]
        self.assertIn('table_name', header.lower())
    
    def test_generate_report_unsupported_format(self):
        """Test generating report with unsupported format"""
        with self.assertRaises(ValueError):
            self.generator.generate_report(self.comparison_result, 'xml')
    
    def test_json_report_schema_differences(self):
        """Test JSON report includes schema differences correctly"""
        report = self.generator.generate_report(self.comparison_result, 'json')
        data = json.loads(report)
        
        schema = data['schema_comparison']
        self.assertFalse(schema['identical'])
        self.assertEqual(schema['missing_in_db1'], ["table_c"])
        self.assertEqual(schema['missing_in_db2'], ["table_d"])
        
        # Check table differences
        table_diffs = schema['details']['table_differences']
        self.assertEqual(len(table_diffs), 1)
        
        users_diff = table_diffs[0]
        self.assertEqual(users_diff['table_name'], 'users')
        self.assertFalse(users_diff['identical'])
        self.assertEqual(users_diff['columns_only_in_db1'], ['status'])
        self.assertEqual(users_diff['columns_only_in_db2'], ['role'])
    
    def test_json_report_data_differences(self):
        """Test JSON report includes data differences correctly"""
        report = self.generator.generate_report(self.comparison_result, 'json')
        data = json.loads(report)
        
        data_comp = data['data_comparison']
        self.assertEqual(data_comp['tables_compared'], 1)
        
        table_details = data_comp['table_details']
        self.assertEqual(len(table_details), 1)
        
        users_data = table_details[0]
        self.assertEqual(users_data['table_name'], 'users')
        self.assertEqual(users_data['row_count_db1'], 50)
        self.assertEqual(users_data['row_count_db2'], 50)
        self.assertEqual(users_data['matching_rows'], 47)
        self.assertEqual(users_data['rows_only_in_db1'], 1)
        self.assertEqual(users_data['rows_only_in_db2'], 1)
        self.assertEqual(users_data['rows_with_differences'], 1)
    
    def test_json_report_uuid_statistics(self):
        """Test JSON report includes UUID statistics correctly"""
        report = self.generator.generate_report(self.comparison_result, 'json')
        data = json.loads(report)
        
        table_details = data['data_comparison']['table_details'][0]
        uuid_stats = table_details['uuid_statistics']
        
        self.assertEqual(uuid_stats['uuid_columns'], ["id", "user_id"])
        self.assertEqual(uuid_stats['total_uuid_values_db1'], 25)
        self.assertEqual(uuid_stats['total_uuid_values_db2'], 25)
        self.assertEqual(uuid_stats['uuid_value_differences'], 0)
        self.assertEqual(uuid_stats['normalized_match_count'], 20)
    
    def test_html_report_styling(self):
        """Test HTML report includes proper styling"""
        report = self.generator.generate_report(self.comparison_result, 'html')
        
        # Check for CSS classes
        self.assertIn('class="metric"', report)
        self.assertIn('class="metric-value"', report)
        self.assertIn('class="field-name"', report)
        
        # Check for responsive design
        self.assertIn('margin: 0 auto', report)
        self.assertIn('max-width:', report)
    
    def test_markdown_report_structure(self):
        """Test Markdown report has proper structure"""
        report = self.generator.generate_report(self.comparison_result, 'markdown')
        
        # Check for proper markdown headers
        self.assertIn('## Schema Differences', report)
        self.assertIn('## Data Differences', report)
        
        # Check for table format
        self.assertIn('| Column |', report)
        self.assertIn('|--------|', report)
    
    def test_report_with_no_differences(self):
        """Test report generation when there are no differences"""
        # Create comparison result with no differences
        no_diff_summary = ComparisonSummary(
            total_tables=2,
            identical_tables=2,
            tables_with_differences=0,
            total_rows_compared=100,
            total_differences_found=0
        )
        
        no_diff_schema = SchemaComparisonResult(
            identical=True,
            missing_in_db1=[],
            missing_in_db2=[],
            table_differences={}
        )
        
        no_diff_data = DataComparisonResult(
            table_results={},
            total_differences=0
        )
        
        no_diff_result = ComparisonResult(
            schema_comparison=no_diff_schema,
            data_comparison=no_diff_data,
            summary=no_diff_summary,
            timestamp=datetime.now()
        )
        
        # Test each format
        for format_type in ['json', 'html', 'markdown']:
            report = self.generator.generate_report(no_diff_result, format_type)
            self.assertIsInstance(report, str)
            
            if format_type == 'json':
                data = json.loads(report)
                self.assertEqual(data['summary']['total_differences_found'], 0)
            elif format_type == 'html':
                self.assertIn('No differences found', report)
            elif format_type == 'markdown':
                self.assertIn('No differences found', report)
    
    def test_report_with_large_differences(self):
        """Test report generation with large number of differences"""
        # Create many row differences
        many_row_diffs = []
        for i in range(150):  # More than default max
            row_diff = RowDifference(
                row_identifier=f"row_{i}",
                differences=[FieldDifference("field", f"value1_{i}", f"value2_{i}")]
            )
            many_row_diffs.append(row_diff)
        
        large_table_comp = TableDataComparison(
            table_name="large_table",
            row_count_db1=1000,
            row_count_db2=1000,
            matching_rows=850,
            rows_only_in_db1=[],
            rows_only_in_db2=[],
            rows_with_differences=many_row_diffs
        )
        
        large_data_result = DataComparisonResult(
            table_results={"large_table": large_table_comp},
            total_differences=150
        )
        
        large_summary = ComparisonSummary(
            total_tables=1,
            identical_tables=0,
            tables_with_differences=1,
            total_rows_compared=1000,
            total_differences_found=150
        )
        
        large_result = ComparisonResult(
            schema_comparison=None,
            data_comparison=large_data_result,
            summary=large_summary,
            timestamp=datetime.now()
        )
        
        # Test that report handles large data gracefully
        report = self.generator.generate_report(large_result, 'json')
        self.assertIsInstance(report, str)
        
        # Verify truncation or pagination is handled
        data = json.loads(report)
        table_details = data['data_comparison']['table_details'][0]
        
        # Should either limit the differences or handle them appropriately
        self.assertIsInstance(table_details['differences'], list)
    
    def test_report_with_special_characters(self):
        """Test report generation with special characters in data"""
        # Create data with special characters
        special_row_diff = RowDifference(
            row_identifier="special_row",
            differences=[
                FieldDifference("name", "Test \"quoted\" text", "Test 'apostrophe' text"),
                FieldDifference("description", "Line 1\nLine 2", "Line 1\rLine 2"),
                FieldDifference("unicode", "Test café", "Test naïve"),
                FieldDifference("html", "<script>alert('test')</script>", "&lt;safe&gt;")
            ]
        )
        
        special_table_comp = TableDataComparison(
            table_name="special_table",
            row_count_db1=1,
            row_count_db2=1,
            matching_rows=0,
            rows_only_in_db1=[],
            rows_only_in_db2=[],
            rows_with_differences=[special_row_diff]
        )
        
        special_data_result = DataComparisonResult(
            table_results={"special_table": special_table_comp},
            total_differences=1
        )
        
        special_result = ComparisonResult(
            schema_comparison=None,
            data_comparison=special_data_result,
            summary=self.summary,
            timestamp=datetime.now()
        )
        
        # Test JSON handling of special characters
        json_report = self.generator.generate_report(special_result, 'json')
        json_data = json.loads(json_report)  # Should not raise JSON decode error
        
        # Test HTML escaping
        html_report = self.generator.generate_report(special_result, 'html')
        self.assertNotIn('<script>', html_report)  # Should be escaped
        
        # Test Markdown handling
        md_report = self.generator.generate_report(special_result, 'markdown')
        self.assertIsInstance(md_report, str)
    
    def test_report_timestamp_formatting(self):
        """Test that timestamps are properly formatted in reports"""
        test_time = datetime(2024, 1, 15, 14, 30, 45)
        
        timed_result = ComparisonResult(
            schema_comparison=None,
            data_comparison=None,
            summary=self.summary,
            timestamp=test_time
        )
        
        json_report = self.generator.generate_report(timed_result, 'json')
        data = json.loads(json_report)
        
        # Check timestamp is included and properly formatted
        self.assertIn('timestamp', data)
        self.assertIsInstance(data['timestamp'], str)
        
        html_report = self.generator.generate_report(timed_result, 'html')
        self.assertIn('2024', html_report)  # Year should appear somewhere
    
    def test_generate_multiple_formats(self):
        """Test generating reports in multiple formats"""
        formats = ['json', 'html', 'markdown', 'csv']
        
        for format_type in formats:
            with self.subTest(format=format_type):
                report = self.generator.generate_report(self.comparison_result, format_type)
                
                self.assertIsInstance(report, str)
                self.assertGreater(len(report), 0)


if __name__ == '__main__':
    unittest.main()
