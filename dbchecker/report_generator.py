"""Report generator for database comparison results"""
from typing import Dict, Any, List, Union
import json
from datetime import datetime
import csv
import io

from .models import ComparisonResult, TableDataComparison, RowDifference


class ReportGenerator:
    """Generates reports from comparison results in multiple formats"""
    
    def __init__(self):
        self.supported_formats = ['json', 'html', 'markdown', 'csv']
    
    def generate_report(self, result: ComparisonResult, format: str = 'json') -> str:
        """Generate a report in the specified format"""
        if format not in self.supported_formats:
            raise ValueError(f"Unsupported format: {format}. Supported: {self.supported_formats}")
        
        if format == 'json':
            return self._generate_json_report(result)
        elif format == 'html':
            return self._generate_html_report(result)
        elif format == 'markdown':
            return self._generate_markdown_report(result)
        elif format == 'csv':
            return self._generate_csv_report(result)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def save_report(self, result: ComparisonResult, filepath: str, format: str = 'json'):
        """Save a report to file"""
        report_content = self.generate_report(result, format)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
    
    def _generate_json_report(self, result: ComparisonResult) -> str:
        """Generate JSON report with enhanced difference details"""
        # Convert result to dictionary format
        report_data = {
            'timestamp': result.timestamp.isoformat(),
            'summary': {
                'total_tables': result.summary.total_tables,
                'identical_tables': result.summary.identical_tables,
                'tables_with_differences': result.summary.tables_with_differences,
                'total_rows_compared': result.summary.total_rows_compared,
                'total_differences_found': result.summary.total_differences_found
            }
        }
        
        # Add schema comparison if available
        if result.schema_comparison:
            tables_with_differences = [name for name, diff in result.schema_comparison.table_differences.items() if not diff.identical]
            
            report_data['schema_comparison'] = {
                'schema_identical': result.schema_comparison.identical,
                'tables_missing_in_db1': len(result.schema_comparison.missing_in_db1),
                'tables_missing_in_db2': len(result.schema_comparison.missing_in_db2),
                'tables_with_differences': len(tables_with_differences),
                'details': {
                    'missing_in_db1': result.schema_comparison.missing_in_db1,
                    'missing_in_db2': result.schema_comparison.missing_in_db2,
                    'table_differences': [
                        {
                            'table_name': table_name,
                            'identical': table_diff.identical,
                            'columns_only_in_db1': table_diff.missing_columns_db1,
                            'columns_only_in_db2': table_diff.missing_columns_db2,
                            'different_columns': [
                                {
                                    'column_name': col_diff.field_name,
                                    'db1_definition': col_diff.value_db1,
                                    'db2_definition': col_diff.value_db2
                                }
                                for col_diff in table_diff.column_differences
                            ]
                        }
                        for table_name, table_diff in result.schema_comparison.table_differences.items()
                        if not table_diff.identical
                    ]
                }
            }
        
        # Add data comparison if available
        if result.data_comparison:
            report_data['data_comparison'] = {
                'tables_compared': len(result.data_comparison.table_results),
                'table_details': []
            }
            
            for table_name, table_comp in result.data_comparison.table_results.items():
                table_detail = {
                    'table_name': table_comp.table_name,
                    'row_count_db1': table_comp.row_count_db1,
                    'row_count_db2': table_comp.row_count_db2,
                    'matching_rows': table_comp.matching_rows,
                    'rows_only_in_db1': len(table_comp.rows_only_in_db1),
                    'rows_only_in_db2': len(table_comp.rows_only_in_db2),
                    'rows_with_differences': len(table_comp.rows_with_differences),
                    'differences': []
                }
                
                # Add detailed row differences
                for row_diff in table_comp.rows_with_differences:
                    diff_detail = {
                        'row_identifier': row_diff.row_identifier,
                        'field_differences': [
                            {
                                'field_name': field_diff.field_name,
                                'db1_value': field_diff.value_db1,
                                'db2_value': field_diff.value_db2
                            }
                            for field_diff in row_diff.differences
                        ]
                    }
                    table_detail['differences'].append(diff_detail)
                
                # Add rows unique to each database
                table_detail['rows_only_in_db1_details'] = [
                    {
                        'row_identifier': row.get('_row_id', 'unknown'),
                        'data': row
                    }
                    for row in table_comp.rows_only_in_db1
                ]
                
                table_detail['rows_only_in_db2_details'] = [
                    {
                        'row_identifier': row.get('_row_id', 'unknown'),
                        'data': row
                    }
                    for row in table_comp.rows_only_in_db2
                ]
                
                report_data['data_comparison']['table_details'].append(table_detail)
        
        return json.dumps(report_data, indent=2, default=str)
    
    def _generate_markdown_report(self, result: ComparisonResult) -> str:
        """Generate Markdown report with enhanced difference details"""
        md = []
        md.append("# Database Comparison Report")
        md.append("")
        md.append(f"**Generated:** {result.timestamp.isoformat()}")
        md.append("")
        
        # Summary
        md.append("## Summary")
        md.append(f"- **Total Tables:** {result.summary.total_tables}")
        md.append(f"- **Identical Tables:** {result.summary.identical_tables}")
        md.append(f"- **Tables with Differences:** {result.summary.tables_with_differences}")
        md.append(f"- **Total Rows Compared:** {result.summary.total_rows_compared}")
        md.append(f"- **Total Differences Found:** {result.summary.total_differences_found}")
        md.append("")
        
        # Schema differences
        if result.schema_comparison:
            schema_differences = [table_diff for table_diff in result.schema_comparison.table_differences.values() if not table_diff.identical]
            if schema_differences:
                md.append("## Schema Differences")
                md.append("")
                for table_diff in schema_differences:
                    md.append(f"### Table: {table_diff.table_name}")
                    md.append("")
                    
                    if table_diff.missing_columns_db1:
                        md.append("**Columns only in Database 1:**")
                        for col in table_diff.missing_columns_db1:
                            md.append(f"- {col}")
                        md.append("")
                    
                    if table_diff.missing_columns_db2:
                        md.append("**Columns only in Database 2:**")
                        for col in table_diff.missing_columns_db2:
                            md.append(f"- {col}")
                        md.append("")
                    
                    if table_diff.column_differences:
                        md.append("**Column Definition Differences:**")
                        md.append("| Column | Database 1 | Database 2 |")
                        md.append("|--------|------------|------------|")
                        for col_diff in table_diff.column_differences:
                            md.append(f"| {col_diff.field_name} | {col_diff.value_db1} | {col_diff.value_db2} |")
                        md.append("")
        
        # Data differences
        if result.data_comparison:
            has_differences = any(len(table.rows_with_differences) > 0 or len(table.rows_only_in_db1) > 0 or len(table.rows_only_in_db2) > 0 
                                for table in result.data_comparison.table_results.values())
            
            if has_differences:
                md.append("## Data Differences")
                md.append("")
                
                for table_name, table_comp in result.data_comparison.table_results.items():
                    if (len(table_comp.rows_with_differences) > 0 or 
                        len(table_comp.rows_only_in_db1) > 0 or 
                        len(table_comp.rows_only_in_db2) > 0):
                        
                        md.append(f"### Table: {table_comp.table_name}")
                        md.append("")
                        md.append(f"- **Row Count DB1:** {table_comp.row_count_db1}")
                        md.append(f"- **Row Count DB2:** {table_comp.row_count_db2}")
                        md.append(f"- **Matching Rows:** {table_comp.matching_rows}")
                        md.append(f"- **Rows Only in DB1:** {len(table_comp.rows_only_in_db1)}")
                        md.append(f"- **Rows Only in DB2:** {len(table_comp.rows_only_in_db2)}")
                        md.append(f"- **Rows with Differences:** {len(table_comp.rows_with_differences)}")
                        md.append("")
                        
                        # Show detailed differences
                        if table_comp.rows_with_differences:
                            md.append("#### Row Differences")
                            md.append("")
                            for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                                md.append(f"**Difference #{i} - Row: {row_diff.row_identifier}**")
                                md.append("")
                                md.append("| Field | Database 1 | Database 2 |")
                                md.append("|-------|------------|------------|")
                                
                                for field_diff in row_diff.differences:
                                    val1 = str(field_diff.value_db1).replace('|', '\\|')
                                    val2 = str(field_diff.value_db2).replace('|', '\\|')
                                    md.append(f"| {field_diff.field_name} | {val1} | {val2} |")
                                md.append("")
                        
                        # Show rows unique to each database
                        if table_comp.rows_only_in_db1:
                            md.append("#### Rows Only in Database 1")
                            md.append("")
                            for i, row in enumerate(table_comp.rows_only_in_db1[:5], 1):  # Limit to first 5
                                md.append(f"- Row {i}: {dict(row)}")
                            if len(table_comp.rows_only_in_db1) > 5:
                                md.append(f"- ... and {len(table_comp.rows_only_in_db1) - 5} more")
                            md.append("")
                        
                        if table_comp.rows_only_in_db2:
                            md.append("#### Rows Only in Database 2")
                            md.append("")
                            for i, row in enumerate(table_comp.rows_only_in_db2[:5], 1):  # Limit to first 5
                                md.append(f"- Row {i}: {dict(row)}")
                            if len(table_comp.rows_only_in_db2) > 5:
                                md.append(f"- ... and {len(table_comp.rows_only_in_db2) - 5} more")
                            md.append("")
            else:
                md.append("## Result")
                md.append("✅ No data differences found between the databases!")
                md.append("")
        
        return "\n".join(md)
    
    def _generate_html_report(self, result: ComparisonResult) -> str:
        """Generate HTML report with enhanced difference details"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Database Comparison Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .summary {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .difference {{ background-color: #fff3cd; padding: 15px; margin: 15px 0; border-left: 4px solid #ffc107; border-radius: 4px; }}
        .identical {{ background-color: #d4edda; padding: 15px; margin: 15px 0; border-left: 4px solid #28a745; border-radius: 4px; }}
        .table-section {{ margin: 20px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .field-name {{ font-weight: bold; color: #495057; }}
        .value-diff {{ background-color: #ffebee; }}
        .metric {{ display: inline-block; margin: 10px 15px 10px 0; }}
        .metric-value {{ font-weight: bold; color: #0056b3; }}
        h2 {{ color: #343a40; border-bottom: 2px solid #dee2e6; padding-bottom: 10px; }}
        h3 {{ color: #495057; }}
        h4 {{ color: #6c757d; }}
    </style>
</head>
<body>
    <h1>🔍 Database Comparison Report</h1>
    
    <div class="summary">
        <h2>📊 Summary</h2>
        <p><strong>Generated:</strong> {result.timestamp.isoformat()}</p>
        <div class="metric">
            <span>Total Tables:</span> 
            <span class="metric-value">{result.summary.total_tables}</span>
        </div>
        <div class="metric">
            <span>Identical Tables:</span> 
            <span class="metric-value">{result.summary.identical_tables}</span>
        </div>
        <div class="metric">
            <span>Tables with Differences:</span> 
            <span class="metric-value">{result.summary.tables_with_differences}</span>
        </div>
        <div class="metric">
            <span>Total Rows Compared:</span> 
            <span class="metric-value">{result.summary.total_rows_compared}</span>
        </div>
        <div class="metric">
            <span>Total Differences Found:</span> 
            <span class="metric-value">{result.summary.total_differences_found}</span>
        </div>
    </div>
"""
        
        # Add schema differences
        if result.schema_comparison:
            schema_differences = [table_diff for table_diff in result.schema_comparison.table_differences.values() if not table_diff.identical]
            if schema_differences:
                html += "<h2>🏗️ Schema Differences</h2>"
                for table_diff in schema_differences:
                    html += f'<div class="table-section">'
                    html += f"<h3>Table: {table_diff.table_name}</h3>"
                    
                    if table_diff.column_differences:
                        html += "<h4>Column Definition Differences</h4>"
                        html += "<table>"
                        html += "<tr><th>Column</th><th>Database 1</th><th>Database 2</th></tr>"
                        for col_diff in table_diff.column_differences:
                            html += f"<tr><td class='field-name'>{col_diff.field_name}</td><td>{col_diff.value_db1}</td><td>{col_diff.value_db2}</td></tr>"
                        html += "</table>"
                    
                    html += "</div>"
        
        # Add data differences
        if result.data_comparison:
            has_differences = any(len(table.rows_with_differences) > 0 or len(table.rows_only_in_db1) > 0 or len(table.rows_only_in_db2) > 0 
                                for table in result.data_comparison.table_results.values())
            
            if has_differences:
                html += "<h2>📊 Data Differences</h2>"
                
                for table_name, table_comp in result.data_comparison.table_results.items():
                    if (len(table_comp.rows_with_differences) > 0 or 
                        len(table_comp.rows_only_in_db1) > 0 or 
                        len(table_comp.rows_only_in_db2) > 0):
                        
                        html += f'<div class="table-section">'
                        html += f"<h3>Table: {table_comp.table_name}</h3>"
                        
                        # Table metrics
                        html += f"""
                        <div class="metric">Row Count DB1: <span class="metric-value">{table_comp.row_count_db1}</span></div>
                        <div class="metric">Row Count DB2: <span class="metric-value">{table_comp.row_count_db2}</span></div>
                        <div class="metric">Matching Rows: <span class="metric-value">{table_comp.matching_rows}</span></div>
                        <div class="metric">Rows Only in DB1: <span class="metric-value">{len(table_comp.rows_only_in_db1)}</span></div>
                        <div class="metric">Rows Only in DB2: <span class="metric-value">{len(table_comp.rows_only_in_db2)}</span></div>
                        <div class="metric">Rows with Differences: <span class="metric-value">{len(table_comp.rows_with_differences)}</span></div>
                        """
                        
                        # Show detailed row differences
                        if table_comp.rows_with_differences:
                            html += "<h4>Row Differences</h4>"
                            for i, row_diff in enumerate(table_comp.rows_with_differences, 1):
                                html += f'<div class="difference">'
                                html += f"<h5>Difference #{i} - Row: {row_diff.row_identifier}</h5>"
                                html += "<table>"
                                html += "<tr><th>Field</th><th>Database 1</th><th>Database 2</th></tr>"
                                
                                for field_diff in row_diff.differences:
                                    html += f"<tr><td class='field-name'>{field_diff.field_name}</td><td class='value-diff'>{field_diff.value_db1}</td><td class='value-diff'>{field_diff.value_db2}</td></tr>"
                                
                                html += "</table></div>"
                        
                        # Show rows only in DB1
                        if table_comp.rows_only_in_db1:
                            html += "<h4>Rows Only in Database 1</h4>"
                            for i, row in enumerate(table_comp.rows_only_in_db1, 1):
                                html += f'<div class="difference">'
                                html += f"<h5>Row #{i}</h5>"
                                html += "<table>"
                                html += "<tr><th>Field</th><th>Value</th></tr>"
                                
                                for field, value in row.items():
                                    html += f"<tr><td class='field-name'>{field}</td><td>{value}</td></tr>"
                                
                                html += "</table></div>"
                        
                        # Show rows only in DB2
                        if table_comp.rows_only_in_db2:
                            html += "<h4>Rows Only in Database 2</h4>"
                            for i, row in enumerate(table_comp.rows_only_in_db2, 1):
                                html += f'<div class="difference">'
                                html += f"<h5>Row #{i}</h5>"
                                html += "<table>"
                                html += "<tr><th>Field</th><th>Value</th></tr>"
                                
                                for field, value in row.items():
                                    html += f"<tr><td class='field-name'>{field}</td><td>{value}</td></tr>"
                                
                                html += "</table></div>"
                        
                        html += "</div>"
            else:
                html += '<div class="identical"><h2>✅ Result</h2><p>No data differences found between the databases!</p></div>'
        
        html += "</body></html>"
        return html
    
    def _generate_csv_report(self, result: ComparisonResult) -> str:
        """Generate CSV report of differences"""
        lines = []
        lines.append("Type,Table,Row_Identifier,Field_Name,Database1_Value,Database2_Value")
        
        # Add data differences
        if result.data_comparison:
            for table_name, table_comp in result.data_comparison.table_results.items():
                # Row differences
                for row_diff in table_comp.rows_with_differences:
                    for field_diff in row_diff.differences:
                        val1 = str(field_diff.value_db1).replace(',', ';').replace('\n', ' ')
                        val2 = str(field_diff.value_db2).replace(',', ';').replace('\n', ' ')
                        lines.append(f"Row Difference,{table_comp.table_name},{row_diff.row_identifier},{field_diff.field_name},{val1},{val2}")
                
                # Rows only in DB1
                for i, row in enumerate(table_comp.rows_only_in_db1, 1):
                    lines.append(f"Row Only in DB1,{table_comp.table_name},Row_{i},,,")
                
                # Rows only in DB2
                for i, row in enumerate(table_comp.rows_only_in_db2, 1):
                    lines.append(f"Row Only in DB2,{table_comp.table_name},Row_{i},,,")
        
        if len(lines) == 1:  # Only header
            lines.append("No differences found,,,,,")
        
        return "\n".join(lines)
