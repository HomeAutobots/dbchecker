"""
Report generator module for creating comparison reports in various formats.
"""

import json
from datetime import datetime
from typing import Dict, Any, List
from .models import ComparisonResult, Report
from .exceptions import DatabaseComparisonError


class ReportGenerator:
    """Generates reports from comparison results in multiple formats"""
    
    def __init__(self):
        """Initialize report generator"""
        pass
    
    def generate_report(self, comparison_result: ComparisonResult) -> Report:
        """Generate a basic report object"""
        return Report(
            comparison_result=comparison_result,
            format_type="basic",
            content=self._generate_summary_text(comparison_result)
        )
    
    def to_json(self, comparison_result: ComparisonResult) -> str:
        """Generate JSON format report"""
        try:
            # Convert dataclasses to dictionaries
            report_data = {
                "timestamp": comparison_result.timestamp.isoformat(),
                "summary": {
                    "total_tables": comparison_result.summary.total_tables,
                    "identical_tables": comparison_result.summary.identical_tables,
                    "tables_with_differences": comparison_result.summary.tables_with_differences,
                    "total_rows_compared": comparison_result.summary.total_rows_compared,
                    "total_differences_found": comparison_result.summary.total_differences_found
                },
                "schema_comparison": {
                    "identical": comparison_result.schema_comparison.identical,
                    "missing_in_db1": comparison_result.schema_comparison.missing_in_db1,
                    "missing_in_db2": comparison_result.schema_comparison.missing_in_db2,
                    "table_differences": {}
                },
                "data_comparison": {
                    "total_differences": comparison_result.data_comparison.total_differences,
                    "table_results": {}
                }
            }
            
            # Add schema differences
            for table_name, table_diff in comparison_result.schema_comparison.table_differences.items():
                report_data["schema_comparison"]["table_differences"][table_name] = {
                    "identical": table_diff.identical,
                    "missing_columns_db1": table_diff.missing_columns_db1,
                    "missing_columns_db2": table_diff.missing_columns_db2,
                    "column_differences": [
                        {
                            "field_name": diff.field_name,
                            "value_db1": diff.value_db1,
                            "value_db2": diff.value_db2
                        }
                        for diff in table_diff.column_differences
                    ]
                }
            
            # Add data differences
            for table_name, table_data in comparison_result.data_comparison.table_results.items():
                report_data["data_comparison"]["table_results"][table_name] = {
                    "row_count_db1": table_data.row_count_db1,
                    "row_count_db2": table_data.row_count_db2,
                    "matching_rows": table_data.matching_rows,
                    "rows_only_in_db1_count": len(table_data.rows_only_in_db1),
                    "rows_only_in_db2_count": len(table_data.rows_only_in_db2),
                    "rows_with_differences_count": len(table_data.rows_with_differences),
                    "sample_differences": [
                        {
                            "row_identifier": row_diff.row_identifier,
                            "differences": [
                                {
                                    "field_name": diff.field_name,
                                    "value_db1": diff.value_db1,
                                    "value_db2": diff.value_db2
                                }
                                for diff in row_diff.differences[:5]  # First 5 differences
                            ]
                        }
                        for row_diff in table_data.rows_with_differences[:10]  # First 10 rows
                    ]
                }
            
            return json.dumps(report_data, indent=2, default=str)
        except Exception as e:
            raise DatabaseComparisonError(f"Failed to generate JSON report: {e}")
    
    def to_html(self, comparison_result: ComparisonResult) -> str:
        """Generate HTML format report"""
        try:
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Database Comparison Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; }}
        .summary {{ margin: 20px 0; }}
        .section {{ margin: 20px 0; }}
        .table {{ border-collapse: collapse; width: 100%; }}
        .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .table th {{ background-color: #f2f2f2; }}
        .identical {{ color: green; }}
        .different {{ color: red; }}
        .warning {{ color: orange; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Database Comparison Report</h1>
        <p><strong>Generated:</strong> {comparison_result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary">
        <h2>Summary</h2>
        <ul>
            <li><strong>Total Tables:</strong> {comparison_result.summary.total_tables}</li>
            <li><strong>Identical Tables:</strong> <span class="identical">{comparison_result.summary.identical_tables}</span></li>
            <li><strong>Tables with Differences:</strong> <span class="different">{comparison_result.summary.tables_with_differences}</span></li>
            <li><strong>Total Rows Compared:</strong> {comparison_result.summary.total_rows_compared}</li>
            <li><strong>Total Differences Found:</strong> <span class="different">{comparison_result.summary.total_differences_found}</span></li>
        </ul>
    </div>
    
    <div class="section">
        <h2>Schema Comparison</h2>
        <p><strong>Schema Identical:</strong> <span class="{'identical' if comparison_result.schema_comparison.identical else 'different'}">{comparison_result.schema_comparison.identical}</span></p>
        
        {self._generate_missing_tables_html(comparison_result.schema_comparison)}
        {self._generate_schema_differences_html(comparison_result.schema_comparison)}
    </div>
    
    <div class="section">
        <h2>Data Comparison</h2>
        {self._generate_data_differences_html(comparison_result.data_comparison)}
    </div>
    
</body>
</html>
"""
            return html_content
        except Exception as e:
            raise DatabaseComparisonError(f"Failed to generate HTML report: {e}")
    
    def to_markdown(self, comparison_result: ComparisonResult) -> str:
        """Generate Markdown format report"""
        try:
            md_content = f"""# Database Comparison Report

**Generated:** {comparison_result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

## Summary

- **Total Tables:** {comparison_result.summary.total_tables}
- **Identical Tables:** {comparison_result.summary.identical_tables}
- **Tables with Differences:** {comparison_result.summary.tables_with_differences}
- **Total Rows Compared:** {comparison_result.summary.total_rows_compared}
- **Total Differences Found:** {comparison_result.summary.total_differences_found}

## Schema Comparison

**Schema Identical:** {comparison_result.schema_comparison.identical}

{self._generate_missing_tables_markdown(comparison_result.schema_comparison)}

{self._generate_schema_differences_markdown(comparison_result.schema_comparison)}

## Data Comparison

{self._generate_data_differences_markdown(comparison_result.data_comparison)}
"""
            return md_content
        except Exception as e:
            raise DatabaseComparisonError(f"Failed to generate Markdown report: {e}")
    
    def to_csv(self, comparison_result: ComparisonResult) -> str:
        """Generate CSV format report for data differences"""
        try:
            csv_lines = []
            csv_lines.append("Table,Type,Row_Identifier,Field,Value_DB1,Value_DB2")
            
            # Add data differences
            for table_name, table_data in comparison_result.data_comparison.table_results.items():
                # Rows only in DB1
                for row in table_data.rows_only_in_db1[:10]:  # Limit to first 10
                    row_id = str(row.get('id', 'unknown'))
                    csv_lines.append(f"{table_name},MISSING_IN_DB2,{row_id},,PRESENT,MISSING")
                
                # Rows only in DB2
                for row in table_data.rows_only_in_db2[:10]:  # Limit to first 10
                    row_id = str(row.get('id', 'unknown'))
                    csv_lines.append(f"{table_name},MISSING_IN_DB1,{row_id},,MISSING,PRESENT")
                
                # Rows with differences
                for row_diff in table_data.rows_with_differences[:50]:  # Limit to first 50
                    for field_diff in row_diff.differences:
                        csv_lines.append(
                            f"{table_name},DIFFERENT,{row_diff.row_identifier},"
                            f"{field_diff.field_name},{field_diff.value_db1},{field_diff.value_db2}"
                        )
            
            return "\n".join(csv_lines)
        except Exception as e:
            raise DatabaseComparisonError(f"Failed to generate CSV report: {e}")
    
    def _generate_summary_text(self, comparison_result: ComparisonResult) -> str:
        """Generate a text summary of the comparison"""
        summary = f"""Database Comparison Summary
Generated: {comparison_result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

Total Tables: {comparison_result.summary.total_tables}
Identical Tables: {comparison_result.summary.identical_tables}
Tables with Differences: {comparison_result.summary.tables_with_differences}
Total Rows Compared: {comparison_result.summary.total_rows_compared}
Total Differences Found: {comparison_result.summary.total_differences_found}

Schema Identical: {comparison_result.schema_comparison.identical}
Data Differences: {comparison_result.data_comparison.total_differences}
"""
        return summary
    
    def _generate_missing_tables_html(self, schema_comparison) -> str:
        """Generate HTML for missing tables"""
        html = ""
        if schema_comparison.missing_in_db1:
            html += f"""
        <h3>Tables Missing in Database 1</h3>
        <ul>
            {''.join(f'<li class="warning">{table}</li>' for table in schema_comparison.missing_in_db1)}
        </ul>
"""
        
        if schema_comparison.missing_in_db2:
            html += f"""
        <h3>Tables Missing in Database 2</h3>
        <ul>
            {''.join(f'<li class="warning">{table}</li>' for table in schema_comparison.missing_in_db2)}
        </ul>
"""
        return html
    
    def _generate_schema_differences_html(self, schema_comparison) -> str:
        """Generate HTML for schema differences"""
        if not schema_comparison.table_differences:
            return "<p>No schema differences found.</p>"
        
        html = "<h3>Table Schema Differences</h3>"
        for table_name, table_diff in schema_comparison.table_differences.items():
            html += f"""
        <h4>{table_name}</h4>
        <ul>
            <li>Missing columns in DB1: {table_diff.missing_columns_db1 or 'None'}</li>
            <li>Missing columns in DB2: {table_diff.missing_columns_db2 or 'None'}</li>
            <li>Column differences: {len(table_diff.column_differences)}</li>
        </ul>
"""
        return html
    
    def _generate_data_differences_html(self, data_comparison) -> str:
        """Generate HTML for data differences"""
        if not data_comparison.table_results:
            return "<p>No data differences found.</p>"
        
        html = ""
        for table_name, table_data in data_comparison.table_results.items():
            html += f"""
        <h3>{table_name}</h3>
        <ul>
            <li>Rows in DB1: {table_data.row_count_db1}</li>
            <li>Rows in DB2: {table_data.row_count_db2}</li>
            <li>Matching rows: {table_data.matching_rows}</li>
            <li>Rows only in DB1: {len(table_data.rows_only_in_db1)}</li>
            <li>Rows only in DB2: {len(table_data.rows_only_in_db2)}</li>
            <li>Rows with differences: {len(table_data.rows_with_differences)}</li>
        </ul>
"""
        return html
    
    def _generate_missing_tables_markdown(self, schema_comparison) -> str:
        """Generate Markdown for missing tables"""
        md = ""
        if schema_comparison.missing_in_db1:
            md += "### Tables Missing in Database 1\n\n"
            for table in schema_comparison.missing_in_db1:
                md += f"- {table}\n"
            md += "\n"
        
        if schema_comparison.missing_in_db2:
            md += "### Tables Missing in Database 2\n\n"
            for table in schema_comparison.missing_in_db2:
                md += f"- {table}\n"
            md += "\n"
        
        return md
    
    def _generate_schema_differences_markdown(self, schema_comparison) -> str:
        """Generate Markdown for schema differences"""
        if not schema_comparison.table_differences:
            return "No schema differences found.\n\n"
        
        md = "### Table Schema Differences\n\n"
        for table_name, table_diff in schema_comparison.table_differences.items():
            md += f"#### {table_name}\n\n"
            md += f"- Missing columns in DB1: {table_diff.missing_columns_db1 or 'None'}\n"
            md += f"- Missing columns in DB2: {table_diff.missing_columns_db2 or 'None'}\n"
            md += f"- Column differences: {len(table_diff.column_differences)}\n\n"
        
        return md
    
    def _generate_data_differences_markdown(self, data_comparison) -> str:
        """Generate Markdown for data differences"""
        if not data_comparison.table_results:
            return "No data differences found.\n\n"
        
        md = ""
        for table_name, table_data in data_comparison.table_results.items():
            md += f"### {table_name}\n\n"
            md += f"- Rows in DB1: {table_data.row_count_db1}\n"
            md += f"- Rows in DB2: {table_data.row_count_db2}\n"
            md += f"- Matching rows: {table_data.matching_rows}\n"
            md += f"- Rows only in DB1: {len(table_data.rows_only_in_db1)}\n"
            md += f"- Rows only in DB2: {len(table_data.rows_only_in_db2)}\n"
            md += f"- Rows with differences: {len(table_data.rows_with_differences)}\n\n"
        
        return md
