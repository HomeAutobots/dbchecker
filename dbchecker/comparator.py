"""
Main database comparator module that orchestrates the comparison process.
"""

from datetime import datetime
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from .models import ComparisonOptions, ComparisonResult, ComparisonSummary
from .database_connector import DatabaseConnector
from .schema_comparator import SchemaComparator
from .data_comparator import DataComparator
from .uuid_handler import UUIDHandler
from .report_generator import ReportGenerator
from .exceptions import DatabaseComparisonError, InvalidConfigurationError


class DatabaseComparator:
    """Main controller that orchestrates the database comparison process"""
    
    def __init__(self, db1_path: str, db2_path: str, uuid_columns: Optional[List[str]] = None):
        """Initialize database comparator
        
        Args:
            db1_path: Path to first SQLite database
            db2_path: Path to second SQLite database  
            uuid_columns: List of explicit UUID column names
        """
        self.db1_path = db1_path
        self.db2_path = db2_path
        
        # Initialize components
        self.uuid_handler = UUIDHandler(explicit_uuid_columns=uuid_columns or [])
        self.schema_comparator = SchemaComparator()
        
        # Default options (will be updated when set_comparison_options is called)
        self.options = ComparisonOptions()
        self.data_comparator = DataComparator(self.uuid_handler, self.options)
        self.report_generator = ReportGenerator()
        
        # Default options
        self.options = ComparisonOptions()
        
        # Database connections (initialized during comparison)
        self.conn1: Optional[DatabaseConnector] = None
        self.conn2: Optional[DatabaseConnector] = None
    
    def set_comparison_options(self, options: ComparisonOptions):
        """Set comparison options"""
        self.options = options
        
        # Reinitialize data comparator with new options
        self.data_comparator = DataComparator(self.uuid_handler, self.options)
        
        # Update UUID handler with new options
        if options.explicit_uuid_columns:
            for col in options.explicit_uuid_columns:
                self.uuid_handler.add_explicit_uuid_column(col)
        
        if options.uuid_patterns:
            for pattern in options.uuid_patterns:
                self.uuid_handler.add_custom_pattern(pattern)
    
    def compare(self) -> ComparisonResult:
        """Run the complete database comparison"""
        try:
            # Validate inputs
            self._validate_configuration()
            
            # Initialize database connections
            self._initialize_connections()
            
            # Run comparison phases
            schema_result = None
            data_result = None
            
            if self.options.compare_schema:
                schema_result = self._compare_schemas()
            
            if self.options.compare_data:
                data_result = self._compare_data()
            
            # Generate summary
            summary = self._generate_summary(schema_result, data_result)
            
            # Create final result
            result = ComparisonResult(
                schema_comparison=schema_result,
                data_comparison=data_result,
                summary=summary,
                timestamp=datetime.now()
            )
            
            return result
            
        except Exception as e:
            raise DatabaseComparisonError(f"Comparison failed: {e}")
        finally:
            self._cleanup_connections()
    
    def _validate_configuration(self):
        """Validate configuration and options"""
        if not self.db1_path or not self.db2_path:
            raise InvalidConfigurationError("Database paths must be provided")
        
        if self.options.batch_size <= 0:
            raise InvalidConfigurationError("Batch size must be positive")
        
        if self.options.max_workers <= 0:
            raise InvalidConfigurationError("Max workers must be positive")
    
    def _initialize_connections(self):
        """Initialize database connections"""
        try:
            self.conn1 = DatabaseConnector(self.db1_path)
            self.conn2 = DatabaseConnector(self.db2_path)
        except Exception as e:
            raise DatabaseComparisonError(f"Failed to initialize database connections: {e}")
    
    def _cleanup_connections(self):
        """Clean up database connections"""
        if self.conn1:
            self.conn1.close()
            self.conn1 = None
        if self.conn2:
            self.conn2.close()
            self.conn2 = None
    
    def _compare_schemas(self):
        """Compare database schemas"""
        if self.options.verbose:
            print("Comparing database schemas...")
        
        if not self.conn1 or not self.conn2:
            raise DatabaseComparisonError("Database connections not initialized")
        
        try:
            schema1 = self.conn1.get_schema()
            schema2 = self.conn2.get_schema()
            
            result = self.schema_comparator.compare_schemas(schema1, schema2)
            
            if self.options.verbose:
                identical_count = len(schema1.tables) - len(result.table_differences)
                print(f"Schema comparison complete: {identical_count} identical tables, "
                      f"{len(result.table_differences)} with differences")
            
            return result
            
        except Exception as e:
            raise DatabaseComparisonError(f"Schema comparison failed: {e}")
    
    def _compare_data(self):
        """Compare database data"""
        if self.options.verbose:
            print("Comparing database data...")
        
        if not self.conn1 or not self.conn2:
            raise DatabaseComparisonError("Database connections not initialized")
        
        try:
            # Get list of common tables
            tables1 = set(self.conn1.get_table_names())
            tables2 = set(self.conn2.get_table_names())
            common_tables = list(tables1 & tables2)
            
            if self.options.verbose:
                print(f"Found {len(common_tables)} common tables to compare")
            
            # Compare data
            if self.options.parallel_tables and len(common_tables) > 1:
                result = self._compare_data_parallel(common_tables)
            else:
                result = self._compare_data_sequential(common_tables)
            
            if self.options.verbose:
                print(f"Data comparison complete: {result.total_differences} differences found")
            
            return result
            
        except Exception as e:
            raise DatabaseComparisonError(f"Data comparison failed: {e}")
    
    def _compare_data_sequential(self, table_names: List[str]):
        """Compare data sequentially"""
        if not self.conn1 or not self.conn2:
            raise DatabaseComparisonError("Database connections not initialized")
        return self.data_comparator.compare_all_tables(
            self.conn1, self.conn2, table_names, self.options.batch_size
        )
    
    def _compare_data_parallel(self, table_names: List[str]):
        """Compare data in parallel"""
        if not self.conn1 or not self.conn2:
            raise DatabaseComparisonError("Database connections not initialized")
            
        table_results = {}
        total_differences = 0
        
        with ThreadPoolExecutor(max_workers=self.options.max_workers) as executor:
            # Submit comparison tasks
            future_to_table = {
                executor.submit(
                    self.data_comparator.compare_table_data,
                    table_name, self.conn1, self.conn2, self.options.batch_size
                ): table_name
                for table_name in table_names
            }
            
            # Collect results
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    table_comparison = future.result()
                    table_results[table_name] = table_comparison
                    
                    # Count differences
                    total_differences += (
                        len(table_comparison.rows_only_in_db1) +
                        len(table_comparison.rows_only_in_db2) +
                        len(table_comparison.rows_with_differences)
                    )
                    
                    if self.options.verbose:
                        print(f"Completed comparison for table: {table_name}")
                        
                except Exception as e:
                    if self.options.verbose:
                        print(f"Failed to compare table {table_name}: {e}")
                    raise DatabaseComparisonError(f"Failed to compare table {table_name}: {e}")
        
        from .models import DataComparisonResult
        return DataComparisonResult(
            table_results=table_results,
            total_differences=total_differences
        )
    
    def _generate_summary(self, schema_result, data_result) -> ComparisonSummary:
        """Generate comparison summary"""
        total_tables = 0
        identical_tables = 0
        tables_with_differences = 0
        total_rows_compared = 0
        total_differences_found = 0
        
        # UUID statistics
        total_uuid_columns = 0
        total_uuid_values_db1 = 0
        total_uuid_values_db2 = 0
        uuid_integrity_check = True
        
        schema_identical_tables = 0
        
        if schema_result:
            # Get table count from schema
            tables1 = set()
            tables2 = set()
            if self.conn1 and self.conn2:
                tables1 = set(self.conn1.get_table_names())
                tables2 = set(self.conn2.get_table_names())
            total_tables = len(tables1 | tables2)
            
            # Count schema differences
            schema_identical_tables = total_tables - len(schema_result.table_differences)
            if not data_result:
                identical_tables = schema_identical_tables
                tables_with_differences = len(schema_result.table_differences)
        
        if data_result:
            # Count data differences and UUID statistics
            data_identical_tables = 0
            for table_comp in data_result.table_results.values():
                total_rows_compared += table_comp.row_count_db1 + table_comp.row_count_db2
                
                has_differences = (
                    table_comp.rows_only_in_db1 or
                    table_comp.rows_only_in_db2 or
                    table_comp.rows_with_differences
                )
                
                if not has_differences:
                    data_identical_tables += 1
                
                # Collect UUID statistics
                if table_comp.uuid_statistics:
                    total_uuid_columns += len(table_comp.uuid_statistics.uuid_columns)
                    total_uuid_values_db1 += table_comp.uuid_statistics.total_uuid_values_db1
                    total_uuid_values_db2 += table_comp.uuid_statistics.total_uuid_values_db2
                    
                    # Check UUID integrity - expect same number of UUID values
                    if table_comp.uuid_statistics.total_uuid_values_db1 != table_comp.uuid_statistics.total_uuid_values_db2:
                        uuid_integrity_check = False
            
            total_differences_found = data_result.total_differences
            
            if schema_result:
                # Combine schema and data results
                identical_tables = min(schema_identical_tables, data_identical_tables)
                tables_with_differences = total_tables - identical_tables
            else:
                identical_tables = data_identical_tables
                tables_with_differences = len(data_result.table_results) - data_identical_tables
                total_tables = len(data_result.table_results)
        
        return ComparisonSummary(
            total_tables=total_tables,
            identical_tables=identical_tables,
            tables_with_differences=tables_with_differences,
            total_rows_compared=total_rows_compared,
            total_differences_found=total_differences_found,
            total_uuid_columns=total_uuid_columns,
            total_uuid_values_db1=total_uuid_values_db1,
            total_uuid_values_db2=total_uuid_values_db2,
            uuid_integrity_check=uuid_integrity_check
        )
    
    def generate_reports(self, comparison_result: ComparisonResult, 
                        output_dir: str = ".", filename_prefix: str = "comparison_report"):
        """Generate reports in specified formats"""
        for format_type in self.options.output_format:
            try:
                if format_type == "json":
                    content = self.report_generator.to_json(comparison_result)
                    filename = f"{filename_prefix}.json"
                elif format_type == "html":
                    content = self.report_generator.to_html(comparison_result)
                    filename = f"{filename_prefix}.html"
                elif format_type == "markdown":
                    content = self.report_generator.to_markdown(comparison_result)
                    filename = f"{filename_prefix}.md"
                elif format_type == "csv":
                    content = self.report_generator.to_csv(comparison_result)
                    filename = f"{filename_prefix}.csv"
                else:
                    if self.options.verbose:
                        print(f"Unknown format type: {format_type}")
                    continue
                
                # Write to file
                import os
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                if self.options.verbose:
                    print(f"Generated {format_type} report: {filepath}")
                    
            except Exception as e:
                if self.options.verbose:
                    print(f"Failed to generate {format_type} report: {e}")
    
    def get_comparison_statistics(self) -> dict:
        """Get statistics about the comparison process"""
        stats = {
            "database_paths": {
                "db1": self.db1_path,
                "db2": self.db2_path
            },
            "options": {
                "compare_schema": self.options.compare_schema,
                "compare_data": self.options.compare_data,
                "auto_detect_uuids": self.options.auto_detect_uuids,
                "batch_size": self.options.batch_size,
                "parallel_tables": self.options.parallel_tables
            }
        }
        
        # Add UUID handler statistics
        stats["uuid_detection"] = self.uuid_handler.get_statistics()
        
        return stats
