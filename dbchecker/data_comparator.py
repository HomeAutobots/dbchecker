"""
Data comparator module for comparing database contents while handling UUIDs and timestamps.
"""

import hashlib
import json
import re
from typing import Dict, List, Any, Tuple, Set
from .models import TableDataComparison, RowDifference, FieldDifference, DataComparisonResult
from .uuid_handler import UUIDHandler
from .database_connector import DatabaseConnector
from .exceptions import DataComparisonError


class DataComparator:
    """Compares actual data between databases while handling UUID and timestamp exclusions"""
    
    def __init__(self, uuid_handler: UUIDHandler):
        """Initialize data comparator with UUID handler"""
        self.uuid_handler = uuid_handler
    
    def _detect_timestamp_columns(self, table_structure) -> List[str]:
        """Detect timestamp columns by name patterns and data types"""
        timestamp_columns = []
        
        # Common timestamp column name patterns
        timestamp_name_patterns = [
            r'.*timestamp.*',
            r'.*created.*',
            r'.*updated.*',
            r'.*modified.*',
            r'.*deleted.*',
            r'.*_at$',
            r'.*_time$',
            r'.*_date$'
        ]
        
        # Common timestamp data types
        timestamp_data_types = {
            'DATETIME', 'TIMESTAMP', 'DATE', 'TIME',
            'datetime', 'timestamp', 'date', 'time'
        }
        
        for column in table_structure.columns:
            # Check by data type first
            if column.type.upper() in timestamp_data_types:
                timestamp_columns.append(column.name)
                continue
            
            # Check by column name patterns
            for pattern in timestamp_name_patterns:
                if re.match(pattern, column.name.lower()):
                    timestamp_columns.append(column.name)
                    break
        
        return timestamp_columns
    
    def get_excluded_columns_info(self, table_structure) -> Dict[str, List[str]]:
        """Get information about which columns are excluded from comparison"""
        uuid_columns = self.uuid_handler.detect_uuid_columns(table_structure, None)
        timestamp_columns = self._detect_timestamp_columns(table_structure)
        
        return {
            'uuid_columns': uuid_columns,
            'timestamp_columns': timestamp_columns,
            'all_excluded': list(set(uuid_columns + timestamp_columns))
        }
    
    def compare_all_tables(self, conn1: DatabaseConnector, conn2: DatabaseConnector, 
                          table_names: List[str], batch_size: int = 1000) -> DataComparisonResult:
        """Compare data in all specified tables"""
        table_results = {}
        total_differences = 0
        
        for table_name in table_names:
            try:
                table_comparison = self.compare_table_data(table_name, conn1, conn2, batch_size)
                table_results[table_name] = table_comparison
                
                # Count total differences
                total_differences += (
                    len(table_comparison.rows_only_in_db1) +
                    len(table_comparison.rows_only_in_db2) +
                    len(table_comparison.rows_with_differences)
                )
            except Exception as e:
                raise DataComparisonError(f"Failed to compare table {table_name}: {e}")
        
        return DataComparisonResult(
            table_results=table_results,
            total_differences=total_differences
        )
    
    def compare_table_data(self, table_name: str, conn1: DatabaseConnector, 
                          conn2: DatabaseConnector, batch_size: int = 1000) -> TableDataComparison:
        """Compare data in a specific table between two databases"""
        # Get table structure to detect UUID and timestamp columns
        table_structure1 = conn1.get_table_structure(table_name)
        
        # Get sample data for UUID detection
        sample_data1 = conn1.get_table_data(table_name, limit=100)
        uuid_columns = self.uuid_handler.detect_uuid_columns(table_structure1, sample_data1)
        
        # Detect timestamp columns
        timestamp_columns = self._detect_timestamp_columns(table_structure1)
        
        # Combine columns to exclude from comparison
        exclude_columns = list(set(uuid_columns + timestamp_columns))
        
        # Get all data from both tables
        data1 = conn1.get_table_data(table_name)
        data2 = conn2.get_table_data(table_name)
        
        row_count_db1 = len(data1)
        row_count_db2 = len(data2)
        
        # Find matching rows and differences (excluding UUID and timestamp columns)
        matching_result = self.find_matching_rows(data1, data2, exclude_columns)
        
        # Compare matched rows for differences
        rows_with_differences = []
        for row1, row2 in matching_result['matched_pairs']:
            differences = self.identify_differences(row1, row2, exclude_columns)
            if differences:
                # Create a unique identifier for the row
                row_id = self._create_row_identifier(row1, exclude_columns)
                row_diff = RowDifference(
                    row_identifier=row_id,
                    differences=differences
                )
                rows_with_differences.append(row_diff)
        
        matching_rows = len(matching_result['matched_pairs']) - len(rows_with_differences)
        
        return TableDataComparison(
            table_name=table_name,
            row_count_db1=row_count_db1,
            row_count_db2=row_count_db2,
            matching_rows=matching_rows,
            rows_only_in_db1=matching_result['only_in_db1'],
            rows_only_in_db2=matching_result['only_in_db2'],
            rows_with_differences=rows_with_differences
        )
    
    def find_matching_rows(self, rows1: List[Dict[str, Any]], rows2: List[Dict[str, Any]], 
                          exclude_columns: List[str]) -> Dict[str, Any]:
        """Find matching rows between two datasets, excluding specified columns"""
        # Create hash maps for efficient lookup
        hash_map1 = {}
        hash_map2 = {}
        
        # Hash rows from first database
        for row in rows1:
            row_hash = self.get_row_hash(row, exclude_columns)
            if row_hash in hash_map1:
                # Handle duplicate rows by storing as list
                if not isinstance(hash_map1[row_hash], list):
                    hash_map1[row_hash] = [hash_map1[row_hash]]
                hash_map1[row_hash].append(row)
            else:
                hash_map1[row_hash] = row
        
        # Hash rows from second database
        for row in rows2:
            row_hash = self.get_row_hash(row, exclude_columns)
            if row_hash in hash_map2:
                # Handle duplicate rows by storing as list
                if not isinstance(hash_map2[row_hash], list):
                    hash_map2[row_hash] = [hash_map2[row_hash]]
                hash_map2[row_hash].append(row)
            else:
                hash_map2[row_hash] = row
        
        # Find matches and differences
        matched_pairs = []
        only_in_db1 = []
        only_in_db2 = []
        
        # Track processed hashes
        processed_hashes = set()
        
        # Find matches
        for row_hash in hash_map1:
            if row_hash in hash_map2:
                rows_1 = hash_map1[row_hash] if isinstance(hash_map1[row_hash], list) else [hash_map1[row_hash]]
                rows_2 = hash_map2[row_hash] if isinstance(hash_map2[row_hash], list) else [hash_map2[row_hash]]
                
                # Match rows one-to-one
                min_length = min(len(rows_1), len(rows_2))
                for i in range(min_length):
                    matched_pairs.append((rows_1[i], rows_2[i]))
                
                # Add unmatched rows to respective lists
                if len(rows_1) > min_length:
                    only_in_db1.extend(rows_1[min_length:])
                if len(rows_2) > min_length:
                    only_in_db2.extend(rows_2[min_length:])
                
                processed_hashes.add(row_hash)
        
        # Add unmatched rows from db1
        for row_hash in hash_map1:
            if row_hash not in processed_hashes:
                rows_1 = hash_map1[row_hash] if isinstance(hash_map1[row_hash], list) else [hash_map1[row_hash]]
                only_in_db1.extend(rows_1)
        
        # Add unmatched rows from db2
        for row_hash in hash_map2:
            if row_hash not in processed_hashes:
                rows_2 = hash_map2[row_hash] if isinstance(hash_map2[row_hash], list) else [hash_map2[row_hash]]
                only_in_db2.extend(rows_2)
        
        return {
            'matched_pairs': matched_pairs,
            'only_in_db1': only_in_db1,
            'only_in_db2': only_in_db2
        }
    
    def get_row_hash(self, row: Dict[str, Any], exclude_columns: List[str]) -> str:
        """Generate a hash for a row, excluding specified columns"""
        # Normalize row for comparison
        normalized_row = self.uuid_handler.normalize_row_for_comparison(row, exclude_columns)
        
        # Sort keys for consistent hashing
        sorted_items = sorted(normalized_row.items())
        
        # Create hash
        row_string = json.dumps(sorted_items, sort_keys=True, default=str)
        return hashlib.md5(row_string.encode('utf-8')).hexdigest()
    
    def identify_differences(self, row1: Dict[str, Any], row2: Dict[str, Any], 
                           exclude_columns: List[str]) -> List[FieldDifference]:
        """Identify differences between two rows, excluding specified columns"""
        differences = []
        
        # Get all column names from both rows
        all_columns = set(row1.keys()) | set(row2.keys())
        
        for column in all_columns:
            # Skip excluded columns (UUIDs, timestamps, etc.)
            if column in exclude_columns:
                continue
            
            value1 = row1.get(column)
            value2 = row2.get(column)
            
            # Compare values
            if not self._values_equal(value1, value2):
                differences.append(FieldDifference(
                    field_name=column,
                    value_db1=value1,
                    value_db2=value2
                ))
        
        return differences
    
    def _values_equal(self, value1: Any, value2: Any) -> bool:
        """Compare two values for equality with type normalization"""
        # Handle None values
        if value1 is None and value2 is None:
            return True
        if value1 is None or value2 is None:
            return False
        
        # Handle string comparison (case-sensitive by default)
        if isinstance(value1, str) and isinstance(value2, str):
            return value1.strip() == value2.strip()
        
        # Handle numeric comparison
        if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
            return abs(value1 - value2) < 1e-10  # Account for floating point precision
        
        # Default comparison
        return value1 == value2
    
    def _create_row_identifier(self, row: Dict[str, Any], exclude_columns: List[str]) -> str:
        """Create a unique identifier for a row based on non-excluded columns"""
        # Use non-excluded columns to create identifier
        identifier_parts = []
        for key, value in sorted(row.items()):
            if key not in exclude_columns:
                identifier_parts.append(f"{key}={value}")
        
        if identifier_parts:
            return "|".join(identifier_parts[:3])  # Use first 3 fields for identifier
        else:
            # Fallback to row index if no non-excluded columns
            return f"row_{hash(str(row)) % 10000}"
    
    def get_statistics(self, comparison_result: DataComparisonResult) -> Dict[str, Any]:
        """Get statistics about the data comparison"""
        total_tables = len(comparison_result.table_results)
        tables_with_differences = sum(
            1 for table_comp in comparison_result.table_results.values()
            if (table_comp.rows_only_in_db1 or 
                table_comp.rows_only_in_db2 or 
                table_comp.rows_with_differences)
        )
        
        total_rows_compared = sum(
            table_comp.row_count_db1 + table_comp.row_count_db2
            for table_comp in comparison_result.table_results.values()
        )
        
        return {
            'total_tables': total_tables,
            'tables_with_differences': tables_with_differences,
            'tables_identical': total_tables - tables_with_differences,
            'total_rows_compared': total_rows_compared,
            'total_differences': comparison_result.total_differences
        }
