"""
UUID handler module for detecting and managing UUID columns.
"""

import re
import uuid
from typing import List, Dict, Any, Set, Optional
from .models import TableStructure
from .exceptions import UUIDDetectionError


class UUIDHandler:
    """Manages UUID detection and exclusion during comparison"""
    
    def __init__(self, explicit_uuid_columns: Optional[List[str]] = None, custom_patterns: Optional[List[str]] = None):
        """Initialize UUID handler with explicit columns and custom patterns"""
        self.explicit_uuid_columns = set(explicit_uuid_columns or [])
        self.custom_patterns = custom_patterns or []
        self.detected_uuid_columns: Dict[str, Set[str]] = {}  # table_name -> set of uuid columns
        
        # Default UUID patterns
        self.default_patterns = [
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',  # Standard UUID
            r'^[0-9a-f]{32}$',  # UUID without hyphens
            r'^[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}$',  # Uppercase UUID
            r'^[0-9A-F]{32}$',  # Uppercase UUID without hyphens
        ]
        
        # Combine all patterns
        self.all_patterns = self.default_patterns + self.custom_patterns
    
    def is_uuid_column(self, column_name: str, column_type: str = '') -> bool:
        """Check if a column is explicitly marked as UUID"""
        # Check explicit UUID columns (case-insensitive)
        if column_name.lower() in {col.lower() for col in self.explicit_uuid_columns}:
            return True
        
        # Check common UUID column name patterns (more conservative)
        uuid_name_patterns = [
            r'.*uuid.*',
            r'.*guid.*'
        ]
        
        for pattern in uuid_name_patterns:
            if re.match(pattern, column_name.lower()):
                return True
        
        # Check column type
        if column_type.upper() in ['UUID', 'GUID']:
            return True
        
        return False
    
    def is_valid_uuid(self, value: Any) -> bool:
        """Check if a value matches UUID patterns"""
        if value is None:
            return False
        
        str_value = str(value)
        
        # Try to parse as UUID using built-in library
        try:
            uuid.UUID(str_value)
            return True
        except (ValueError, TypeError):
            pass
        
        # Check against regex patterns
        for pattern in self.all_patterns:
            if re.match(pattern, str_value, re.IGNORECASE):
                return True
        
        return False
    
    def detect_uuid_columns(self, table_structure: TableStructure, sample_data: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """Detect UUID columns in a table structure and optionally sample data"""
        uuid_columns = set()
        
        # Check explicit UUID columns
        for column in table_structure.columns:
            if self.is_uuid_column(column.name, column.type):
                uuid_columns.add(column.name)
        
        # If sample data is provided, analyze values
        if sample_data:
            for column in table_structure.columns:
                if column.name in uuid_columns:
                    continue  # Already identified
                
                # Sample values from this column
                sample_values = []
                for row in sample_data[:100]:  # Sample first 100 rows
                    value = row.get(column.name)
                    if value is not None:
                        sample_values.append(value)
                
                # Check if majority of values are UUIDs
                if sample_values:
                    uuid_count = sum(1 for value in sample_values if self.is_valid_uuid(value))
                    uuid_ratio = uuid_count / len(sample_values)
                    
                    # If 80% or more values are UUIDs, consider it a UUID column
                    if uuid_ratio >= 0.8:
                        uuid_columns.add(column.name)
        
        # Cache the result
        self.detected_uuid_columns[table_structure.name] = uuid_columns
        
        return list(uuid_columns)
    
    def get_uuid_columns(self, table_name: str) -> List[str]:
        """Get cached UUID columns for a table"""
        return list(self.detected_uuid_columns.get(table_name, set()))
    
    def normalize_row_for_comparison(self, row: Dict[str, Any], uuid_columns: List[str]) -> Dict[str, Any]:
        """Remove UUID columns from a row for comparison purposes"""
        normalized_row = {}
        for key, value in row.items():
            if key not in uuid_columns:
                # Normalize value for comparison
                if isinstance(value, str):
                    normalized_row[key] = value.strip() if value else value
                else:
                    normalized_row[key] = value
        
        return normalized_row
    
    def exclude_columns(self, row: Dict[str, Any], exclude_columns: List[str]) -> Dict[str, Any]:
        """Exclude specified columns from a row"""
        return {k: v for k, v in row.items() if k not in exclude_columns}
    
    def add_explicit_uuid_column(self, column_name: str):
        """Add a column to the explicit UUID columns list"""
        self.explicit_uuid_columns.add(column_name)
    
    def remove_explicit_uuid_column(self, column_name: str):
        """Remove a column from the explicit UUID columns list"""
        self.explicit_uuid_columns.discard(column_name)
    
    def add_custom_pattern(self, pattern: str):
        """Add a custom UUID pattern"""
        try:
            # Validate the regex pattern
            re.compile(pattern)
            self.custom_patterns.append(pattern)
            self.all_patterns = self.default_patterns + self.custom_patterns
        except re.error as e:
            raise UUIDDetectionError(f"Invalid regex pattern: {pattern}. Error: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about UUID detection"""
        total_tables = len(self.detected_uuid_columns)
        total_uuid_columns = sum(len(cols) for cols in self.detected_uuid_columns.values())
        
        return {
            'total_tables_analyzed': total_tables,
            'total_uuid_columns_detected': total_uuid_columns,
            'explicit_uuid_columns': len(self.explicit_uuid_columns),
            'custom_patterns': len(self.custom_patterns),
            'tables_with_uuid_columns': len([cols for cols in self.detected_uuid_columns.values() if cols])
        }

    def collect_uuid_statistics(self, table_data: List[Dict[str, Any]], uuid_columns: List[str], 
                               comparison_options=None) -> Dict[str, Any]:
        """Collect statistics about UUID values in a dataset"""
        if not uuid_columns or not table_data:
            return {
                'total_uuid_values': 0,
                'unique_uuid_values': 0,
                'uuid_column_stats': {},
                'detected_patterns': {},
                'normalized_values': {}
            }
        
        total_uuid_values = 0
        all_uuid_values = set()
        uuid_column_stats = {}
        detected_patterns = {}
        normalized_values = {}
        
        for column in uuid_columns:
            column_values = []
            column_normalized_values = []
            for row in table_data:
                if column in row and row[column] is not None:
                    value = str(row[column])
                    column_values.append(value)
                    all_uuid_values.add(value)
                    total_uuid_values += 1
                    
                    # Try to normalize the value if patterns are provided
                    if comparison_options and hasattr(comparison_options, 'unique_id_patterns'):
                        normalized = self._normalize_unique_id(value, comparison_options)
                        column_normalized_values.append(normalized)
            
            # Detect pattern for this column
            if column_values:
                detected_pattern = self._detect_unique_id_pattern(column_values)
                if detected_pattern:
                    detected_patterns[column] = detected_pattern
            
            uuid_column_stats[column] = {
                'total_values': len(column_values),
                'unique_values': len(set(column_values)),
                'null_values': sum(1 for row in table_data if row.get(column) is None),
                'sample_values': column_values[:5]  # First 5 values for pattern analysis
            }
            
            if column_normalized_values:
                normalized_values[column] = column_normalized_values
        
        return {
            'total_uuid_values': total_uuid_values,
            'unique_uuid_values': len(all_uuid_values),
            'uuid_column_stats': uuid_column_stats,
            'detected_patterns': detected_patterns,
            'normalized_values': normalized_values
        }

    def _detect_unique_id_pattern(self, values: List[str]) -> Optional[str]:
        """Automatically detect the pattern of unique identifiers"""
        if not values:
            return None
        
        # Sample first few values to detect pattern
        sample_values = values[:10]
        
        # Common patterns to check
        patterns_to_check = [
            (r'^[a-zA-Z]+-\d+$', 'prefix-number'),  # report-123, record-456
            (r'^\d+-[a-zA-Z]+$', 'number-suffix'),  # 123-report, 456-record
            (r'^[a-zA-Z]+_\d+$', 'prefix_number'),  # report_123, record_456
            (r'^\d+_[a-zA-Z]+$', 'number_suffix'),  # 123_report, 456_record
            (r'^[A-Z]{2,4}\d{6,}$', 'code-number'), # ABC123456, DEFG789012
            (r'^\d{8,}-\d{4,}$', 'timestamp-serial'), # 20240101-1234
        ]
        
        for pattern, description in patterns_to_check:
            matches = sum(1 for value in sample_values if re.match(pattern, value))
            if matches / len(sample_values) >= 0.8:  # 80% match threshold
                return description
        
        return 'custom'  # Couldn't detect a standard pattern

    def _normalize_unique_id(self, value: str, comparison_options) -> str:
        """Normalize a unique identifier based on configured patterns"""
        if not hasattr(comparison_options, 'unique_id_normalize_patterns'):
            return value
        
        normalized = value
        
        # Apply normalization patterns
        for pattern_rule in comparison_options.unique_id_normalize_patterns:
            if 'pattern' in pattern_rule and 'replacement' in pattern_rule:
                try:
                    normalized = re.sub(pattern_rule['pattern'], pattern_rule['replacement'], normalized)
                except re.error:
                    continue  # Skip invalid patterns
        
        return normalized

    def compare_normalized_unique_ids(self, data1: List[Dict[str, Any]], data2: List[Dict[str, Any]], 
                                    uuid_columns: List[str], comparison_options) -> Dict[str, Any]:
        """Compare unique IDs after normalization to find logical matches"""
        if not uuid_columns or not data1 or not data2:
            return {'normalized_matches': 0, 'total_comparisons': 0}
        
        # Get normalized values for both datasets
        stats1 = self.collect_uuid_statistics(data1, uuid_columns, comparison_options)
        stats2 = self.collect_uuid_statistics(data2, uuid_columns, comparison_options)
        
        normalized_matches = 0
        total_comparisons = 0
        
        # For each UUID column, compare normalized values
        for column in uuid_columns:
            if column in stats1['normalized_values'] and column in stats2['normalized_values']:
                norm_vals1 = set(stats1['normalized_values'][column])
                norm_vals2 = set(stats2['normalized_values'][column])
                
                # Count intersection (logical matches)
                matches = len(norm_vals1 & norm_vals2)
                total = max(len(norm_vals1), len(norm_vals2))
                
                normalized_matches += matches
                total_comparisons += total
        
        return {
            'normalized_matches': normalized_matches,
            'total_comparisons': total_comparisons,
            'match_percentage': (normalized_matches / total_comparisons * 100) if total_comparisons > 0 else 0
        }
