"""
Metadata detector module for identifying database columns that should be excluded from comparison.
"""

import re
from typing import Dict, List, Set, Any, Optional, Union
from .models import TableStructure, ComparisonOptions


class MetadataDetector:
    """Detects various types of metadata columns that should be excluded from comparison"""
    
    def __init__(self, options: ComparisonOptions):
        """Initialize metadata detector with comparison options"""
        self.options = options
        
        # Default patterns for different types of metadata
        self.default_timestamp_patterns = [
            r'.*timestamp.*',
            r'.*_at$',
            r'.*_time$',
            r'.*_date$',
            r'^created$',
            r'^modified$',
            r'^updated$',
            r'^deleted$',
            r'.*created_time.*',
            r'.*updated_time.*',
            r'.*modified_time.*',
            r'.*deleted_time.*'
        ]
        
        self.default_metadata_patterns = [
            r'.*created_by.*',
            r'.*modified_by.*',
            r'.*updated_by.*',
            r'.*session_id.*',
            r'.*transaction_id.*',
            r'.*row_version.*',
            r'.*record_version.*',
            r'.*version_number.*',
            r'.*etag.*',
            r'.*checksum.*',
            r'.*hash.*',
            r'.*audit_log.*',
            r'.*trace_id.*',
            r'.*source_system.*',
            r'.*external_id.*'
        ]
        
        self.default_sequence_patterns = [
            r'^id$',
            r'.*_seq$',
            r'.*_sequence$',
            r'.*_number$',
            r'.*rowid.*',
            r'.*autoincrement.*'
        ]
        
        # Common timestamp data types
        self.timestamp_data_types = {
            'DATETIME', 'TIMESTAMP', 'DATE', 'TIME',
            'datetime', 'timestamp', 'date', 'time'
        }
        
        # Common sequence/auto-increment data types
        self.sequence_data_types = {
            'SERIAL', 'BIGSERIAL', 'IDENTITY',
            'serial', 'bigserial', 'identity'
        }
    
    def detect_timestamp_columns(self, table_structure: TableStructure, sample_data: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """Detect timestamp columns by name patterns and data types"""
        if not self.options.auto_detect_timestamps:
            return self.options.explicit_timestamp_columns.copy()
        
        timestamp_columns = []
        
        # Get patterns to use
        patterns = self.options.timestamp_patterns if self.options.timestamp_patterns else self.default_timestamp_patterns
        
        for column in table_structure.columns:
            # Check by data type first
            if column.type.upper() in self.timestamp_data_types:
                timestamp_columns.append(column.name)
                continue
            
            # Check by column name patterns
            for pattern in patterns:
                if re.match(pattern, column.name.lower()):
                    timestamp_columns.append(column.name)
                    break
        
        # Add explicitly specified columns
        timestamp_columns.extend(self.options.explicit_timestamp_columns)
        
        return list(set(timestamp_columns))
    
    def detect_metadata_columns(self, table_structure: TableStructure, sample_data: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """Detect metadata columns that contain system-generated or audit information"""
        if not self.options.auto_detect_metadata:
            return self.options.explicit_metadata_columns.copy()
        
        metadata_columns = []
        
        # Get patterns to use
        patterns = self.options.metadata_patterns if self.options.metadata_patterns else self.default_metadata_patterns
        
        for column in table_structure.columns:
            # Check by column name patterns
            for pattern in patterns:
                if re.match(pattern, column.name.lower()):
                    metadata_columns.append(column.name)
                    break
        
        # Add pattern-based detection for common audit fields
        audit_patterns = [
            r'.*user$',    # author_user, editor_user, etc.
            r'.*_by$',     # created_by, updated_by, etc.
            r'.*_user$',   # created_user, modified_user, etc.
            r'.*source.*', # data_source, source_system, etc.
            r'.*system.*'  # system_id, source_system, etc.
        ]
        
        for column in table_structure.columns:
            for pattern in audit_patterns:
                if re.match(pattern, column.name.lower()):
                    metadata_columns.append(column.name)
                    break
        
        # Add explicitly specified columns
        metadata_columns.extend(self.options.explicit_metadata_columns)
        
        return list(set(metadata_columns))
    
    def detect_sequence_columns(self, table_structure: TableStructure, sample_data: Optional[List[Dict[str, Any]]] = None) -> List[str]:
        """Detect auto-increment, sequence, or system-generated ID columns"""
        if not self.options.auto_detect_sequences:
            return self.options.explicit_sequence_columns.copy()
        
        sequence_columns = []
        
        # Get patterns to use
        patterns = self.options.sequence_patterns if self.options.sequence_patterns else self.default_sequence_patterns
        
        for column in table_structure.columns:
            # Check by data type first (auto-increment types)
            if column.type.upper() in self.sequence_data_types:
                sequence_columns.append(column.name)
                continue
            
            # Check if it's a primary key with integer type (likely auto-increment)
            if column.is_primary_key and 'INT' in column.type.upper():
                sequence_columns.append(column.name)
                continue
            
            # Check by column name patterns
            for pattern in patterns:
                if re.match(pattern, column.name.lower()):
                    sequence_columns.append(column.name)
                    break
        
        # If we have sample data, check for sequential patterns
        if sample_data and len(sample_data) > 1:
            for column in table_structure.columns:
                if column.name not in sequence_columns and 'INT' in column.type.upper():
                    if self._appears_sequential(sample_data, column.name):
                        sequence_columns.append(column.name)
        
        # Add explicitly specified columns
        sequence_columns.extend(self.options.explicit_sequence_columns)
        
        return list(set(sequence_columns))
    
    def _get_excluded_columns(self, table_structure: TableStructure) -> List[str]:
        """Get user-specified excluded columns (both explicit and pattern-based)"""
        excluded_columns = []
        
        # Add explicitly specified columns
        excluded_columns.extend(self.options.excluded_columns)
        
        # Check pattern-based exclusions
        for column in table_structure.columns:
            for pattern in self.options.excluded_column_patterns:
                try:
                    if re.match(pattern.lower(), column.name.lower()):
                        excluded_columns.append(column.name)
                        break
                except re.error:
                    # Skip invalid regex patterns
                    continue
        
        return list(set(excluded_columns))
    
    def _appears_sequential(self, sample_data: List[Dict[str, Any]], column_name: str) -> bool:
        """Check if a column appears to contain sequential values (auto-increment)"""
        try:
            values = [row.get(column_name) for row in sample_data if row.get(column_name) is not None]
            if len(values) < 2:
                return False
            
            # Convert to integers
            int_values = []
            for val in values:
                if val is not None:
                    try:
                        int_values.append(int(val))
                    except (ValueError, TypeError):
                        return False
            
            # Sort and check if they form a sequence
            sorted_values = sorted(int_values)
            differences = [sorted_values[i+1] - sorted_values[i] for i in range(len(sorted_values)-1)]
            
            # If most differences are 1, it's likely sequential
            sequential_count = sum(1 for diff in differences if diff == 1)
            return sequential_count / len(differences) > 0.7
            
        except Exception:
            return False
    
    def get_all_excluded_columns(self, table_structure: TableStructure, 
                                uuid_columns: List[str], sample_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, List[str]]:
        """Get all columns that should be excluded from comparison"""
        
        timestamp_columns = self.detect_timestamp_columns(table_structure, sample_data)
        metadata_columns = self.detect_metadata_columns(table_structure, sample_data)
        sequence_columns = self.detect_sequence_columns(table_structure, sample_data)
        
        # Get user-specified excluded columns
        excluded_columns = self._get_excluded_columns(table_structure)
        
        # Combine all exclusions
        all_excluded = list(set(
            uuid_columns + 
            timestamp_columns + 
            metadata_columns + 
            sequence_columns +
            excluded_columns
        ))
        
        return {
            'uuid_columns': uuid_columns,
            'timestamp_columns': timestamp_columns,
            'metadata_columns': metadata_columns,
            'sequence_columns': sequence_columns,
            'excluded_columns': excluded_columns,
            'all_excluded': all_excluded
        }
    
    def get_exclusion_summary(self, exclusions: Dict[str, List[str]]) -> str:
        """Generate a human-readable summary of excluded columns"""
        summary_parts = []
        
        if exclusions['uuid_columns']:
            summary_parts.append(f"UUID columns: {', '.join(exclusions['uuid_columns'])}")
        
        if exclusions['timestamp_columns']:
            summary_parts.append(f"Timestamp columns: {', '.join(exclusions['timestamp_columns'])}")
        
        if exclusions['metadata_columns']:
            summary_parts.append(f"Metadata columns: {', '.join(exclusions['metadata_columns'])}")
        
        if exclusions['sequence_columns']:
            summary_parts.append(f"Sequence columns: {', '.join(exclusions['sequence_columns'])}")
        
        if exclusions.get('excluded_columns'):
            summary_parts.append(f"User-excluded columns: {', '.join(exclusions['excluded_columns'])}")
        
        if not summary_parts:
            return "No columns excluded from comparison"
        
        return "Excluded from comparison - " + "; ".join(summary_parts)
