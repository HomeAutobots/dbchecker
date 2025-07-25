"""
Data models for the database comparison module.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any


@dataclass
class Column:
    """Represents a database column"""
    name: str
    type: str
    nullable: bool
    default: Optional[Any]
    is_primary_key: bool


@dataclass
class Index:
    """Represents a database index"""
    name: str
    table_name: str
    columns: List[str]
    unique: bool


@dataclass
class PrimaryKey:
    """Represents a primary key constraint"""
    columns: List[str]


@dataclass
class ForeignKey:
    """Represents a foreign key constraint"""
    columns: List[str]
    referenced_table: str
    referenced_columns: List[str]


@dataclass
class Constraint:
    """Represents a generic database constraint"""
    name: str
    type: str
    definition: str


@dataclass
class UniqueConstraint:
    """Represents a unique constraint"""
    name: str
    columns: List[str]


@dataclass
class CheckConstraint:
    """Represents a check constraint"""
    name: str
    expression: str


@dataclass
class Trigger:
    """Represents a database trigger"""
    name: str
    table_name: str
    event: str
    timing: str
    definition: str


@dataclass
class View:
    """Represents a database view"""
    name: str
    definition: str


@dataclass
class TableStructure:
    """Represents the structure of a database table"""
    name: str
    columns: List[Column]
    primary_key: Optional[PrimaryKey]
    foreign_keys: List[ForeignKey]
    unique_constraints: List[UniqueConstraint]
    check_constraints: List[CheckConstraint]


@dataclass
class DatabaseSchema:
    """Represents the complete schema of a database"""
    tables: Dict[str, TableStructure]
    views: List[View]
    triggers: List[Trigger]
    indexes: List[Index]


@dataclass
class FieldDifference:
    """Represents a difference in a specific field between two rows"""
    field_name: str
    value_db1: Any
    value_db2: Any


@dataclass
class RowDifference:
    """Represents differences between two rows"""
    row_identifier: str
    differences: List[FieldDifference]


@dataclass
class TableComparisonResult:
    """Represents the result of comparing two table structures"""
    table_name: str
    identical: bool
    missing_columns_db1: List[str]
    missing_columns_db2: List[str]
    column_differences: List[FieldDifference]


@dataclass
class UUIDStatistics:
    """Statistics about UUID columns and values in comparison"""
    uuid_columns: List[str]
    total_uuid_values_db1: int
    total_uuid_values_db2: int
    unique_uuid_values_db1: int
    unique_uuid_values_db2: int
    uuid_value_differences: int  # Number of UUID mismatches (expected for different databases)
    detected_patterns: Dict[str, str] = field(default_factory=dict)  # column -> detected pattern
    normalized_match_count: int = 0  # Number of records that match after pattern normalization


@dataclass
class TableDataComparison:
    """Represents the result of comparing data in two tables"""
    table_name: str
    row_count_db1: int
    row_count_db2: int
    matching_rows: int
    rows_only_in_db1: List[Dict]
    rows_only_in_db2: List[Dict]
    rows_with_differences: List[RowDifference]
    uuid_statistics: Optional[UUIDStatistics] = None


@dataclass
class SchemaComparisonResult:
    """Represents the result of comparing two database schemas"""
    identical: bool
    missing_in_db1: List[str]
    missing_in_db2: List[str]
    table_differences: Dict[str, TableComparisonResult]


@dataclass
class DataComparisonResult:
    """Represents the result of comparing data in two databases"""
    table_results: Dict[str, TableDataComparison]
    total_differences: int


@dataclass
class ComparisonSummary:
    """Summary statistics for the comparison"""
    total_tables: int
    identical_tables: int
    tables_with_differences: int
    total_rows_compared: int
    total_differences_found: int
    total_uuid_columns: int = 0
    total_uuid_values_db1: int = 0
    total_uuid_values_db2: int = 0
    uuid_integrity_check: bool = True  # True if UUID counts match between databases


@dataclass
class ComparisonResult:
    """Complete result of database comparison"""
    schema_comparison: Optional[SchemaComparisonResult]
    data_comparison: Optional[DataComparisonResult]
    summary: ComparisonSummary
    timestamp: datetime


@dataclass
class ComparisonOptions:
    """Configuration options for database comparison"""
    # UUID handling
    explicit_uuid_columns: List[str] = field(default_factory=list)
    auto_detect_uuids: bool = True
    uuid_patterns: List[str] = field(default_factory=list)
    uuid_comparison_mode: str = 'exclude'  # 'exclude', 'include_with_tracking', 'include_normal'
    
    # Unique identifier patterns for tracking (beyond traditional UUIDs)
    unique_id_patterns: List[str] = field(default_factory=list)  # e.g., r'^(report|record)-\d+$'
    unique_id_normalize_patterns: List[Dict[str, str]] = field(default_factory=list)  # Pattern normalization rules
    
    # Timestamp handling
    auto_detect_timestamps: bool = True
    explicit_timestamp_columns: List[str] = field(default_factory=list)
    timestamp_patterns: List[str] = field(default_factory=list)
    
    # Metadata exclusions
    auto_detect_metadata: bool = True
    explicit_metadata_columns: List[str] = field(default_factory=list)
    metadata_patterns: List[str] = field(default_factory=list)
    
    # Auto-increment/sequence exclusions
    auto_detect_sequences: bool = True
    explicit_sequence_columns: List[str] = field(default_factory=list)
    sequence_patterns: List[str] = field(default_factory=list)
    
    # Column exclusions - for user-specified columns to exclude from comparison
    excluded_columns: List[str] = field(default_factory=list)
    excluded_column_patterns: List[str] = field(default_factory=list)
    
    # Comparison options
    compare_schema: bool = True
    compare_data: bool = True
    case_sensitive: bool = True
    ignore_column_order: bool = True
    ignore_whitespace: bool = False
    
    # Performance options
    batch_size: int = 1000
    parallel_tables: bool = False  # Disabled by default due to SQLite threading limitations
    max_workers: int = 4
    
    # Output options
    output_format: List[str] = field(default_factory=lambda: ['json', 'html'])
    verbose: bool = False
    include_sample_data: bool = True
    max_differences_per_table: int = 100


@dataclass
class Report:
    """Generated report containing comparison results"""
    comparison_result: ComparisonResult
    format_type: str
    content: str
