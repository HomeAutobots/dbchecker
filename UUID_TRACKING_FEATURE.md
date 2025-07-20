# UUID Tracking Feature - Implementation Guide

## Overview

The UUID tracking feature allows you to include UUID columns in database comparisons while acknowledging their unique nature. This is particularly useful when you need to verify data relationships across tables even though the UUIDs themselves will be different between databases.

## Problem Statement

Previously, the dbchecker tool excluded UUID columns entirely from comparisons because UUIDs are unique by design and will differ between database environments. However, UUIDs serve important purposes:

1. **Data Relationships**: UUIDs link records across tables (foreign key relationships)
2. **Data Integrity**: The count and distribution of UUIDs can indicate data completeness
3. **Business Logic**: Understanding UUID usage patterns helps verify database structure

## Solution: UUID Comparison Modes

The tool now offers three UUID handling modes:

### 1. `exclude` (Default/Traditional)
- **Behavior**: Completely excludes UUID columns from comparison
- **Use Case**: When you only care about business data and want to ignore all UUID differences
- **Result**: Databases with identical business data but different UUIDs will show as identical

### 2. `include_with_tracking` (New)
- **Behavior**: Includes UUIDs in comparison but tracks detailed statistics
- **Use Case**: When you want to verify data relationships and integrity while acknowledging UUID differences
- **Features**:
  - Tracks total UUID count per database
  - Tracks unique UUID count per database
  - Verifies UUID count consistency (same number of UUIDs = same amount of data)
  - Reports UUID statistics in comparison results
  - Helps identify data integrity issues

### 3. `include_normal`
- **Behavior**: Treats UUIDs as regular columns (will show many differences)
- **Use Case**: When you want to see all UUID differences explicitly
- **Result**: Will show differences for every UUID mismatch

## Implementation Details

### New Data Models

#### UUIDStatistics
```python
@dataclass
class UUIDStatistics:
    uuid_columns: List[str]
    total_uuid_values_db1: int
    total_uuid_values_db2: int
    unique_uuid_values_db1: int
    unique_uuid_values_db2: int
    uuid_value_differences: int
```

#### Enhanced ComparisonSummary
```python
@dataclass
class ComparisonSummary:
    # ... existing fields ...
    total_uuid_columns: int = 0
    total_uuid_values_db1: int = 0
    total_uuid_values_db2: int = 0
    uuid_integrity_check: bool = True
```

### Command Line Usage

```bash
# Traditional mode (default)
dbchecker db1.db db2.db

# UUID tracking mode  
dbchecker db1.db db2.db --uuid-comparison-mode include_with_tracking

# Treat UUIDs as normal columns
dbchecker db1.db db2.db --uuid-comparison-mode include_normal

# Combine with verbose output to see UUID statistics
dbchecker db1.db db2.db --uuid-comparison-mode include_with_tracking --verbose
```

### Programmatic Usage

```python
from dbchecker import DatabaseComparator, ComparisonOptions

options = ComparisonOptions(
    uuid_comparison_mode='include_with_tracking',
    auto_detect_uuids=True,
    verbose=True
)

comparator = DatabaseComparator(db1_path="prod.db", db2_path="staging.db")
comparator.set_comparison_options(options)

result = comparator.compare()

# Access UUID statistics
print(f"Total UUID columns: {result.summary.total_uuid_columns}")
print(f"UUID integrity check: {result.summary.uuid_integrity_check}")

# Per-table statistics
for table_name, table_comp in result.data_comparison.table_results.items():
    if table_comp.uuid_statistics:
        stats = table_comp.uuid_statistics
        print(f"Table {table_name}: {len(stats.uuid_columns)} UUID columns")
        print(f"  UUID count DB1: {stats.total_uuid_values_db1}")
        print(f"  UUID count DB2: {stats.total_uuid_values_db2}")
```

## Use Cases

### 1. Production vs Staging Verification
```bash
# Verify that staging has the same data relationships as production
dbchecker production.db staging.db --uuid-comparison-mode include_with_tracking --verbose
```

**Expected Output:**
- Tables show as identical (business data matches)
- UUID statistics show matching counts
- UUID integrity check passes

### 2. Data Migration Validation
```bash
# Verify migration preserved all data relationships
dbchecker old_system.db new_system.db --uuid-comparison-mode include_with_tracking
```

**What to Check:**
- Same number of UUIDs in each table
- Same number of total records
- UUID integrity check passes

### 3. Backup Verification
```bash
# Verify backup contains all data with correct relationships
dbchecker production.db backup.db --uuid-comparison-mode include_with_tracking
```

## Benefits

1. **Data Relationship Verification**: Ensures foreign key relationships are preserved
2. **Data Completeness Check**: UUID counts indicate if any records were lost
3. **Integrity Validation**: Verifies database structure consistency
4. **Troubleshooting**: Helps identify data synchronization issues
5. **Audit Trail**: Provides detailed statistics for compliance reporting

## Technical Implementation

### UUID Detection
- **Automatic**: Detects columns with UUID patterns (`.*uuid.*`, `.*guid.*`)
- **Explicit**: Specify columns with `--uuid-columns`
- **Custom Patterns**: Add patterns with `--uuid-patterns`

### Statistics Collection
- Counts total UUID values per column
- Tracks unique UUID values
- Compares counts between databases
- Reports discrepancies

### Performance Considerations
- UUID tracking adds minimal overhead
- Statistics collected during normal comparison process
- Memory usage scales with number of UUID values

## Example Scenarios

### Scenario 1: Identical Data, Different UUIDs
```
Database 1: 100 users with UUIDs user-001 to user-100
Database 2: 100 users with UUIDs user-a01 to user-a100

Result with tracking mode:
✅ Tables identical (business data matches)
✅ UUID integrity check passes (same count)
📊 UUID statistics: 100 UUIDs in each database
```

### Scenario 2: Missing Records
```
Database 1: 100 users
Database 2: 95 users (5 missing)

Result with tracking mode:
❌ Tables different (row count mismatch)
❌ UUID integrity check fails (different counts)
📊 UUID statistics: 100 vs 95 UUIDs
```

### Scenario 3: Data Corruption
```
Database 1: Clean data with proper UUIDs
Database 2: Some UUIDs corrupted/null

Result with tracking mode:
❌ Tables different (null values)
❌ UUID integrity check fails
📊 UUID statistics show discrepancy
```

## Migration Guide

### From Traditional Mode
If you're currently using:
```bash
dbchecker db1.db db2.db --uuid-columns id user_id
```

To enable tracking:
```bash
dbchecker db1.db db2.db --uuid-columns id user_id --uuid-comparison-mode include_with_tracking
```

### No Breaking Changes
- Default behavior remains unchanged
- Existing scripts continue to work
- New features are opt-in

## Future Enhancements

1. **Cross-table UUID validation**: Verify foreign key relationships
2. **UUID format validation**: Ensure UUIDs follow expected patterns
3. **Performance optimization**: Parallel UUID analysis
4. **Enhanced reporting**: Visual UUID relationship maps

This feature provides a powerful way to verify data integrity while respecting the unique nature of UUIDs, making it ideal for production database validation scenarios.
