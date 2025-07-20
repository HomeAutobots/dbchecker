# Enhanced Unique Identifier Detection and Normalization

## Overview

The enhanced UUID tracking system now supports intelligent detection and normalization of custom unique identifier patterns. This allows you to compare databases that use different naming conventions for unique identifiers while tracking their logical equivalence.

## Problem Solved

**Before**: Only traditional UUIDs (like `550e8400-e29b-41d4-a716-446655440000`) were detected and handled.

**Now**: Any custom unique identifier pattern can be detected, normalized, and tracked:
- `report-02035030325` vs `record-02035030325`
- `ABC123456` vs `DEF123456` 
- `user_001` vs `customer_001`
- And many more patterns...

## Key Features

### 1. Automatic Pattern Detection
The system automatically detects common unique identifier patterns:

- **prefix-number**: `report-123`, `user-456`
- **number-suffix**: `123-report`, `456-user`
- **prefix_number**: `report_123`, `user_456`
- **number_suffix**: `123_report`, `456_user`
- **code-number**: `ABC123456`, `DEF789012`
- **timestamp-serial**: `20240101-1234`

### 2. Pattern Normalization
Transform different patterns into equivalent forms for comparison:

```bash
# Example: Normalize report-123 and record-123 to id-123
--normalize-patterns '^(report|record)-(\d+)$:id-\2'
```

### 3. Logical Matching
Track how many records match logically even when raw unique IDs differ:

```
Raw UUID differences: 200 (all different, as expected)
Normalized matches: 200 (all logically equivalent)
```

## CLI Usage

### Basic Pattern Detection
```bash
dbchecker prod.db staging.db --uuid-comparison-mode include_with_tracking
```
Automatically detects and reports unique identifier patterns.

### Custom Pattern Detection
```bash
dbchecker prod.db staging.db \
  --uuid-comparison-mode include_with_tracking \
  --unique-id-patterns '^(report|record)-\d+$'
```
Explicitly specify what patterns to look for.

### Pattern Normalization
```bash
dbchecker prod.db staging.db \
  --uuid-comparison-mode include_with_tracking \
  --unique-id-patterns '^(report|record)-\d+$' \
  --normalize-patterns '^(report|record)-(\d+)$:id-\2'
```
Normalize `report-123` and `record-123` to `id-123` for comparison.

### Multiple Normalization Rules
```bash
dbchecker prod.db staging.db \
  --uuid-comparison-mode include_with_tracking \
  --normalize-patterns \
    '^(report|record)-(\d+)$:id-\2' \
    '^(old|new)_prefix_(\d+)$:normalized_\2'
```

## Programmatic Usage

```python
from dbchecker import DatabaseComparator, ComparisonOptions

options = ComparisonOptions(
    uuid_comparison_mode='include_with_tracking',
    
    # Detect custom patterns
    unique_id_patterns=[
        r'^(report|record)-\d+$',
        r'^[A-Z]{3}\d{6}$'
    ],
    
    # Normalize for comparison
    unique_id_normalize_patterns=[
        {
            'pattern': r'^(report|record)-(\d+)$',
            'replacement': r'id-\2'
        },
        {
            'pattern': r'^[A-Z]{3}(\d{6})$', 
            'replacement': r'code-\1'
        }
    ],
    
    verbose=True
)

comparator = DatabaseComparator(db1_path="prod.db", db2_path="staging.db")
comparator.set_comparison_options(options)

result = comparator.compare()

# Access pattern analysis
for table_name, table_comp in result.data_comparison.table_results.items():
    if table_comp.uuid_statistics:
        stats = table_comp.uuid_statistics
        print(f"Table: {table_name}")
        print(f"  Detected patterns: {stats.detected_patterns}")
        print(f"  Normalized matches: {stats.normalized_match_count}")
        print(f"  Raw differences: {stats.uuid_value_differences}")
```

## Real-World Examples

### Example 1: Production vs Staging with Different Prefixes
```
Production DB: report-001, report-002, report-003
Staging DB:    record-001, record-002, record-003

Normalization: ^(report|record)-(\d+)$ → id-\2
Result:        id-001, id-002, id-003 (100% logical match)
```

### Example 2: System Migration with ID Format Change
```
Old System:    user_12345, order_67890
New System:    customer_12345, purchase_67890

Normalization: ^(user|customer)_(\d+)$ → person_\2
               ^(order|purchase)_(\d+)$ → transaction_\2
Result:        100% logical data match verified
```

### Example 3: Multi-Environment Database Sync
```
Environment A: ABC123456, DEF789012
Environment B: XYZ123456, QRS789012

Pattern:       ^[A-Z]{3}(\d{6})$ (code-number pattern)
Normalization: ^[A-Z]{3}(\d{6})$ → code-\1
Result:        Tracks same underlying data despite different prefixes
```

## Output Analysis

### Pattern Detection Results
```
--- Detailed Pattern Detection Results ---
Table: reports
  UUID columns: ['id', 'author_id']
  Detected patterns: {'id': 'prefix-number', 'author_id': 'prefix-number'}
  Total UUID values DB1: 10
  Total UUID values DB2: 10
  Normalized matches: 10
  Raw UUID differences: 20
```

### Interpretation
- **Detected patterns**: System identified `prefix-number` pattern (e.g., `report-123`)
- **Total UUID values**: Both databases have same number of unique identifiers
- **Normalized matches**: All 10 records match logically after normalization
- **Raw UUID differences**: All 20 raw values differ (expected for different environments)

## Use Cases

### 1. Production vs Staging Validation
**Scenario**: Production uses `report-*` IDs, staging uses `record-*` IDs
**Solution**: Normalize both to `id-*` format to verify data integrity

### 2. Database Migration Verification
**Scenario**: Migrating from legacy system with `OLD_PREFIX_123` to new system with `NEW_PREFIX_123`
**Solution**: Normalize both to `ITEM_123` to validate migration completeness

### 3. Multi-Tenant Environment Sync
**Scenario**: Different tenants use different ID schemes (`TENANT_A_123` vs `TENANT_B_123`)
**Solution**: Normalize to `RECORD_123` to verify cross-tenant data consistency

### 4. Backup Validation Across Systems
**Scenario**: Backup system uses different ID prefixes for organizational purposes
**Solution**: Pattern normalization ensures backup integrity verification

## Pattern Syntax Reference

### Regex Pattern Examples
```bash
# Prefix-number patterns
'^(report|record|item)-\d+$'

# Number-suffix patterns  
'^\d+-(report|record|item)$'

# Underscore separated
'^(user|customer)_\d+$'

# Code-number patterns
'^[A-Z]{2,4}\d{4,}$'

# Timestamp-based
'^\d{8}-\d{4}$'
```

### Normalization Replacement Examples
```bash
# Convert prefix-number to standard format
'^(report|record)-(\d+)$:id-\2'

# Extract just the number part
'^[A-Z]{3}(\d+)$:num-\1'

# Swap order
'^(\d+)-(report|record)$:\2-\1'

# Add consistent prefix
'^(.+)_(\d+)$:item_\2'
```

## Benefits

1. **Universal Compatibility**: Works with any unique identifier scheme
2. **Logical Data Verification**: Confirms data relationships despite ID differences  
3. **Migration Confidence**: Validates data integrity across system changes
4. **Flexible Configuration**: Adapts to any naming convention
5. **Detailed Analytics**: Provides comprehensive pattern analysis and statistics
6. **Non-Breaking**: Maintains compatibility with traditional UUID handling

## Performance Considerations

- **Pattern Detection**: Minimal overhead, analyzes sample of values
- **Normalization**: Applied only during comparison, not stored
- **Memory Usage**: Scales with number of unique identifiers
- **Speed**: Optimized regex matching with compiled patterns

This enhancement makes the database comparison tool universally applicable to any unique identifier scheme while maintaining the ability to track logical data relationships and integrity.
