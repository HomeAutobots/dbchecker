# User-Specified Column Exclusions Feature

## Overview

This feature adds the ability for users to exclude specific columns from database comparisons. This is useful when you have columns that contain environment-specific data, debug information, internal notes, or other data that should not be compared between databases.

## Implementation Details

### New Configuration Options in `ComparisonOptions`

1. **`excluded_columns`**: List of specific column names to exclude
2. **`excluded_column_patterns`**: List of regex patterns to match column names for exclusion

### CLI Interface

New command-line arguments:
- `--exclude-columns`: Specify column names to exclude
- `--exclude-column-patterns`: Specify regex patterns for columns to exclude

### Examples

#### Command Line Usage

```bash
# Exclude specific columns
dbchecker db1.sqlite db2.sqlite --exclude-columns internal_notes debug_field admin_comments

# Exclude columns by pattern
dbchecker db1.sqlite db2.sqlite --exclude-column-patterns ".*debug.*" ".*temp.*" ".*internal.*"

# Combine with existing features
dbchecker db1.sqlite db2.sqlite --uuid-columns id --exclude-columns notes created_by
```

#### Programmatic Usage

```python
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

options = ComparisonOptions(
    # Explicit column exclusions
    excluded_columns=['internal_notes', 'debug_info', 'admin_comments'],
    
    # Pattern-based exclusions
    excluded_column_patterns=[r'.*temp.*', r'.*debug.*', r'.*internal.*'],
    
    # Works alongside existing exclusions
    auto_detect_uuids=True,
    auto_detect_timestamps=True,
    verbose=True
)

comparator = DatabaseComparator(db1_path, db2_path)
comparator.set_comparison_options(options)
result = comparator.compare()
```

## Use Cases

1. **Environment-Specific Data**: Exclude columns that contain environment-specific information (e.g., server names, environment tags)

2. **Debug/Development Fields**: Exclude debug flags, development notes, or testing data

3. **Audit/Metadata Fields**: Exclude internal audit trails, system metadata that differs between environments

4. **Temporary Data**: Exclude fields used for temporary processing or caching

5. **User-Specific Content**: Exclude columns containing user-generated content that varies (notes, comments)

## How It Works

1. **Detection Phase**: The system identifies columns to exclude based on:
   - Explicit column names provided by the user
   - Regex patterns matching column names

2. **Integration**: User exclusions are combined with automatic exclusions (UUIDs, timestamps, metadata, sequences)

3. **Comparison**: Excluded columns are ignored during data comparison

4. **Reporting**: The exclusion summary shows which columns were excluded and why

## Testing

The feature includes comprehensive tests:
- `test_column_exclusion.py`: Tests explicit, pattern-based, and combined exclusions
- `test_cli_exclusion.py`: Tests CLI interface
- `example_column_exclusion.py`: Demonstrates real-world usage

## Benefits

1. **Flexible Configuration**: Both explicit names and patterns supported
2. **Environment Agnostic**: Compare databases across environments ignoring environment-specific fields
3. **No Code Changes**: Can be configured via CLI without modifying code
4. **Combines with Existing Features**: Works alongside UUID, timestamp, and metadata exclusions
5. **Clear Reporting**: Shows which columns were excluded in verbose output

## Technical Implementation

### Files Modified

1. **`models.py`**: Added `excluded_columns` and `excluded_column_patterns` to `ComparisonOptions`
2. **`metadata_detector.py`**: Added `_get_excluded_columns()` method and updated exclusion logic
3. **`cli.py`**: Added CLI arguments and configuration
4. **`README.md`**: Updated documentation with examples

### Integration Points

- Integrates with existing `MetadataDetector` class
- Uses the same exclusion pipeline as automatic detection
- Appears in verbose output and exclusion summaries
- Compatible with all existing comparison options

This feature provides a powerful and flexible way to customize database comparisons for specific use cases while maintaining compatibility with all existing functionality.
