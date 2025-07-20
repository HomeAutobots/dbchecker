# DBChecker - SQLite Database Comparator

A Python application that compares two SQLite databases for structural and data equality while intelligently ignoring UUID differences.

## Features

- **Schema Comparison**: Compare table structures, columns, constraints, indexes, triggers, and views
- **Data Comparison**: Compare actual data while handling UUID exclusions
- **UUID Detection**: Automatic detection of UUID columns plus support for explicit column specification
- **Multiple Output Formats**: JSON, HTML, Markdown, and CSV reports
- **Performance Optimized**: Configurable batch processing and parallel table comparison
- **Flexible Configuration**: Extensive options for customizing comparison behavior

## Installation

### From Source

```bash
cd dbchecker
pip install -e .
```

### Development Installation

```bash
cd dbchecker
pip install -e .[dev]
```

## Quick Start

### Command Line Usage

```bash
# Basic comparison
dbchecker database1.db database2.db

# With explicit UUID columns
dbchecker database1.db database2.db --uuid-columns id user_id entity_guid

# Schema only comparison
dbchecker database1.db database2.db --schema-only

# Generate multiple report formats
dbchecker database1.db database2.db --output-format json html markdown csv
```

### Programmatic Usage

```python
from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions

# Initialize comparator
comparator = DatabaseComparator(
    db1_path='database1.db',
    db2_path='database2.db',
    uuid_columns=['id', 'uuid', 'guid']
)

# Configure options
options = ComparisonOptions(
    auto_detect_uuids=True,
    compare_schema=True,
    compare_data=True,
    output_format=['json', 'html'],
    verbose=True
)
comparator.set_comparison_options(options)

# Run comparison
result = comparator.compare()

# Generate reports
comparator.generate_reports(result, output_dir='./reports')
```

## Architecture

The tool follows a modular architecture with the following core components:

- **DatabaseComparator**: Main controller orchestrating the comparison process
- **DatabaseConnector**: SQLite database abstraction layer
- **SchemaComparator**: Handles database structure comparison
- **DataComparator**: Manages data comparison with UUID handling
- **UUIDHandler**: Detects and manages UUID column exclusions
- **ReportGenerator**: Creates reports in multiple formats

## UUID Handling

The tool intelligently handles UUIDs in several ways:

1. **Explicit UUID Columns**: Specify columns by name using `--uuid-columns`
2. **Automatic Detection**: Detects common UUID patterns and column names
3. **Custom Patterns**: Add custom regex patterns for UUID detection
4. **Column Name Patterns**: Recognizes columns like `id`, `*_id`, `*uuid*`, `*guid*`

## Configuration Options

### Comparison Options
- `compare_schema`: Enable/disable schema comparison
- `compare_data`: Enable/disable data comparison
- `case_sensitive`: Case-sensitive string comparison
- `ignore_whitespace`: Ignore whitespace in string values

### Performance Options
- `batch_size`: Data processing batch size (default: 1000)
- `parallel_tables`: Enable parallel table processing
- `max_workers`: Maximum worker threads (default: 4)

### Output Options
- `output_format`: Report formats (json, html, markdown, csv)
- `verbose`: Detailed progress output
- `max_differences_per_table`: Limit differences per table in reports

## Examples

See `example.py` for comprehensive usage examples including:
- Comparing identical databases
- Handling databases with differences
- Report generation in multiple formats

## Testing

Run the test suite:

```bash
python -m pytest tests/
```

Run with coverage:

```bash
python -m pytest tests/ --cov=dbchecker --cov-report=html
```

## Use Cases

This tool is intended for:
- **Database Testing**: Ensure database operations produce identical results
- **Migration Validation**: Verify data integrity after migrations
- **Environment Synchronization**: Compare databases across environments
- **Regression Testing**: Detect unintended changes in database state
- **Data Quality Assurance**: Validate data consistency between systems

## Exit Codes

- `0`: No differences found
- `1`: Differences found between databases
- `2`: Comparison error (configuration, connection, etc.)
- `3`: Unexpected error
- `130`: Interrupted by user

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Run the test suite
5. Submit a pull request

## License

MIT License - see LICENSE file for details. 
