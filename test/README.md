# DBChecker Test Suite

This directory contains the comprehensive test suite for the dbchecker package.

## Directory Structure

```
dbchecker/test/
├── __init__.py
├── conftest.py                 # Test configuration and fixtures
├── run_tests.py               # Main test runner script
├── unit/                      # Unit tests for individual components
│   ├── __init__.py
│   ├── test_database_connector.py
│   ├── test_schema_comparator.py
│   ├── test_data_comparator.py
│   ├── test_report_generator.py
│   ├── test_exception_handling.py
│   ├── test_uuid_handler.py
│   └── test_timestamp_detection.py
└── functional/                # Functional/integration tests
    ├── __init__.py
    ├── test_enhanced_uuid.py
    ├── test_timestamp_exclusion.py
    ├── test_uuid_tracking.py
    ├── test_enhanced_metadata_exclusion.py
    ├── test_column_exclusion.py
    └── test_cli_exclusion.py
```

## Running Tests

### All Tests
```bash
cd dbchecker/test
python run_tests.py
```

### Unit Tests Only
```bash
cd dbchecker/test
python run_tests.py unit
```

### Functional Tests Only
```bash
cd dbchecker/test
python run_tests.py functional
```

### Individual Test Files
```bash
cd dbchecker/test
python -m unittest unit.test_database_connector
python -m unittest functional.test_enhanced_uuid
```

## Test Categories

### Unit Tests
- **test_database_connector.py** - Database connection and query functionality
- **test_schema_comparator.py** - Schema comparison logic
- **test_data_comparator.py** - Data comparison algorithms
- **test_report_generator.py** - Report generation in multiple formats
- **test_exception_handling.py** - Error handling and edge cases
- **test_uuid_handler.py** - UUID detection and handling
- **test_timestamp_detection.py** - Timestamp detection and exclusion

### Functional Tests
- **test_enhanced_uuid.py** - Enhanced UUID functionality
- **test_timestamp_exclusion.py** - Timestamp exclusion features
- **test_uuid_tracking.py** - UUID tracking capabilities
- **test_enhanced_metadata_exclusion.py** - Metadata exclusion patterns
- **test_column_exclusion.py** - Column exclusion functionality
- **test_cli_exclusion.py** - CLI-based exclusion features

## Test Coverage

The test suite provides comprehensive coverage including:
- ✅ Database connectivity and error handling
- ✅ Schema extraction and comparison
- ✅ Data comparison with various exclusion patterns
- ✅ Report generation in multiple formats (JSON, HTML, Markdown, CSV)
- ✅ UUID detection, tracking, and exclusion
- ✅ Timestamp detection and exclusion
- ✅ Error conditions and edge cases
- ✅ Performance testing with large datasets
- ✅ Unicode and special character handling

## Requirements

The tests are designed to run with Python's built-in unittest framework. No additional dependencies are required beyond what's needed for the dbchecker package itself.

## Test Data

Tests use temporary SQLite databases created during execution. All test databases are automatically cleaned up after tests complete.
