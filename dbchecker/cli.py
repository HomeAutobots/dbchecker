#!/usr/bin/env python3
"""
Command-line interface for the SQLite Database Comparator.
"""

import argparse
import sys
import os
from pathlib import Path

from dbchecker.comparator import DatabaseComparator
from dbchecker.models import ComparisonOptions
from dbchecker.exceptions import DatabaseComparisonError


def main():
    """Main entry point for the command-line interface"""
    parser = argparse.ArgumentParser(
        description="Compare two SQLite databases for structural and data equality while ignoring UUID differences"
    )
    
    # Required arguments
    parser.add_argument("database1", help="Path to first SQLite database")
    parser.add_argument("database2", help="Path to second SQLite database")
    
    # UUID handling options
    parser.add_argument(
        "--uuid-columns", 
        nargs="*", 
        default=[],
        help="Explicit UUID column names to ignore during comparison"
    )
    parser.add_argument(
        "--no-auto-detect-uuids", 
        action="store_true",
        help="Disable automatic UUID detection"
    )
    parser.add_argument(
        "--uuid-patterns",
        nargs="*",
        default=[],
        help="Custom regex patterns for UUID detection"
    )
    
    # Column exclusion options
    parser.add_argument(
        "--exclude-columns",
        nargs="*",
        default=[],
        help="Specific column names to exclude from comparison"
    )
    parser.add_argument(
        "--exclude-column-patterns",
        nargs="*",
        default=[],
        help="Regex patterns for column names to exclude from comparison"
    )
    
    # Comparison options
    parser.add_argument(
        "--schema-only", 
        action="store_true",
        help="Compare only database schemas, skip data comparison"
    )
    parser.add_argument(
        "--data-only", 
        action="store_true",
        help="Compare only data, skip schema comparison"
    )
    parser.add_argument(
        "--case-insensitive", 
        action="store_true",
        help="Perform case-insensitive comparison"
    )
    parser.add_argument(
        "--ignore-whitespace", 
        action="store_true",
        help="Ignore whitespace differences in string values"
    )
    
    # Performance options
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=1000,
        help="Batch size for data processing (default: 1000)"
    )
    parser.add_argument(
        "--no-parallel", 
        action="store_true",
        help="Disable parallel table processing"
    )
    parser.add_argument(
        "--max-workers", 
        type=int, 
        default=4,
        help="Maximum number of worker threads (default: 4)"
    )
    
    # Output options
    parser.add_argument(
        "--output-dir", 
        default=".",
        help="Output directory for reports (default: current directory)"
    )
    parser.add_argument(
        "--output-format", 
        nargs="*", 
        choices=["json", "html", "markdown", "csv"],
        default=["json", "html"],
        help="Output formats for reports (default: json html)"
    )
    parser.add_argument(
        "--filename-prefix", 
        default="comparison_report",
        help="Prefix for output filenames (default: comparison_report)"
    )
    parser.add_argument(
        "--max-differences", 
        type=int, 
        default=100,
        help="Maximum differences to include per table (default: 100)"
    )
    
    # Verbosity
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--quiet", "-q", 
        action="store_true",
        help="Suppress all output except errors"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.schema_only and args.data_only:
        print("Error: Cannot specify both --schema-only and --data-only", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(args.database1):
        print(f"Error: Database file not found: {args.database1}", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(args.database2):
        print(f"Error: Database file not found: {args.database2}", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    try:
        # Configure comparison options
        options = ComparisonOptions(
            explicit_uuid_columns=args.uuid_columns,
            auto_detect_uuids=not args.no_auto_detect_uuids,
            uuid_patterns=args.uuid_patterns,
            excluded_columns=args.exclude_columns,
            excluded_column_patterns=args.exclude_column_patterns,
            compare_schema=not args.data_only,
            compare_data=not args.schema_only,
            case_sensitive=not args.case_insensitive,
            ignore_whitespace=args.ignore_whitespace,
            batch_size=args.batch_size,
            parallel_tables=not args.no_parallel,
            max_workers=args.max_workers,
            output_format=args.output_format,
            verbose=args.verbose and not args.quiet,
            max_differences_per_table=args.max_differences
        )
        
        # Initialize and run comparison
        if not args.quiet:
            print(f"Comparing databases:")
            print(f"  Database 1: {args.database1}")
            print(f"  Database 2: {args.database2}")
            print()
        
        comparator = DatabaseComparator(
            db1_path=args.database1,
            db2_path=args.database2,
            uuid_columns=args.uuid_columns
        )
        comparator.set_comparison_options(options)
        
        # Run comparison
        result = comparator.compare()
        
        # Generate reports
        comparator.generate_reports(
            result, 
            output_dir=args.output_dir, 
            filename_prefix=args.filename_prefix
        )
        
        # Print summary
        if not args.quiet:
            print("\nComparison Summary:")
            print(f"  Total tables: {result.summary.total_tables}")
            print(f"  Identical tables: {result.summary.identical_tables}")
            print(f"  Tables with differences: {result.summary.tables_with_differences}")
            print(f"  Total rows compared: {result.summary.total_rows_compared}")
            print(f"  Total differences found: {result.summary.total_differences_found}")
            print()
            
            if result.schema_comparison:
                schema_status = "identical" if result.schema_comparison.identical else "different"
                print(f"  Schema: {schema_status}")
            
            if result.data_comparison:
                print(f"  Data differences: {result.data_comparison.total_differences}")
            
            print()
            print("Reports generated:")
            for format_type in args.output_format:
                filename = f"{args.filename_prefix}.{format_type}"
                filepath = os.path.join(args.output_dir, filename)
                print(f"  {filepath}")
        
        # Set exit code based on results
        if result.summary.tables_with_differences > 0 or result.summary.total_differences_found > 0:
            sys.exit(1)  # Differences found
        else:
            sys.exit(0)  # No differences
            
    except DatabaseComparisonError as e:
        print(f"Comparison error: {e}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        print("\nComparison interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(3)


if __name__ == "__main__":
    main()
