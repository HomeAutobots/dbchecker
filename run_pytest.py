#!/usr/bin/env python3
"""
Pytest runner with comprehensive reporting for dbchecker.

This script provides a convenient way to run pytest with all the configured
reporting options and generates comprehensive test and coverage reports.
"""

import subprocess
import sys
import os
from pathlib import Path
import argparse
import json
from datetime import datetime

def setup_environment():
    """Setup the environment for running tests."""
    # Get the project root
    script_dir = Path(__file__).parent
    project_root = script_dir
    
    # Ensure test_reports directory exists
    reports_dir = project_root / "test_reports"
    reports_dir.mkdir(exist_ok=True)
    
    # Add project root to Python path
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        
    return project_root, reports_dir

def run_pytest_with_coverage(test_type="all", verbose=True):
    """Run pytest with comprehensive coverage reporting."""
    project_root, reports_dir = setup_environment()
    
    # Base pytest command
    cmd = [sys.executable, "-m", "pytest"]
    
    # Add test paths based on type
    if test_type == "unit":
        cmd.append("test/unit")
    elif test_type == "functional":
        cmd.append("test/functional")
    elif test_type == "all":
        cmd.extend(["test/unit", "test/functional"])
    else:
        print(f"Unknown test type: {test_type}")
        return False
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add coverage and reporting options (these are in pytest.ini, but we can override)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    additional_args = [
        f"--html={reports_dir}/pytest_report_{timestamp}.html",
        f"--json-report-file={reports_dir}/pytest_report_{timestamp}.json",
        f"--cov-report=html:{reports_dir}/pytest_coverage_html_{timestamp}",
        f"--cov-report=xml:{reports_dir}/pytest_coverage_{timestamp}.xml",
        f"--cov-report=json:{reports_dir}/pytest_coverage_{timestamp}.json"
    ]
    
    cmd.extend(additional_args)
    
    print(f"Running pytest for {test_type} tests...")
    print("Command:", " ".join(cmd))
    print("=" * 60)
    
    # Run pytest
    try:
        result = subprocess.run(cmd, cwd=project_root, capture_output=False)
        
        # Generate summary report
        generate_summary_report(reports_dir, timestamp, test_type, result.returncode == 0)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"Error running pytest: {e}")
        return False

def generate_summary_report(reports_dir, timestamp, test_type, success):
    """Generate a summary report combining pytest results."""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "test_type": test_type,
        "success": success,
        "reports": {
            "html": f"pytest_report_{timestamp}.html",
            "json": f"pytest_report_{timestamp}.json",
            "coverage_html": f"pytest_coverage_html_{timestamp}/index.html",
            "coverage_xml": f"pytest_coverage_{timestamp}.xml",
            "coverage_json": f"pytest_coverage_{timestamp}.json"
        }
    }
    
    # Try to extract test statistics from JSON report
    json_report_path = reports_dir / f"pytest_report_{timestamp}.json"
    if json_report_path.exists():
        try:
            with open(json_report_path, 'r') as f:
                pytest_data = json.load(f)
                
            summary["statistics"] = {
                "total": pytest_data.get("summary", {}).get("total", 0),
                "passed": pytest_data.get("summary", {}).get("passed", 0),
                "failed": pytest_data.get("summary", {}).get("failed", 0),
                "error": pytest_data.get("summary", {}).get("error", 0),
                "skipped": pytest_data.get("summary", {}).get("skipped", 0)
            }
            
            summary["duration"] = pytest_data.get("duration", 0)
            
        except Exception as e:
            print(f"Warning: Could not parse pytest JSON report: {e}")
    
    # Try to extract coverage data
    coverage_json_path = reports_dir / f"pytest_coverage_{timestamp}.json"
    if coverage_json_path.exists():
        try:
            with open(coverage_json_path, 'r') as f:
                coverage_data = json.load(f)
                
            summary["coverage"] = {
                "percent_covered": coverage_data.get("totals", {}).get("percent_covered", 0),
                "num_statements": coverage_data.get("totals", {}).get("num_statements", 0),
                "missing_lines": coverage_data.get("totals", {}).get("missing_lines", 0),
                "covered_lines": coverage_data.get("totals", {}).get("covered_lines", 0)
            }
            
        except Exception as e:
            print(f"Warning: Could not parse coverage JSON report: {e}")
    
    # Save summary
    summary_path = reports_dir / f"test_summary_{timestamp}.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Also save as latest summary
    latest_summary_path = reports_dir / "latest_test_summary.json"
    with open(latest_summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Test Type: {test_type}")
    print(f"Success: {'✓' if success else '✗'}")
    
    if "statistics" in summary:
        stats = summary["statistics"]
        print(f"Total Tests: {stats['total']}")
        print(f"Passed: {stats['passed']}")
        print(f"Failed: {stats['failed']}")
        print(f"Errors: {stats['error']}")
        print(f"Skipped: {stats['skipped']}")
        if stats['total'] > 0:
            success_rate = (stats['passed'] / stats['total']) * 100
            print(f"Success Rate: {success_rate:.1f}%")
    
    if "coverage" in summary:
        cov = summary["coverage"]
        print(f"Coverage: {cov['percent_covered']:.1f}%")
        print(f"Covered Lines: {cov['covered_lines']}/{cov['num_statements']}")
    
    if "duration" in summary:
        print(f"Duration: {summary['duration']:.2f}s")
    
    print("\nGenerated Reports:")
    for report_type, filename in summary["reports"].items():
        report_path = reports_dir / filename
        if report_path.exists() or report_type.endswith('_html'):  # HTML reports are directories
            print(f"  {report_type}: {report_path}")
    
    print(f"\nSummary saved to: {summary_path}")
    print(f"Latest summary: {latest_summary_path}")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Run pytest with comprehensive reporting')
    parser.add_argument('test_type', nargs='?', default='all',
                       choices=['all', 'unit', 'functional'],
                       help='Type of tests to run (default: all)')
    parser.add_argument('-q', '--quiet', action='store_true',
                       help='Run with minimal output')
    parser.add_argument('--no-cov', action='store_true',
                       help='Skip coverage reporting')
    
    args = parser.parse_args()
    
    # Override pytest.ini if no coverage requested
    if args.no_cov:
        # This would require modifying the command, but for now we'll just warn
        print("Warning: --no-cov not implemented yet, coverage will still be generated")
    
    success = run_pytest_with_coverage(
        test_type=args.test_type,
        verbose=not args.quiet
    )
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()
