#!/usr/bin/env python3
"""
Demo script showcasing the DBChecker test reporting system.

This script demonstrates all the features of the comprehensive test reporting
system including coverage analysis, report generation, and comparison tools.
"""

import subprocess
import sys
import os
import time
from pathlib import Path

def run_command(cmd, description):
    """Run a command and print its output."""
    print(f"\n🔄 {description}")
    print("=" * 60)
    print(f"Command: {cmd}")
    print("-" * 60)
    
    result = subprocess.run(cmd, shell=True, capture_output=False)
    
    print(f"\n✅ Command completed with exit code: {result.returncode}")
    return result.returncode == 0

def main():
    """Main demo workflow."""
    print("🚀 DBChecker Test Reporting System Demo")
    print("=" * 70)
    print("This demo will showcase the comprehensive test reporting capabilities.")
    print("We'll run tests, generate reports, and demonstrate analysis tools.")
    
    # Ensure we're in the right directory
    project_root = Path(__file__).parent
    os.chdir(project_root)
    
    print(f"\n📁 Working directory: {project_root}")
    
    # Step 1: Run unit tests with coverage
    success = run_command(
        "python -m pytest test/unit -v --cov=dbchecker --cov-report=html:test_reports/demo_coverage_html --cov-report=term-missing",
        "Running unit tests with coverage analysis"
    )
    
    if not success:
        print("⚠️  Some tests failed, but continuing with demo...")
    
    # Step 2: Generate a pytest report with our enhanced runner
    run_command(
        "python run_pytest.py unit -q",
        "Generating comprehensive test report with pytest runner"
    )
    
    # Step 3: List available reports
    run_command(
        "python view_reports.py list",
        "Listing all available test reports"
    )
    
    # Step 4: Show latest report
    run_command(
        "python view_reports.py show --latest",
        "Displaying latest test report summary"
    )
    
    # Step 5: Run functional tests for comparison
    print(f"\n⏳ Waiting 2 seconds before running functional tests...")
    time.sleep(2)
    
    run_command(
        "python run_pytest.py functional -q",
        "Running functional tests for comparison"
    )
    
    # Step 6: List reports again to show new ones
    run_command(
        "python view_reports.py list",
        "Listing reports after functional test run"
    )
    
    # Step 7: Show test structure and files
    print(f"\n📊 Generated Test Reports:")
    print("=" * 60)
    reports_dir = project_root / "test_reports"
    if reports_dir.exists():
        for item in sorted(reports_dir.iterdir()):
            if item.is_file():
                size = item.stat().st_size
                print(f"📄 {item.name} ({size:,} bytes)")
            elif item.is_dir():
                file_count = len(list(item.glob("*")))
                print(f"📁 {item.name}/ ({file_count} files)")
    
    # Step 8: Display coverage summary
    print(f"\n📈 Coverage Summary:")
    print("=" * 60)
    run_command(
        "python -c \"import json; data=json.load(open('test_reports/latest_test_summary.json')); print(f'Tests: {data.get(\\\"statistics\\\", {}).get(\\\"total\\\", \\\"N/A\\\")}'); print(f'Passed: {data.get(\\\"statistics\\\", {}).get(\\\"passed\\\", \\\"N/A\\\")}'); print(f'Coverage: {data.get(\\\"coverage\\\", {}).get(\\\"percent_covered\\\", \\\"N/A\\\")}%')\" 2>/dev/null || echo 'Coverage data not available'",
        "Extracting key metrics from latest report"
    )
    
    # Step 9: Show how to open HTML reports
    print(f"\n🌐 HTML Reports Available:")
    print("=" * 60)
    coverage_html = reports_dir / "demo_coverage_html" / "index.html"
    if coverage_html.exists():
        print(f"📊 Coverage Report: {coverage_html}")
        print("   Open this file in your browser for interactive coverage analysis")
    
    pytest_html = list(reports_dir.glob("pytest_report_*.html"))
    if pytest_html:
        print(f"📋 Test Report: {pytest_html[0]}")
        print("   Open this file in your browser for detailed test results")
    
    # Step 10: Show next steps
    print(f"\n🎯 Next Steps:")
    print("=" * 60)
    print("1. Open the HTML coverage report in your browser:")
    if coverage_html.exists():
        print(f"   open {coverage_html}")
    
    print("\n2. Compare test reports:")
    print("   python view_reports.py compare --report1 <old> --report2 <new>")
    
    print("\n3. View detailed failures:")
    print("   python view_reports.py failures --latest")
    
    print("\n4. Run all tests with enhanced runner:")
    print("   python test/run_tests.py")
    
    print("\n5. Monitor coverage trends:")
    print("   Run tests regularly and compare coverage reports")
    
    print(f"\n🎉 Demo completed! Check the test_reports/ directory for all generated files.")
    print("=" * 70)

if __name__ == "__main__":
    main()
