#!/usr/bin/env python3
"""
Comprehensive test reporting system for dbchecker package.

This module provides advanced test reporting capabilities including:
- Code coverage analysis with HTML and JSON reports
- Detailed unit test reports with timing and results
- Compiled test summaries with trends
- Coverage gap analysis
"""

import json
import time
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import unittest
from unittest.result import TestResult

try:
    import coverage
except ImportError:
    print("Coverage package not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "coverage"])
    import coverage


class TestReporter:
    """Comprehensive test reporter with coverage analysis."""
    
    def __init__(self, project_root: Path, reports_dir: Optional[Path] = None):
        """Initialize the test reporter.
        
        Args:
            project_root: Root directory of the project
            reports_dir: Directory to store reports (default: project_root/test_reports)
        """
        self.project_root = project_root
        self.reports_dir = reports_dir or project_root / "test_reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        # Initialize coverage
        self.cov = coverage.Coverage(
            source=[str(project_root / "dbchecker")],
            omit=[
                "*/test/*",
                "*/tests/*", 
                "*/__pycache__/*",
                "*/setup.py",
                "*/.venv/*",
                "*/venv/*"
            ]
        )
        
        self.test_start_time = None
        self.test_results = {}
        
    def start_coverage(self):
        """Start coverage monitoring."""
        self.cov.start()
        self.test_start_time = time.time()
        
    def stop_coverage(self):
        """Stop coverage monitoring."""
        self.cov.stop()
        
    def generate_coverage_reports(self) -> Dict[str, Any]:
        """Generate comprehensive coverage reports.
        
        Returns:
            Dict containing coverage statistics and report paths
        """
        # Save coverage data
        self.cov.save()
        
        # Generate HTML report
        html_dir = self.reports_dir / "coverage_html"
        html_dir.mkdir(exist_ok=True)
        self.cov.html_report(directory=str(html_dir))
        
        # Generate JSON report
        json_file = self.reports_dir / "coverage.json"
        self.cov.json_report(outfile=str(json_file))
        
        # Generate XML report (for CI/CD)
        xml_file = self.reports_dir / "coverage.xml"
        self.cov.xml_report(outfile=str(xml_file))
        
        # Get coverage statistics
        total_coverage = self.cov.report(show_missing=False)
        
        # Get detailed coverage data
        coverage_data = self.cov.get_data()
        files_coverage = {}
        
        for filename in coverage_data.measured_files():
            rel_path = os.path.relpath(filename, self.project_root)
            analysis = self.cov.analysis2(filename)
            
            executed_lines = len(analysis.executed)
            missing_lines = len(analysis.missing)
            total_lines = executed_lines + missing_lines
            
            if total_lines > 0:
                file_coverage = (executed_lines / total_lines) * 100
            else:
                file_coverage = 100.0
                
            files_coverage[rel_path] = {
                'coverage_percent': round(file_coverage, 2),
                'executed_lines': executed_lines,
                'missing_lines': missing_lines,
                'total_lines': total_lines,
                'missing_line_numbers': list(analysis.missing),
                'excluded_lines': list(analysis.excluded)
            }
        
        coverage_summary = {
            'total_coverage': round(total_coverage, 2),
            'timestamp': datetime.now().isoformat(),
            'files': files_coverage,
            'reports': {
                'html': str(html_dir / "index.html"),
                'json': str(json_file),
                'xml': str(xml_file)
            }
        }
        
        # Save summary
        summary_file = self.reports_dir / "coverage_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(coverage_summary, f, indent=2)
            
        return coverage_summary
    
    def run_tests_with_coverage(self, test_type: str = "all") -> Dict[str, Any]:
        """Run tests with coverage monitoring.
        
        Args:
            test_type: Type of tests to run ("unit", "functional", or "all")
            
        Returns:
            Dict containing test results and coverage data
        """
        print(f"Running {test_type} tests with coverage analysis...")
        print("=" * 60)
        
        # Start coverage
        self.start_coverage()
        
        test_dir = self.project_root / "test"
        
        try:
            if test_type == "unit":
                result = self._run_unit_tests_detailed()
            elif test_type == "functional":
                result = self._run_functional_tests_detailed()
            elif test_type == "all":
                unit_result = self._run_unit_tests_detailed()
                functional_result = self._run_functional_tests_detailed()
                result = self._combine_test_results(unit_result, functional_result)
            else:
                raise ValueError(f"Unknown test type: {test_type}")
                
        finally:
            # Always stop coverage
            self.stop_coverage()
        
        # Generate coverage reports
        coverage_data = self.generate_coverage_reports()
        
        # Generate test report
        test_report = self._generate_test_report(result, coverage_data)
        
        return test_report
    
    def _run_unit_tests_detailed(self) -> Dict[str, Any]:
        """Run unit tests with detailed result collection."""
        print("Running Unit Tests...")
        print("-" * 40)
        
        test_dir = self.project_root / "test" / "unit"
        loader = unittest.TestLoader()
        suite = loader.discover(str(test_dir), pattern='test_*.py')
        
        # Custom test result collector
        result = DetailedTestResult()
        start_time = time.time()
        
        runner = unittest.TextTestRunner(
            stream=sys.stdout,
            verbosity=2
        )
        
        # Run tests with our custom result collector
        runner.run(suite)
        
        end_time = time.time()
        
        return {
            'type': 'unit',
            'tests_run': result.testsRun,
            'failures': [self._format_test_error(test, error) for test, error in result.failures],
            'errors': [self._format_test_error(test, error) for test, error in result.errors],
            'skipped': [self._format_test_skip(test, reason) for test, reason in result.skipped],
            'success_count': result.testsRun - len(result.failures) - len(result.errors),
            'duration': round(end_time - start_time, 3),
            'successful': result.wasSuccessful(),
            'test_details': getattr(result, 'test_details', [])
        }
    
    def _run_functional_tests_detailed(self) -> Dict[str, Any]:
        """Run functional tests with detailed result collection."""
        print("Running Functional Tests...")
        print("-" * 40)
        
        test_dir = self.project_root / "test" / "functional"
        loader = unittest.TestLoader()
        suite = loader.discover(str(test_dir), pattern='test_*.py')
        
        # Custom test result collector
        result = DetailedTestResult()
        start_time = time.time()
        
        runner = unittest.TextTestRunner(
            stream=sys.stdout,
            verbosity=2
        )
        
        # Run tests with our custom result collector
        runner.run(suite)
        
        end_time = time.time()
        
        return {
            'type': 'functional',
            'tests_run': result.testsRun,
            'failures': [self._format_test_error(test, error) for test, error in result.failures],
            'errors': [self._format_test_error(test, error) for test, error in result.errors],
            'skipped': [self._format_test_skip(test, reason) for test, reason in result.skipped],
            'success_count': result.testsRun - len(result.failures) - len(result.errors),
            'duration': round(end_time - start_time, 3),
            'successful': result.wasSuccessful(),
            'test_details': getattr(result, 'test_details', [])
        }
    
    def _combine_test_results(self, unit_result: Dict, functional_result: Dict) -> Dict[str, Any]:
        """Combine unit and functional test results."""
        return {
            'type': 'combined',
            'unit_tests': unit_result,
            'functional_tests': functional_result,
            'tests_run': unit_result['tests_run'] + functional_result['tests_run'],
            'failures': unit_result['failures'] + functional_result['failures'],
            'errors': unit_result['errors'] + functional_result['errors'],
            'skipped': unit_result['skipped'] + functional_result['skipped'],
            'success_count': unit_result['success_count'] + functional_result['success_count'],
            'duration': round(unit_result['duration'] + functional_result['duration'], 3),
            'successful': unit_result['successful'] and functional_result['successful']
        }
    
    def _format_test_error(self, test, error):
        """Format test error for JSON serialization."""
        return {
            'test': str(test),
            'error': str(error),
            'test_method': test._testMethodName if hasattr(test, '_testMethodName') else 'unknown',
            'test_class': test.__class__.__name__ if hasattr(test, '__class__') else 'unknown'
        }
    
    def _format_test_skip(self, test, reason):
        """Format test skip for JSON serialization."""
        return {
            'test': str(test),
            'reason': str(reason),
            'test_method': test._testMethodName if hasattr(test, '_testMethodName') else 'unknown',
            'test_class': test.__class__.__name__ if hasattr(test, '__class__') else 'unknown'
        }
    
    def _generate_test_report(self, test_result: Dict, coverage_data: Dict) -> Dict[str, Any]:
        """Generate comprehensive test report combining test results and coverage."""
        timestamp = datetime.now()
        
        report = {
            'report_metadata': {
                'timestamp': timestamp.isoformat(),
                'project': 'dbchecker',
                'report_version': '1.0.0',
                'python_version': sys.version,
                'total_duration': test_result.get('duration', 0)
            },
            'test_results': test_result,
            'coverage': coverage_data,
            'summary': {
                'total_tests': test_result.get('tests_run', 0),
                'passed_tests': test_result.get('success_count', 0),
                'failed_tests': len(test_result.get('failures', [])),
                'error_tests': len(test_result.get('errors', [])),
                'skipped_tests': len(test_result.get('skipped', [])),
                'success_rate': self._calculate_success_rate(test_result),
                'coverage_percentage': coverage_data.get('total_coverage', 0.0)
            }
        }
        
        # Save comprehensive report
        report_file = self.reports_dir / f"test_report_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Generate HTML report
        html_report = self._generate_html_report(report)
        html_file = self.reports_dir / f"test_report_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"
        with open(html_file, 'w') as f:
            f.write(html_report)
        
        # Save latest report links
        latest_json = self.reports_dir / "latest_report.json"
        latest_html = self.reports_dir / "latest_report.html"
        
        with open(latest_json, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        with open(latest_html, 'w') as f:
            f.write(html_report)
        
        report['report_files'] = {
            'json': str(report_file),
            'html': str(html_file),
            'latest_json': str(latest_json),
            'latest_html': str(latest_html)
        }
        
        return report
    
    def _calculate_success_rate(self, test_result: Dict) -> float:
        """Calculate test success rate."""
        total = test_result.get('tests_run', 0)
        if total == 0:
            return 100.0
        
        failures = len(test_result.get('failures', []))
        errors = len(test_result.get('errors', []))
        passed = total - failures - errors
        
        return round((passed / total) * 100, 2)
    
    def _generate_html_report(self, report_data: Dict) -> str:
        """Generate HTML test report."""
        timestamp = report_data['report_metadata']['timestamp']
        summary = report_data['summary']
        test_results = report_data['test_results']
        coverage = report_data['coverage']
        
        # Determine status color
        success_rate = summary['success_rate']
        coverage_rate = summary['coverage_percentage']
        
        if success_rate >= 95 and coverage_rate >= 80:
            status_color = "#28a745"  # Green
            status_text = "EXCELLENT"
        elif success_rate >= 85 and coverage_rate >= 70:
            status_color = "#ffc107"  # Yellow
            status_text = "GOOD"
        else:
            status_color = "#dc3545"  # Red
            status_text = "NEEDS IMPROVEMENT"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>DBChecker Test Report - {timestamp}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ text-align: center; margin-bottom: 30px; }}
        .header h1 {{ color: #333; margin-bottom: 10px; }}
        .status {{ display: inline-block; padding: 8px 16px; border-radius: 4px; color: white; font-weight: bold; background-color: {status_color}; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px 0; }}
        .metric {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; border-left: 4px solid #007bff; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; text-transform: uppercase; }}
        .metric .value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .section {{ margin: 30px 0; }}
        .section h2 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        .test-results {{ margin: 20px 0; }}
        .test-type {{ background: #e9ecef; padding: 15px; margin: 10px 0; border-radius: 5px; }}
        .failure {{ background: #f8d7da; border: 1px solid #f5c6cb; padding: 10px; margin: 5px 0; border-radius: 4px; }}
        .error {{ background: #fff3cd; border: 1px solid #ffeaa7; padding: 10px; margin: 5px 0; border-radius: 4px; }}
        .coverage-files {{ max-height: 400px; overflow-y: auto; border: 1px solid #ddd; }}
        .file-coverage {{ display: flex; justify-content: space-between; padding: 8px 12px; border-bottom: 1px solid #eee; }}
        .coverage-bar {{ width: 100px; height: 20px; background: #eee; border-radius: 10px; overflow: hidden; }}
        .coverage-fill {{ height: 100%; background: linear-gradient(90deg, #dc3545 0%, #ffc107 50%, #28a745 80%); }}
        .footer {{ margin-top: 40px; text-align: center; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>DBChecker Test Report</h1>
            <div class="status">{status_text}</div>
            <p>Generated on {timestamp}</p>
        </div>
        
        <div class="summary">
            <div class="metric">
                <h3>Total Tests</h3>
                <div class="value">{summary['total_tests']}</div>
            </div>
            <div class="metric">
                <h3>Success Rate</h3>
                <div class="value">{summary['success_rate']}%</div>
            </div>
            <div class="metric">
                <h3>Code Coverage</h3>
                <div class="value">{summary['coverage_percentage']}%</div>
            </div>
            <div class="metric">
                <h3>Duration</h3>
                <div class="value">{report_data['report_metadata']['total_duration']}s</div>
            </div>
        </div>
        
        <div class="section">
            <h2>Test Results Breakdown</h2>
            <div class="summary">
                <div class="metric">
                    <h3>Passed</h3>
                    <div class="value" style="color: #28a745;">{summary['passed_tests']}</div>
                </div>
                <div class="metric">
                    <h3>Failed</h3>
                    <div class="value" style="color: #dc3545;">{summary['failed_tests']}</div>
                </div>
                <div class="metric">
                    <h3>Errors</h3>
                    <div class="value" style="color: #ffc107;">{summary['error_tests']}</div>
                </div>
                <div class="metric">
                    <h3>Skipped</h3>
                    <div class="value" style="color: #6c757d;">{summary['skipped_tests']}</div>
                </div>
            </div>
        </div>"""

        # Add test details if available
        if test_results.get('type') == 'combined':
            html_content += f"""
        <div class="section">
            <h2>Test Type Details</h2>
            <div class="test-type">
                <h3>Unit Tests</h3>
                <p>Tests: {test_results['unit_tests']['tests_run']} | Duration: {test_results['unit_tests']['duration']}s</p>
            </div>
            <div class="test-type">
                <h3>Functional Tests</h3>
                <p>Tests: {test_results['functional_tests']['tests_run']} | Duration: {test_results['functional_tests']['duration']}s</p>
            </div>
        </div>"""

        # Add failures and errors
        if test_results.get('failures') or test_results.get('errors'):
            html_content += f"""
        <div class="section">
            <h2>Test Failures & Errors</h2>"""
            
            for failure in test_results.get('failures', []):
                html_content += f"""
            <div class="failure">
                <strong>FAILED:</strong> {failure['test_class']}.{failure['test_method']}<br>
                <pre>{failure['error']}</pre>
            </div>"""
            
            for error in test_results.get('errors', []):
                html_content += f"""
            <div class="error">
                <strong>ERROR:</strong> {error['test_class']}.{error['test_method']}<br>
                <pre>{error['error']}</pre>
            </div>"""
            
            html_content += "</div>"

        # Add coverage details
        html_content += f"""
        <div class="section">
            <h2>Code Coverage by File</h2>
            <div class="coverage-files">"""

        for file_path, file_data in coverage.get('files', {}).items():
            coverage_percent = file_data['coverage_percent']
            html_content += f"""
                <div class="file-coverage">
                    <span>{file_path}</span>
                    <div style="display: flex; align-items: center; gap: 10px;">
                        <span>{coverage_percent}%</span>
                        <div class="coverage-bar">
                            <div class="coverage-fill" style="width: {coverage_percent}%;"></div>
                        </div>
                    </div>
                </div>"""

        html_content += f"""
            </div>
        </div>
        
        <div class="footer">
            <p>Report generated by DBChecker Test Reporter v1.0.0</p>
            <p>For detailed coverage analysis, see: {coverage.get('reports', {}).get('html', 'N/A')}</p>
        </div>
    </div>
</body>
</html>"""

        return html_content


class DetailedTestResult(TestResult):
    """Enhanced test result class that captures detailed test information."""
    
    def __init__(self):
        super().__init__()
        self.test_details = []
        
    def startTest(self, test):
        super().startTest(test)
        self.test_start_time = time.time()
        
    def stopTest(self, test):
        super().stopTest(test)
        duration = time.time() - getattr(self, 'test_start_time', time.time())
        
        self.test_details.append({
            'test_name': str(test),
            'test_method': test._testMethodName if hasattr(test, '_testMethodName') else 'unknown',
            'test_class': test.__class__.__name__ if hasattr(test, '__class__') else 'unknown',
            'duration': round(duration, 4),
            'status': 'passed'  # Will be updated by addFailure/addError if needed
        })
        
    def addFailure(self, test, err):
        super().addFailure(test, err)
        # Update the last test detail
        if self.test_details:
            self.test_details[-1]['status'] = 'failed'
            
    def addError(self, test, err):
        super().addError(test, err)
        # Update the last test detail
        if self.test_details:
            self.test_details[-1]['status'] = 'error'
            
    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        # Update the last test detail
        if self.test_details:
            self.test_details[-1]['status'] = 'skipped'
