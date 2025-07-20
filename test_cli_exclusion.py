#!/usr/bin/env python3
"""
Quick CLI test for the new column exclusion functionality.
"""

import tempfile
import os
import sqlite3
import subprocess
import json

def create_test_db(db_path, variant=1):
    """Create a simple test database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price DECIMAL(10,2),
            internal_notes TEXT,
            debug_info TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    if variant == 1:
        cursor.execute('''
            INSERT INTO products (name, price, internal_notes, debug_info)
            VALUES 
                ('Widget A', 19.99, 'Internal note 1', 'debug_data_1'),
                ('Widget B', 29.99, 'Internal note 2', 'debug_data_2')
        ''')
    else:
        cursor.execute('''
            INSERT INTO products (name, price, internal_notes, debug_info)
            VALUES 
                ('Widget A', 19.99, 'Different internal note', 'different_debug'),
                ('Widget B', 29.99, 'Another different note', 'other_debug')
        ''')
    
    conn.commit()
    conn.close()

def main():
    """Test CLI functionality"""
    print("=== Testing CLI Column Exclusion ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "db1.sqlite")
        db2_path = os.path.join(temp_dir, "db2.sqlite")
        output_dir = os.path.join(temp_dir, "reports")
        
        # Create test databases
        print("Creating test databases...")
        create_test_db(db1_path, variant=1)
        create_test_db(db2_path, variant=2)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Test CLI without exclusions
        print("Testing CLI without column exclusions...")
        result1 = subprocess.run([
            "python", "-m", "dbchecker.cli",
            db1_path, db2_path,
            "--output-dir", output_dir,
            "--output-format", "json",
            "--filename-prefix", "test_no_exclusions"
        ], capture_output=True, text=True, cwd="/Users/tylerstandish/Projects/dbchecker/dbchecker")
        
        print(f"Exit code without exclusions: {result1.returncode}")
        
        # Test CLI with exclusions
        print("Testing CLI with column exclusions...")
        result2 = subprocess.run([
            "python", "-m", "dbchecker.cli",
            db1_path, db2_path,
            "--exclude-columns", "internal_notes", "debug_info",
            "--output-dir", output_dir,
            "--output-format", "json",
            "--filename-prefix", "test_with_exclusions"
        ], capture_output=True, text=True, cwd="/Users/tylerstandish/Projects/dbchecker/dbchecker")
        
        print(f"Exit code with exclusions: {result2.returncode}")
        
        # Test CLI with pattern exclusions
        print("Testing CLI with pattern exclusions...")
        result3 = subprocess.run([
            "python", "-m", "dbchecker.cli",
            db1_path, db2_path,
            "--exclude-column-patterns", ".*internal.*", ".*debug.*",
            "--output-dir", output_dir,
            "--output-format", "json",
            "--filename-prefix", "test_pattern_exclusions"
        ], capture_output=True, text=True, cwd="/Users/tylerstandish/Projects/dbchecker/dbchecker")
        
        print(f"Exit code with patterns: {result3.returncode}")
        
        # Check results
        if result1.returncode == 1 and result2.returncode == 0 and result3.returncode == 0:
            print("✅ CLI tests PASSED!")
            print("- Without exclusions: Found differences (expected)")
            print("- With explicit exclusions: No differences (expected)")
            print("- With pattern exclusions: No differences (expected)")
        else:
            print("❌ CLI tests FAILED")
            print(f"Result 1 stderr: {result1.stderr}")
            print(f"Result 2 stderr: {result2.stderr}")
            print(f"Result 3 stderr: {result3.stderr}")

if __name__ == "__main__":
    main()
