#!/usr/bin/env python3
"""
Debug script to see what columns are being compared in the complex test.
"""

import tempfile
import os
import sqlite3
from dbchecker.database_connector import DatabaseConnector

def create_test_database_with_all_metadata(db_path, add_differences=False):
    """Create a test database with various types of metadata columns"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create comprehensive test table with all types of metadata
    cursor.execute('''
        CREATE TABLE comprehensive_test (
            -- Primary key (sequence/auto-increment)
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Business data (should be compared)
            username TEXT NOT NULL,
            email TEXT,
            status TEXT,
            
            -- Timestamp columns
            created_at DATETIME,
            updated_timestamp TIMESTAMP,
            last_login_time TIME,
            birth_date DATE,
            
            -- Audit metadata
            created_by TEXT,
            modified_by TEXT,
            session_id TEXT,
            transaction_id TEXT,
            
            -- System metadata
            row_version INTEGER,
            record_checksum TEXT,
            source_system TEXT,
            audit_log TEXT,
            
            -- UUID columns
            record_uuid TEXT,
            external_id TEXT
        )
    ''')
    
    # Base data for consistent business logic
    base_data = {
        'username': 'john_doe',
        'email': 'john@example.com', 
        'status': 'active'
    }
    
    # Different metadata based on timing/system state
    if add_differences:
        metadata = {
            'created_at': '2024-01-01 10:05:00',
            'updated_timestamp': '2024-01-01 10:05:00',
            'last_login_time': '10:05:00',
            'created_by': 'system_b',
            'modified_by': 'user_b',
            'session_id': 'session_456',
            'transaction_id': 'txn_789',
            'row_version': 2,
            'record_checksum': 'abc123def456',
            'source_system': 'system_b',
            'audit_log': 'updated by system_b',
            'record_uuid': '550e8400-e29b-41d4-a716-446655440001',
            'external_id': '789def456abc'
        }
    else:
        metadata = {
            'created_at': '2024-01-01 10:00:00',
            'updated_timestamp': '2024-01-01 10:00:00',
            'last_login_time': '10:00:00',
            'created_by': 'system_a',
            'modified_by': 'user_a',
            'session_id': 'session_123',
            'transaction_id': 'txn_456',
            'row_version': 1,
            'record_checksum': '123abc456def',
            'source_system': 'system_a',
            'audit_log': 'created by system_a',
            'record_uuid': '550e8400-e29b-41d4-a716-446655440000',
            'external_id': '456abc123def'
        }
    
    # Insert data into comprehensive_test table
    cursor.execute('''
        INSERT INTO comprehensive_test (
            username, email, status, created_at, updated_timestamp, last_login_time, birth_date,
            created_by, modified_by, session_id, transaction_id, row_version, record_checksum,
            source_system, audit_log, record_uuid, external_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        base_data['username'], base_data['email'], base_data['status'],
        metadata['created_at'], metadata['updated_timestamp'], metadata['last_login_time'], '1990-01-01',
        metadata['created_by'], metadata['modified_by'], metadata['session_id'], metadata['transaction_id'],
        metadata['row_version'], metadata['record_checksum'], metadata['source_system'], metadata['audit_log'],
        metadata['record_uuid'], metadata['external_id']
    ))
    
    conn.commit()
    conn.close()

def main():
    print("=== Debug Column Exclusions ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "db1.db")
        
        print("Creating test database...")
        create_test_database_with_all_metadata(db1_path)
        
        # Check table structure
        conn = DatabaseConnector(db1_path)
        table_structure = conn.get_table_structure('comprehensive_test')
        
        print("\\nAll columns in table:")
        for col in table_structure.columns:
            print(f"  - {col.name} ({col.type})")
        
        # Check what gets excluded
        from dbchecker.models import ComparisonOptions
        from dbchecker.metadata_detector import MetadataDetector
        from dbchecker.uuid_handler import UUIDHandler
        
        options = ComparisonOptions(
            auto_detect_uuids=True,
            auto_detect_timestamps=True,
            auto_detect_metadata=True,
            auto_detect_sequences=True
        )
        
        uuid_handler = UUIDHandler()
        metadata_detector = MetadataDetector(options)
        
        sample_data = conn.get_table_data('comprehensive_test', limit=100)
        uuid_columns = uuid_handler.detect_uuid_columns(table_structure, sample_data)
        exclusions = metadata_detector.get_all_excluded_columns(table_structure, uuid_columns, sample_data)
        
        print("\\nExclusion results:")
        for category, columns in exclusions.items():
            if columns:
                print(f"  {category}: {columns}")
        
        # What's left for comparison?
        all_columns = [col.name for col in table_structure.columns]
        remaining_columns = [col for col in all_columns if col not in exclusions['all_excluded']]
        print(f"\\nColumns that will be compared: {remaining_columns}")
        
        # Check actual data
        print("\\nActual data in row:")
        row = sample_data[0]
        for col in remaining_columns:
            print(f"  {col}: {row.get(col)}")

if __name__ == "__main__":
    main()
