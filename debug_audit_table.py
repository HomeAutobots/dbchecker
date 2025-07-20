#!/usr/bin/env python3
"""
Debug the audit table specifically.
"""

import tempfile
import os
import sqlite3
from dbchecker.database_connector import DatabaseConnector

def create_audit_table(db_path, add_differences=False):
    """Create audit table for debugging"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE audit_table (
            post_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            
            -- Timestamp patterns
            created TEXT,
            modified TEXT,
            deleted_at TEXT,
            published_date TEXT,
            
            -- User tracking
            author_user TEXT,
            editor_user TEXT,
            reviewer_by TEXT,
            
            -- System tracking
            version_number INTEGER,
            trace_id TEXT,
            system_hash TEXT,
            
            -- Reference data
            author_id INTEGER,
            category_id INTEGER
        )
    ''')
    
    base_data = {
        'title': 'Test Post',
        'content': 'This is test content',
        'author_id': 1,
        'category_id': 2
    }
    
    if add_differences:
        metadata = {
            'created': '2024-01-01 10:05:00',
            'modified': '2024-01-01 10:05:00',
            'author_user': 'john_b',
            'editor_user': 'editor_b',
            'reviewer_by': 'reviewer_b',
            'version_number': 2,
            'trace_id': 'trace_789',
            'system_hash': 'hash789'
        }
    else:
        metadata = {
            'created': '2024-01-01 10:00:00',
            'modified': '2024-01-01 10:00:00',
            'author_user': 'john_a',
            'editor_user': 'editor_a',
            'reviewer_by': 'reviewer_a',
            'version_number': 1,
            'trace_id': 'trace_456',
            'system_hash': 'hash456'
        }
    
    cursor.execute('''
        INSERT INTO audit_table (
            title, content, created, modified, deleted_at, published_date,
            author_user, editor_user, reviewer_by, version_number, trace_id, system_hash,
            author_id, category_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        base_data['title'], base_data['content'], metadata['created'], metadata['modified'], None, '2024-01-01',
        metadata['author_user'], metadata['editor_user'], metadata['reviewer_by'], metadata['version_number'],
        metadata['trace_id'], metadata['system_hash'], base_data['author_id'], base_data['category_id']
    ))
    
    conn.commit()
    conn.close()

def main():
    print("=== Debug Audit Table ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db1_path = os.path.join(temp_dir, "db1.db")
        
        print("Creating audit table...")
        create_audit_table(db1_path)
        
        # Check table structure
        conn = DatabaseConnector(db1_path)
        table_structure = conn.get_table_structure('audit_table')
        
        print("\\nAll columns in audit_table:")
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
        
        sample_data = conn.get_table_data('audit_table', limit=100)
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
