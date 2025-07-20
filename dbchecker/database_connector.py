"""
Database connector module for SQLite database operations.
"""

import sqlite3
from typing import Dict, List, Any, Optional
from .models import DatabaseSchema, TableStructure, Column, Index, Trigger, View
from .models import PrimaryKey, ForeignKey, UniqueConstraint, CheckConstraint
from .exceptions import DatabaseConnectionError, SchemaExtractionError


class DatabaseConnector:
    """Abstracts database operations for SQLite"""
    
    def __init__(self, db_path: str):
        """Initialize database connection"""
        self.db_path = db_path
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to the database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            raise DatabaseConnectionError(f"Failed to connect to database {self.db_path}: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        if not self.connection:
            raise DatabaseConnectionError("No database connection")
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            raise SchemaExtractionError(f"Query execution failed: {e}")
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names in the database"""
        query = """
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
        results = self.execute_query(query)
        return [row['name'] for row in results]
    
    def get_table_structure(self, table_name: str) -> TableStructure:
        """Get complete structure of a table"""
        columns = self._get_columns(table_name)
        primary_key = self._get_primary_key(table_name)
        foreign_keys = self._get_foreign_keys(table_name)
        unique_constraints = self._get_unique_constraints(table_name)
        check_constraints = self._get_check_constraints(table_name)
        
        return TableStructure(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            unique_constraints=unique_constraints,
            check_constraints=check_constraints
        )
    
    def _get_columns(self, table_name: str) -> List[Column]:
        """Get column information for a table"""
        query = f"PRAGMA table_info({table_name})"
        results = self.execute_query(query)
        
        columns = []
        for row in results:
            column = Column(
                name=row['name'],
                type=row['type'],
                nullable=not bool(row['notnull']),
                default=row['dflt_value'],
                is_primary_key=bool(row['pk'])
            )
            columns.append(column)
        
        return columns
    
    def _get_primary_key(self, table_name: str) -> Optional[PrimaryKey]:
        """Get primary key information for a table"""
        query = f"PRAGMA table_info({table_name})"
        results = self.execute_query(query)
        
        pk_columns = [row['name'] for row in results if row['pk']]
        return PrimaryKey(columns=pk_columns) if pk_columns else None
    
    def _get_foreign_keys(self, table_name: str) -> List[ForeignKey]:
        """Get foreign key information for a table"""
        query = f"PRAGMA foreign_key_list({table_name})"
        results = self.execute_query(query)
        
        # Group foreign keys by id
        fk_groups = {}
        for row in results:
            fk_id = row['id']
            if fk_id not in fk_groups:
                fk_groups[fk_id] = {
                    'table': row['table'],
                    'from_columns': [],
                    'to_columns': []
                }
            fk_groups[fk_id]['from_columns'].append(row['from'])
            fk_groups[fk_id]['to_columns'].append(row['to'])
        
        foreign_keys = []
        for fk_data in fk_groups.values():
            foreign_key = ForeignKey(
                columns=fk_data['from_columns'],
                referenced_table=fk_data['table'],
                referenced_columns=fk_data['to_columns']
            )
            foreign_keys.append(foreign_key)
        
        return foreign_keys
    
    def _get_unique_constraints(self, table_name: str) -> List[UniqueConstraint]:
        """Get unique constraint information for a table"""
        # Get index list for the table
        query = f"PRAGMA index_list({table_name})"
        results = self.execute_query(query)
        
        unique_constraints = []
        for row in results:
            if row['unique']:
                # Get index info to get columns
                index_query = f"PRAGMA index_info({row['name']})"
                index_results = self.execute_query(index_query)
                columns = [idx_row['name'] for idx_row in index_results]
                
                constraint = UniqueConstraint(
                    name=row['name'],
                    columns=columns
                )
                unique_constraints.append(constraint)
        
        return unique_constraints
    
    def _get_check_constraints(self, table_name: str) -> List[CheckConstraint]:
        """Get check constraint information for a table"""
        # SQLite doesn't provide direct access to check constraints via PRAGMA
        # We need to parse the CREATE TABLE statement
        query = """
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name=?
        """
        results = self.execute_query(query, (table_name,))
        
        check_constraints = []
        if results and results[0]['sql']:
            sql = results[0]['sql']
            # Basic parsing for CHECK constraints
            # This is a simplified implementation
            import re
            check_pattern = r'CHECK\s*\(([^)]+)\)'
            matches = re.finditer(check_pattern, sql, re.IGNORECASE)
            
            for i, match in enumerate(matches):
                constraint = CheckConstraint(
                    name=f"check_{table_name}_{i}",
                    expression=match.group(1)
                )
                check_constraints.append(constraint)
        
        return check_constraints
    
    def get_indexes(self, table_name: Optional[str] = None) -> List[Index]:
        """Get index information for a table or all tables"""
        if table_name:
            query = f"PRAGMA index_list({table_name})"
            index_list = self.execute_query(query)
            
            indexes = []
            for row in index_list:
                # Get index info to get columns
                index_query = f"PRAGMA index_info({row['name']})"
                index_results = self.execute_query(index_query)
                columns = [idx_row['name'] for idx_row in index_results]
                
                index = Index(
                    name=row['name'],
                    table_name=table_name,
                    columns=columns,
                    unique=bool(row['unique'])
                )
                indexes.append(index)
            
            return indexes
        else:
            # Get all indexes
            all_indexes = []
            for table in self.get_table_names():
                all_indexes.extend(self.get_indexes(table))
            return all_indexes
    
    def get_triggers(self) -> List[Trigger]:
        """Get all triggers in the database"""
        query = """
        SELECT name, tbl_name, sql FROM sqlite_master 
        WHERE type='trigger'
        ORDER BY name
        """
        results = self.execute_query(query)
        
        triggers = []
        for row in results:
            # Parse trigger definition for event and timing
            sql = row['sql'] or ''
            event = 'UNKNOWN'
            timing = 'UNKNOWN'
            
            # Basic parsing
            if 'INSERT' in sql.upper():
                event = 'INSERT'
            elif 'UPDATE' in sql.upper():
                event = 'UPDATE'
            elif 'DELETE' in sql.upper():
                event = 'DELETE'
            
            if 'BEFORE' in sql.upper():
                timing = 'BEFORE'
            elif 'AFTER' in sql.upper():
                timing = 'AFTER'
            elif 'INSTEAD OF' in sql.upper():
                timing = 'INSTEAD OF'
            
            trigger = Trigger(
                name=row['name'],
                table_name=row['tbl_name'],
                event=event,
                timing=timing,
                definition=sql
            )
            triggers.append(trigger)
        
        return triggers
    
    def get_views(self) -> List[View]:
        """Get all views in the database"""
        query = """
        SELECT name, sql FROM sqlite_master 
        WHERE type='view'
        ORDER BY name
        """
        results = self.execute_query(query)
        
        views = []
        for row in results:
            view = View(
                name=row['name'],
                definition=row['sql'] or ''
            )
            views.append(view)
        
        return views
    
    def get_schema(self) -> DatabaseSchema:
        """Get complete database schema"""
        tables = {}
        for table_name in self.get_table_names():
            tables[table_name] = self.get_table_structure(table_name)
        
        return DatabaseSchema(
            tables=tables,
            views=self.get_views(),
            triggers=self.get_triggers(),
            indexes=self.get_indexes()
        )
    
    def get_table_data(self, table_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all data from a table"""
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
    
    def get_row_count(self, table_name: str) -> int:
        """Get total number of rows in a table"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
