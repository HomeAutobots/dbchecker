"""
Schema comparator module for comparing database structures.
"""

from typing import Dict, List
from .models import (
    DatabaseSchema, TableStructure, Column, SchemaComparisonResult,
    TableComparisonResult, FieldDifference
)


class SchemaComparator:
    """Compares database schemas and structures"""
    
    def __init__(self):
        """Initialize schema comparator"""
        pass
    
    def compare_schemas(self, schema1: DatabaseSchema, schema2: DatabaseSchema) -> SchemaComparisonResult:
        """Compare two complete database schemas"""
        table_names1 = set(schema1.tables.keys())
        table_names2 = set(schema2.tables.keys())
        
        # Find missing tables
        missing_in_db1 = list(table_names2 - table_names1)
        missing_in_db2 = list(table_names1 - table_names2)
        
        # Compare common tables
        common_tables = table_names1 & table_names2
        table_differences = {}
        
        for table_name in common_tables:
            table1 = schema1.tables[table_name]
            table2 = schema2.tables[table_name]
            comparison = self.compare_tables(table1, table2)
            if not comparison.identical:
                table_differences[table_name] = comparison
        
        # Schema is identical if no missing tables and no table differences
        identical = (not missing_in_db1 and not missing_in_db2 and not table_differences)
        
        return SchemaComparisonResult(
            identical=identical,
            missing_in_db1=missing_in_db1,
            missing_in_db2=missing_in_db2,
            table_differences=table_differences
        )
    
    def compare_tables(self, table1: TableStructure, table2: TableStructure) -> TableComparisonResult:
        """Compare two table structures"""
        column_names1 = {col.name for col in table1.columns}
        column_names2 = {col.name for col in table2.columns}
        
        # Find missing columns
        missing_columns_db1 = list(column_names2 - column_names1)
        missing_columns_db2 = list(column_names1 - column_names2)
        
        # Compare common columns
        common_columns = column_names1 & column_names2
        column_differences = []
        
        # Create lookup dictionaries for columns
        columns1_dict = {col.name: col for col in table1.columns}
        columns2_dict = {col.name: col for col in table2.columns}
        
        for col_name in common_columns:
            col1 = columns1_dict[col_name]
            col2 = columns2_dict[col_name]
            differences = self.compare_columns(col1, col2)
            column_differences.extend(differences)
        
        # Check primary key differences
        pk_differences = self._compare_primary_keys(table1, table2)
        column_differences.extend(pk_differences)
        
        # Check foreign key differences
        fk_differences = self._compare_foreign_keys(table1, table2)
        column_differences.extend(fk_differences)
        
        # Check unique constraint differences
        uc_differences = self._compare_unique_constraints(table1, table2)
        column_differences.extend(uc_differences)
        
        # Check check constraint differences
        cc_differences = self._compare_check_constraints(table1, table2)
        column_differences.extend(cc_differences)
        
        # Table is identical if no missing columns and no differences
        identical = (not missing_columns_db1 and not missing_columns_db2 and not column_differences)
        
        return TableComparisonResult(
            table_name=table1.name,
            identical=identical,
            missing_columns_db1=missing_columns_db1,
            missing_columns_db2=missing_columns_db2,
            column_differences=column_differences
        )
    
    def compare_columns(self, col1: Column, col2: Column) -> List[FieldDifference]:
        """Compare two column definitions"""
        differences = []
        
        if col1.type != col2.type:
            differences.append(FieldDifference(
                field_name=f"{col1.name}.type",
                value_db1=col1.type,
                value_db2=col2.type
            ))
        
        if col1.nullable != col2.nullable:
            differences.append(FieldDifference(
                field_name=f"{col1.name}.nullable",
                value_db1=col1.nullable,
                value_db2=col2.nullable
            ))
        
        if col1.default != col2.default:
            differences.append(FieldDifference(
                field_name=f"{col1.name}.default",
                value_db1=col1.default,
                value_db2=col2.default
            ))
        
        if col1.is_primary_key != col2.is_primary_key:
            differences.append(FieldDifference(
                field_name=f"{col1.name}.is_primary_key",
                value_db1=col1.is_primary_key,
                value_db2=col2.is_primary_key
            ))
        
        return differences
    
    def _compare_primary_keys(self, table1: TableStructure, table2: TableStructure) -> List[FieldDifference]:
        """Compare primary key constraints"""
        differences = []
        
        pk1_cols = table1.primary_key.columns if table1.primary_key else []
        pk2_cols = table2.primary_key.columns if table2.primary_key else []
        
        if set(pk1_cols) != set(pk2_cols):
            differences.append(FieldDifference(
                field_name=f"{table1.name}.primary_key",
                value_db1=pk1_cols,
                value_db2=pk2_cols
            ))
        
        return differences
    
    def _compare_foreign_keys(self, table1: TableStructure, table2: TableStructure) -> List[FieldDifference]:
        """Compare foreign key constraints"""
        differences = []
        
        # Convert foreign keys to comparable format
        fk1_set = set()
        for fk in table1.foreign_keys:
            fk_tuple = (tuple(fk.columns), fk.referenced_table, tuple(fk.referenced_columns))
            fk1_set.add(fk_tuple)
        
        fk2_set = set()
        for fk in table2.foreign_keys:
            fk_tuple = (tuple(fk.columns), fk.referenced_table, tuple(fk.referenced_columns))
            fk2_set.add(fk_tuple)
        
        if fk1_set != fk2_set:
            differences.append(FieldDifference(
                field_name=f"{table1.name}.foreign_keys",
                value_db1=list(fk1_set),
                value_db2=list(fk2_set)
            ))
        
        return differences
    
    def _compare_unique_constraints(self, table1: TableStructure, table2: TableStructure) -> List[FieldDifference]:
        """Compare unique constraints"""
        differences = []
        
        # Convert unique constraints to comparable format
        uc1_set = set()
        for uc in table1.unique_constraints:
            uc_tuple = (uc.name, tuple(sorted(uc.columns)))
            uc1_set.add(uc_tuple)
        
        uc2_set = set()
        for uc in table2.unique_constraints:
            uc_tuple = (uc.name, tuple(sorted(uc.columns)))
            uc2_set.add(uc_tuple)
        
        if uc1_set != uc2_set:
            differences.append(FieldDifference(
                field_name=f"{table1.name}.unique_constraints",
                value_db1=list(uc1_set),
                value_db2=list(uc2_set)
            ))
        
        return differences
    
    def _compare_check_constraints(self, table1: TableStructure, table2: TableStructure) -> List[FieldDifference]:
        """Compare check constraints"""
        differences = []
        
        # Convert check constraints to comparable format
        cc1_set = set()
        for cc in table1.check_constraints:
            cc_tuple = (cc.name, cc.expression)
            cc1_set.add(cc_tuple)
        
        cc2_set = set()
        for cc in table2.check_constraints:
            cc_tuple = (cc.name, cc.expression)
            cc2_set.add(cc_tuple)
        
        if cc1_set != cc2_set:
            differences.append(FieldDifference(
                field_name=f"{table1.name}.check_constraints",
                value_db1=list(cc1_set),
                value_db2=list(cc2_set)
            ))
        
        return differences
