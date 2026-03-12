"""
Schema Adapter Module.

Translates the SocialMediaDB schema definition into SQLite CREATE TABLE statements
and TestSuiteEval-compatible data structures.
"""

import sqlite3
import os
from typing import Dict, Tuple, List, Any, Optional


# Type mapping from our schema types to SQLite types
TYPE_MAPPING = {
    "int": "INTEGER",
    "varchar": "TEXT",
    "text": "TEXT", 
    "datetime": "TEXT",  # SQLite stores datetime as TEXT
    "boolean": "INTEGER",  # SQLite uses 0/1 for boolean
}


def schema_to_sqlite_ddl(
    schema: Dict[str, Dict[str, str]], 
    foreign_keys: Dict[Tuple[str, str], Tuple[str, str]],
    add_not_null: bool = True
) -> str:
    """
    Convert schema definition to SQLite CREATE TABLE statements.
    
    Args:
        schema: Dictionary mapping table names to column definitions
        foreign_keys: Dictionary mapping (from_table, to_table) to (from_col, to_col)
        add_not_null: Whether to add NOT NULL constraints where applicable
        
    Returns:
        Complete DDL string for creating all tables
    """
    ddl_statements = []
    
    # Build foreign key relationships per table
    table_fks: Dict[str, List[Tuple[str, str, str]]] = {}  # table -> [(col, ref_table, ref_col)]
    for (from_table, to_table), (from_col, to_col) in foreign_keys.items():
        if from_table not in table_fks:
            table_fks[from_table] = []
        # Only add if this column references another table's primary key
        if to_col == "id":  # Convention: id is always the primary key
            table_fks[from_table].append((from_col, to_table, to_col))
    
    # Determine primary keys (columns named 'id' or composite keys for junction tables)
    def get_primary_keys(table_name: str, columns: Dict[str, str]) -> List[str]:
        if "id" in columns:
            return ["id"]
        # For junction tables like 'likes' and 'follows', use composite keys
        if table_name == "likes":
            return ["user_id", "post_id"]
        if table_name == "follows":
            return ["follower_id", "followee_id"]
        return []
    
    # Columns that should allow NULL (optional fields)
    nullable_columns = {
        "email",  # Email might be optional
    }
    
    for table_name, columns in schema.items():
        primary_keys = get_primary_keys(table_name, columns)
        
        column_defs = []
        for col_name, col_type in columns.items():
            sqlite_type = TYPE_MAPPING.get(col_type.lower(), "TEXT")
            
            constraints = []
            
            # Primary key constraint
            if col_name in primary_keys and len(primary_keys) == 1:
                constraints.append("PRIMARY KEY")
            
            # NOT NULL constraint (skip for nullable columns and primary keys)
            if add_not_null and col_name not in nullable_columns:
                if col_name not in primary_keys:  # PRIMARY KEY implies NOT NULL
                    constraints.append("NOT NULL")
            
            constraint_str = " " + " ".join(constraints) if constraints else ""
            column_defs.append(f'    "{col_name}" {sqlite_type}{constraint_str}')
        
        # Add composite primary key if needed
        if len(primary_keys) > 1:
            pk_cols = ", ".join(f'"{k}"' for k in primary_keys)
            column_defs.append(f"    PRIMARY KEY ({pk_cols})")
        
        # Add foreign key constraints
        fks = table_fks.get(table_name, [])
        for from_col, ref_table, ref_col in fks:
            column_defs.append(
                f'    FOREIGN KEY ("{from_col}") REFERENCES "{ref_table}"("{ref_col}")'
            )
        
        columns_sql = ",\n".join(column_defs)
        ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n{columns_sql}\n);'
        ddl_statements.append(ddl)
    
    return "\n\n".join(ddl_statements)


def create_database_from_schema(
    db_path: str,
    schema: Dict[str, Dict[str, str]],
    foreign_keys: Dict[Tuple[str, str], Tuple[str, str]],
    overwrite: bool = False
) -> None:
    """
    Create an SQLite database with the given schema.
    
    Args:
        db_path: Path to create the database
        schema: Schema definition
        foreign_keys: Foreign key relationships
        overwrite: Whether to overwrite existing database
    """
    if os.path.exists(db_path):
        if overwrite:
            os.unlink(db_path)
        else:
            raise FileExistsError(f"Database already exists: {db_path}")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    
    ddl = schema_to_sqlite_ddl(schema, foreign_keys, add_not_null=True)
    
    conn = sqlite3.connect(db_path)
    conn.executescript("PRAGMA foreign_keys = ON;\n" + ddl)
    conn.commit()
    conn.close()


def get_table_names(db_path: str) -> List[str]:
    """Get all table names from a database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def get_table_schema(db_path: str, table_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Get schema information for a table.
    
    Returns dict mapping column_name to properties dict with:
    - type: SQLite type
    - notnull: bool
    - pk: bool (is primary key)
    - unique: bool
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    
    columns = {}
    for row in cursor.fetchall():
        cid, name, dtype, notnull, default_val, pk = row
        columns[name] = {
            "type": dtype,
            "notnull": bool(notnull),
            "pk": bool(pk),
            "unique": bool(pk),  # Primary keys are unique
            "checked": False,
        }
    
    conn.close()
    return columns


def get_foreign_keys(db_path: str, table_name: str) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    Get foreign key relationships for a table.
    
    Returns dict mapping (child_table, child_col) to (parent_table, parent_col)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA foreign_key_list('{table_name}');")
    
    fks = {}
    for row in cursor.fetchall():
        # id, seq, table, from, to, on_update, on_delete, match
        _, _, ref_table, from_col, to_col, *_ = row
        fks[(table_name, from_col)] = (ref_table, to_col)
    
    conn.close()
    return fks


def get_column_elements(db_path: str, table_name: str, column_name: str) -> List[Any]:
    """Get all values in a column."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'SELECT DISTINCT "{column_name}" FROM "{table_name}";')
    elements = [row[0] for row in cursor.fetchall()]
    conn.close()
    return elements


def get_testsuite_compatible_info(db_path: str) -> Tuple[
    Dict[Tuple[str, str], Dict[str, Any]],  # table_column_properties
    Dict[Tuple[str, str], Tuple[str, str]],  # child2parent (foreign keys)
    Dict[Tuple[str, str], List[Any]]  # table_column2elements
]:
    """
    Extract database info in TestSuiteEval-compatible format.
    
    This mimics the output of sql_util.dbinfo.get_all_db_info_path()
    
    Returns:
        - table_column_properties: (table, column) -> {type, notnull, pk, unique, checked}
        - child2parent: (child_table, child_col) -> (parent_table, parent_col) 
        - table_column2elements: (table, column) -> [values]
    """
    table_column_properties = {}
    child2parent = {}
    table_column2elements = {}
    
    tables = get_table_names(db_path)
    
    for table_name in tables:
        # Get column properties
        columns = get_table_schema(db_path, table_name)
        for col_name, props in columns.items():
            table_column_properties[(table_name, col_name)] = props
            
            # Get column elements
            elements = get_column_elements(db_path, table_name, col_name)
            table_column2elements[(table_name, col_name)] = elements
        
        # Get foreign keys
        fks = get_foreign_keys(db_path, table_name)
        child2parent.update(fks)
    
    return table_column_properties, child2parent, table_column2elements


def get_process_order_for_schema(
    schema: Dict[str, Dict[str, str]],
    foreign_keys: Dict[Tuple[str, str], Tuple[str, str]]
) -> List[str]:
    """
    Determine table processing order based on foreign key dependencies.
    Tables with no dependencies come first.
    
    Returns list of table names in dependency order.
    """
    # Build dependency graph
    dependencies: Dict[str, set] = {table: set() for table in schema}
    
    for (from_table, to_table), (from_col, to_col) in foreign_keys.items():
        if from_table in schema and to_table in schema:
            if to_col == "id":  # This table depends on the other
                dependencies[from_table].add(to_table)
    
    # Topological sort
    result = []
    visited = set()
    
    def visit(table: str):
        if table in visited:
            return
        visited.add(table)
        for dep in dependencies[table]:
            visit(dep)
        result.append(table)
    
    for table in schema:
        visit(table)
    
    return result
