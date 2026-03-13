"""
Database Seeding Module.

Generates realistic sample data for the SocialMediaDB schema,
respecting foreign key constraints and data type requirements.
"""

import sqlite3
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import os

from src.core.schema_config import SchemaConfig


# Random data generators
def random_username() -> str:
    """Generate a random username."""
    prefixes = ["user", "cool", "super", "mega", "pro", "the", "big", "lil", "mr", "ms"]
    suffixes = ["123", "2024", "x", "99", "jr", "sr", "1st", ""]
    name_parts = ["john", "jane", "alex", "sam", "chris", "pat", "kim", "lee", "max", "sky"]
    return f"{random.choice(prefixes)}_{random.choice(name_parts)}{random.choice(suffixes)}"


def random_email(username: str) -> str:
    """Generate a random email based on username."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com", "test.com"]
    return f"{username}@{random.choice(domains)}"


def random_datetime(start_year: int = 2020, end_year: int = 2025) -> str:
    """Generate a random datetime string."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86399)
    dt = start + timedelta(days=random_days, seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def random_content() -> str:
    """Generate random post/comment content."""
    templates = [
        "Just had an amazing day! #blessed",
        "Check out this cool thing I found",
        "Anyone else experiencing this?",
        "Great weather today!",
        "Can't believe what just happened",
        "This is my {} post of the day",
        "Feeling {} today",
        "Just discovered {}",
        "Who else loves {}?",
        "Throwback to that time when...",
        "Big news coming soon!",
        "Thanks everyone for the support",
        "New update: everything is going great",
        "Question: what do you think about {}?",
        "Just finished reading about {}",
    ]
    fillers = ["amazing", "great", "weird", "interesting", "exciting", "first", "last", "best"]
    template = random.choice(templates)
    if "{}" in template:
        template = template.format(random.choice(fillers))
    return template


def random_country_code() -> str:
    """Generate a random country code."""
    codes = ["US", "UK", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "MX", "ES", "IT"]
    return random.choice(codes)


def seed_database(
    db_path: str,
    schema: Optional[Dict[str, Dict[str, str]]] = None,
    foreign_keys: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None,
    schema_config: Optional[SchemaConfig] = None,
    min_rows: int = 30,
    max_rows: int = 100,
    seed: int = 42
) -> Dict[str, int]:
    """
    Seed the database with realistic sample data.
    
    Args:
        db_path: Path to the SQLite database
        schema: Schema definition
        foreign_keys: Foreign key relationships
        min_rows: Minimum rows per table
        max_rows: Maximum rows per table
        seed: Random seed for reproducibility
        
    Returns:
        Dictionary mapping table names to row counts
    """
    if schema_config is not None:
        schema = schema_config.get_legacy_schema()
        foreign_keys = schema_config.get_fk_pairs()
    elif schema is None or foreign_keys is None:
        raise ValueError(
            "Schema inputs missing: provide either schema_config or both schema and foreign_keys"
        )

    random.seed(seed)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    cursor = conn.cursor()
    
    row_counts = {}
    
    # Determine insertion order based on foreign key dependencies
    table_order = _get_insertion_order(schema, foreign_keys)
    
    # Track generated IDs for foreign key references
    generated_ids: Dict[str, List[int]] = {}
    
    for table_name in table_order:
        columns = schema[table_name]
        num_rows = random.randint(min_rows, max_rows)
        
        rows = []
        for i in range(num_rows):
            row = _generate_row(
                table_name, 
                columns, 
                i + 1,  # ID starts at 1
                foreign_keys,
                generated_ids
            )
            if row:
                rows.append(row)
        
        if rows:
            # Insert rows
            col_names = list(columns.keys())
            placeholders = ", ".join(["?" for _ in col_names])
            col_str = ", ".join(f'"{ c}"' for c in col_names)
            q_table = f'"{table_name}"'
            
            try:
                cursor.executemany(
                    f"INSERT INTO {q_table} ({col_str}) VALUES ({placeholders})",
                    rows
                )
                row_counts[table_name] = len(rows)
                
                # Track generated IDs for this table
                if "id" in columns:
                    generated_ids[table_name] = [row[col_names.index("id")] for row in rows]
                    
            except sqlite3.IntegrityError as e:
                # Handle constraint violations by inserting one at a time
                successful = 0
                for row in rows:
                    try:
                        cursor.execute(
                            f"INSERT INTO {q_table} ({col_str}) VALUES ({placeholders})",
                            row
                        )
                        successful += 1
                    except sqlite3.IntegrityError:
                        pass
                row_counts[table_name] = successful
                
                if "id" in columns:
                    cursor.execute(f'SELECT "id" FROM {q_table}')
                    generated_ids[table_name] = [r[0] for r in cursor.fetchall()]
    
    conn.commit()
    conn.close()
    
    return row_counts


def _get_insertion_order(
    schema: Dict[str, Dict[str, str]],
    foreign_keys: Dict[Tuple[str, str], Tuple[str, str]]
) -> List[str]:
    """
    Determine table insertion order based on foreign key dependencies.
    """
    # Build dependency graph: table -> tables it depends on
    dependencies: Dict[str, set] = {table: set() for table in schema}
    
    for (from_table, to_table), (from_col, to_col) in foreign_keys.items():
        if from_table in schema and to_table in schema:
            # from_table has a column that references to_table
            # So we need to insert into to_table first
            if to_col == "id":
                dependencies[from_table].add(to_table)
    
    # Topological sort
    result = []
    visited = set()
    temp_marked = set()
    
    def visit(table: str):
        if table in temp_marked:
            return  # Cycle detected, skip
        if table in visited:
            return
        temp_marked.add(table)
        for dep in dependencies[table]:
            visit(dep)
        temp_marked.remove(table)
        visited.add(table)
        result.append(table)
    
    for table in schema:
        visit(table)
    
    return result


def _generate_row(
    table_name: str,
    columns: Dict[str, str],
    row_id: int,
    foreign_keys: Dict[Tuple[str, str], Tuple[str, str]],
    generated_ids: Dict[str, List[int]]
) -> Tuple:
    """
    Generate a single row of data for a table.
    """
    # Build reverse FK map: (table, column) -> (ref_table, ref_col)
    fk_map = {}
    for (from_table, to_table), (from_col, to_col) in foreign_keys.items():
        if from_table == table_name and to_col == "id":
            fk_map[from_col] = to_table
    
    values = []
    username = None  # Track for email generation
    
    for col_name, col_type in columns.items():
        # Check if this is a foreign key column
        if col_name in fk_map:
            ref_table = fk_map[col_name]
            if ref_table in generated_ids and generated_ids[ref_table]:
                value = random.choice(generated_ids[ref_table])
            else:
                return None  # Can't generate row without reference
        # Generate value based on column name and type
        elif col_name == "id":
            value = row_id
        elif col_name == "username":
            username = random_username()
            value = username
        elif col_name == "email":
            value = random_email(username or f"user{row_id}")
        elif col_name == "is_verified":
            value = random.choice([0, 1])  # SQLite boolean
        elif col_name == "country_code":
            value = random_country_code()
        elif col_name in ["content", "comment_text"]:
            value = random_content()
        elif col_name == "view_count":
            value = random.randint(0, 10000)
        elif col_type == "datetime":
            value = random_datetime()
        elif col_type == "int":
            value = random.randint(1, 1000)
        elif col_type in ["varchar", "text"]:
            value = f"{col_name}_{row_id}"
        elif col_type == "boolean":
            value = random.choice([0, 1])
        else:
            value = f"value_{row_id}"
        
        values.append(value)
    
    return tuple(values)


def verify_seeding(db_path: str) -> Dict[str, Any]:
    """
    Verify that the database was seeded correctly.
    
    Returns dict with table counts and sample data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    result = {}
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = cursor.fetchone()[0]
        
        cursor.execute(f'SELECT * FROM "{table}" LIMIT 3')
        samples = cursor.fetchall()
        
        result[table] = {
            "count": count,
            "samples": samples
        }
    
    conn.close()
    return result
