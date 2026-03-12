"""
Database Snapshot Module.

Provides functionality to capture and compare database states
for DML equivalence verification.
"""

import sqlite3
import shutil
import os
from typing import Dict, List, Tuple, Any, Optional


class DatabaseSnapshot:
    """
    Captures and compares database states for DML verification.
    
    Provides functionality to:
    - Create copies of databases
    - Capture the state of all tables
    - Compare states between databases
    """
    
    def __init__(self, db_path: str):
        """
        Initialize with a database path.
        
        Args:
            db_path: Path to the SQLite database
        """
        self.db_path = db_path
    
    def copy_database(self, target_path: str) -> "DatabaseSnapshot":
        """
        Create an exact copy of this database.
        
        Args:
            target_path: Path for the copy
            
        Returns:
            New DatabaseSnapshot for the copy
        """
        # Ensure target directory exists
        os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
        
        # Copy the database file
        shutil.copy(self.db_path, target_path)
        
        return DatabaseSnapshot(target_path)
    
    def get_table_names(self) -> List[str]:
        """Get all table names in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    
    def get_table_state(
        self, 
        table_name: str,
        order_by: Optional[List[str]] = None
    ) -> List[Tuple]:
        """
        Get all rows from a table, ordered consistently.
        
        Args:
            table_name: Name of the table
            order_by: Optional list of columns to order by
            
        Returns:
            List of tuples representing rows
        """
        conn = sqlite3.connect(self.db_path)
        conn.text_factory = lambda b: b.decode(errors='ignore')
        cursor = conn.cursor()
        
        # Get column names for consistent ordering
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row[1] for row in cursor.fetchall()]
        
        if order_by:
            order_clause = ", ".join(f'"{c}"' for c in order_by)
        else:
            # Order by all columns for deterministic results
            order_clause = ", ".join(f'"{c}"' for c in columns)
        
        cursor.execute(f'SELECT * FROM "{table_name}" ORDER BY {order_clause}')
        rows = cursor.fetchall()
        conn.close()
        
        return rows
    
    def get_full_state(
        self, 
        table_names: Optional[List[str]] = None
    ) -> Dict[str, List[Tuple]]:
        """
        Capture state of all (or specified) tables.
        
        Args:
            table_names: Optional list of tables to capture
                        If None, captures all tables
                        
        Returns:
            Dictionary mapping table names to lists of rows
        """
        if table_names is None:
            table_names = self.get_table_names()
        
        state = {}
        for table in table_names:
            try:
                state[table] = self.get_table_state(table)
            except Exception as e:
                state[table] = f"ERROR: {str(e)}"
        
        return state
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        count = cursor.fetchone()[0]
        conn.close()
        return count
    
    def execute_dml(self, sql: str) -> Tuple[bool, str]:
        """
        Execute a DML statement on this database.
        
        Args:
            sql: The DML statement to execute
            
        Returns:
            (success, message) tuple
        """
        try:
            conn = sqlite3.connect(self.db_path)
            # Disable foreign keys for testing to avoid FK constraint errors
            conn.execute("PRAGMA foreign_keys = OFF")
            cursor = conn.cursor()
            cursor.execute(sql)
            affected = cursor.rowcount
            conn.commit()
            conn.close()
            return (True, f"Affected {affected} rows")
        except Exception as e:
            return (False, str(e))
    
    @staticmethod
    def compare_states(
        state1: Dict[str, List[Tuple]], 
        state2: Dict[str, List[Tuple]]
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Compare two database states for equality.
        
        Args:
            state1: First database state
            state2: Second database state
            
        Returns:
            (are_equal, details) where details contains diff information
        """
        details = {
            "matching_tables": [],
            "missing_in_first": [],
            "missing_in_second": [],
            "different_tables": {},
        }
        
        all_tables = set(state1.keys()) | set(state2.keys())
        
        is_equal = True
        
        for table in all_tables:
            if table not in state1:
                details["missing_in_first"].append(table)
                is_equal = False
            elif table not in state2:
                details["missing_in_second"].append(table)
                is_equal = False
            else:
                rows1 = state1[table]
                rows2 = state2[table]
                
                # Handle error cases
                if isinstance(rows1, str) or isinstance(rows2, str):
                    if rows1 != rows2:
                        details["different_tables"][table] = {
                            "error": f"State1: {rows1}, State2: {rows2}"
                        }
                        is_equal = False
                    continue
                
                # Compare row counts first (quick check)
                if len(rows1) != len(rows2):
                    details["different_tables"][table] = {
                        "row_count_diff": (len(rows1), len(rows2))
                    }
                    is_equal = False
                    continue
                
                # Compare sorted rows
                sorted1 = sorted(rows1, key=lambda r: tuple(str(x) for x in r))
                sorted2 = sorted(rows2, key=lambda r: tuple(str(x) for x in r))
                
                if sorted1 != sorted2:
                    # Find specific differences
                    diff_rows = []
                    for i, (r1, r2) in enumerate(zip(sorted1, sorted2)):
                        if r1 != r2:
                            diff_rows.append({
                                "index": i,
                                "state1": r1,
                                "state2": r2
                            })
                            if len(diff_rows) >= 5:  # Limit diff output
                                break
                    
                    details["different_tables"][table] = {
                        "diff_rows": diff_rows,
                        "total_diffs": sum(1 for r1, r2 in zip(sorted1, sorted2) if r1 != r2)
                    }
                    is_equal = False
                else:
                    details["matching_tables"].append(table)
        
        return is_equal, details
    
    def cleanup(self) -> None:
        """Remove the database file."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
