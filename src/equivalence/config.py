"""
Configuration for the SQL Equivalence Framework.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple
from enum import Enum


class EquivalenceResult(Enum):
    """Result of an equivalence check."""
    EQUIVALENT = "equivalent"
    NOT_EQUIVALENT = "not_equivalent"
    EXECUTION_ERROR = "execution_error"
    PARSE_ERROR = "parse_error"


@dataclass
class EquivalenceCheckResult:
    """Detailed result of an equivalence check."""
    is_equivalent: bool
    result_type: EquivalenceResult
    details: str
    gold_sql: str
    candidate_sql: str
    query_type: str  # SELECT, INSERT, UPDATE, DELETE
    databases_tested: int = 0
    failed_database: Optional[str] = None
    gold_result: Optional[str] = None
    candidate_result: Optional[str] = None
    

@dataclass  
class EquivalenceConfig:
    """Configuration for the SQL equivalence framework."""
    
    # Base database path (with schema and seed data)
    base_db_path: str
    
    # Directory for test suite databases
    test_suite_dir: str = "./test_dbs"
    
    # Number of fuzzing iterations for test suite generation
    max_fuzz_iterations: int = 100
    
    # Max databases to include in distilled test suite  
    max_distilled_dbs: int = 10
    
    # Whether to consider result ordering for SELECT
    order_matters: bool = False
    
    # Cleanup temp databases after checking
    cleanup_temp_dbs: bool = True
    
    # Schema definition: {table_name: {col_name: col_type}}
    schema: Optional[Dict[str, Dict[str, str]]] = None
    
    # Foreign key relationships: {(from_table, to_table): (from_col, to_col)}
    foreign_keys: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None
    
    # Path to TestSuiteEval directory
    testsuite_eval_path: str = ""
    
    # Tables to compare for DML operations (None = all tables)
    dml_compare_tables: Optional[List[str]] = None
    
    # Minimum rows to generate per table during seeding
    min_rows_per_table: int = 30
    
    # Maximum rows to generate per table during seeding
    max_rows_per_table: int = 100
