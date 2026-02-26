"""
SQL Equivalence Engine.

Unified interface for SQL equivalence checking that automatically routes
to the appropriate checker based on query type.
"""

import os
from typing import Optional, List

from .config import EquivalenceConfig, EquivalenceResult, EquivalenceCheckResult
from .schema_adapter import create_database_from_schema
from .seed_database import seed_database
from .dql_equivalence import DQLEquivalenceChecker
from .dml_equivalence import DMLEquivalenceChecker


class SQLEquivalenceEngine:
    """
    Unified interface for SQL equivalence checking.
    
    Automatically detects query type (SELECT, INSERT, UPDATE, DELETE)
    and routes to the appropriate checker.
    """
    
    def __init__(self, config: EquivalenceConfig):
        """
        Initialize the SQL equivalence engine.
        
        Args:
            config: Configuration for the engine
        """
        self.config = config
        self._ensure_base_database()
        
        # Initialize checkers
        self.dql_checker = DQLEquivalenceChecker(
            base_db_path=config.base_db_path,
            test_suite_dir=os.path.join(config.test_suite_dir, "dql"),
            max_fuzz_iterations=config.max_fuzz_iterations,
            max_distilled_dbs=config.max_distilled_dbs,
            order_matters=config.order_matters
        )
        
        self.dml_checker = DMLEquivalenceChecker(
            base_db_path=config.base_db_path,
            test_suite_dir=os.path.join(config.test_suite_dir, "dml"),
            max_fuzz_iterations=config.max_fuzz_iterations,
            max_distilled_dbs=config.max_distilled_dbs,
            affected_tables=config.dml_compare_tables
        )
    
    def _ensure_base_database(self) -> None:
        """Ensure the base database exists with schema and seed data."""
        if os.path.exists(self.config.base_db_path):
            return
        
        schema = self.config.schema
        foreign_keys = self.config.foreign_keys
        
        if schema is None or foreign_keys is None:
            raise RuntimeError(
                "Cannot create base database: schema and foreign_keys must be "
                "provided in EquivalenceConfig (or use SQLEquivalenceEngine.from_schema())."
            )
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.config.base_db_path) or ".", exist_ok=True)
        
        # Create database with schema
        create_database_from_schema(
            self.config.base_db_path,
            schema,
            foreign_keys,
            overwrite=True
        )
        
        # Seed with data
        seed_database(
            self.config.base_db_path,
            schema,
            foreign_keys,
            min_rows=self.config.min_rows_per_table,
            max_rows=self.config.max_rows_per_table
        )
    
    def check_equivalence(
        self,
        gold_sql: str,
        candidate_sql: str
    ) -> EquivalenceCheckResult:
        """
        Check if two SQL statements are semantically equivalent.
        
        Automatically detects the query type and routes to the
        appropriate checker.
        
        Args:
            gold_sql: The reference (gold) SQL statement
            candidate_sql: The candidate SQL statement to compare
            
        Returns:
            EquivalenceCheckResult with detailed information
        """
        # Detect query type
        gold_type = self._detect_query_type(gold_sql)
        candidate_type = self._detect_query_type(candidate_sql)
        
        # Check if types match
        if gold_type != candidate_type:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.NOT_EQUIVALENT,
                details=f"Query type mismatch: gold is {gold_type}, candidate is {candidate_type}",
                gold_sql=gold_sql,
                candidate_sql=candidate_sql,
                query_type=f"{gold_type}/{candidate_type}"
            )
        
        # Route to appropriate checker
        if gold_type == "SELECT":
            return self.dql_checker.check_equivalence(gold_sql, candidate_sql)
        elif gold_type in ("INSERT", "UPDATE", "DELETE"):
            return self.dml_checker.check_equivalence(gold_sql, candidate_sql)
        else:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.PARSE_ERROR,
                details=f"Unsupported query type: {gold_type}",
                gold_sql=gold_sql,
                candidate_sql=candidate_sql,
                query_type=gold_type
            )
    
    def _detect_query_type(self, sql: str) -> str:
        """
        Detect the type of SQL statement.
        
        Returns one of: SELECT, INSERT, UPDATE, DELETE, UNKNOWN
        """
        # Normalize and get first keyword
        sql_normalized = sql.strip().upper()
        
        # Handle common prefixes
        if sql_normalized.startswith("SELECT"):
            return "SELECT"
        elif sql_normalized.startswith("INSERT"):
            return "INSERT"
        elif sql_normalized.startswith("UPDATE"):
            return "UPDATE"
        elif sql_normalized.startswith("DELETE"):
            return "DELETE"
        elif sql_normalized.startswith("WITH"):
            # CTE - treat as SELECT
            return "SELECT"
        else:
            return "UNKNOWN"
    
    def cleanup(self) -> None:
        """Clean up all generated test databases."""
        self.dql_checker.cleanup()
        self.dml_checker.cleanup()
    
    @classmethod
    def from_schema(
        cls,
        schema: dict,
        foreign_keys: dict,
        db_path: str = "./test_dbs/base.sqlite",
        test_suite_dir: str = "./test_dbs",
        **config_kwargs
    ) -> "SQLEquivalenceEngine":
        """
        Create an engine from a schema definition.
        
        Args:
            schema: Dictionary mapping table names to column definitions
            foreign_keys: Dictionary of foreign key relationships
            db_path: Path for the base database
            test_suite_dir: Directory for test databases
            **config_kwargs: Additional config parameters
            
        Returns:
            Configured SQLEquivalenceEngine
        """
        # Create directory
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        
        # Create database
        create_database_from_schema(db_path, schema, foreign_keys, overwrite=True)
        
        # Seed database
        seed_database(db_path, schema, foreign_keys)
        
        # Create config (include schema/fk so _ensure_base_database is a no-op
        # since we already created the DB above, but store for reference)
        config = EquivalenceConfig(
            base_db_path=db_path,
            test_suite_dir=test_suite_dir,
            schema=schema,
            foreign_keys=foreign_keys,
            **config_kwargs
        )
        
        return cls(config)
