"""
DML (INSERT, UPDATE, DELETE) Equivalence Checker.

Implements semantic equivalence checking for DML statements using
"State Delta" comparison across distilled test databases.
"""

import sqlite3
import os
import re
from typing import Tuple, Dict, Any, List, Optional

from .config import EquivalenceResult, EquivalenceCheckResult
from .testsuite_wrapper import TestSuiteGenerator, DatabaseFuzzer
from .dml_snapshot import DatabaseSnapshot


class DMLEquivalenceChecker:
    """
    Checks semantic equivalence of DML (INSERT, UPDATE, DELETE) statements.
    
    Uses the "State Delta" comparison approach:
    1. For each database in the test suite:
       a. Create two identical copies (Trial A, Trial B)
       b. Execute gold DML on Trial A
       c. Execute candidate DML on Trial B
       d. Compare full state of affected tables
    2. Statements are equivalent iff all trials match
    """
    
    def __init__(
        self,
        base_db_path: str,
        test_suite_dir: str,
        max_fuzz_iterations: int = 20,
        max_distilled_dbs: int = 5,
        affected_tables: Optional[List[str]] = None
    ):
        """
        Initialize the DML equivalence checker.
        
        Args:
            base_db_path: Path to the seed database
            test_suite_dir: Directory to store generated test databases
            max_fuzz_iterations: Maximum fuzzing iterations
            max_distilled_dbs: Maximum databases in test suite
            affected_tables: Tables to compare (None = auto-detect)
        """
        self.base_db_path = base_db_path
        self.test_suite_dir = test_suite_dir
        self.affected_tables = affected_tables
        
        self.fuzzer = DatabaseFuzzer(base_db_path, test_suite_dir)
        self.max_fuzz_iterations = max_fuzz_iterations
        self.max_distilled_dbs = max_distilled_dbs
    
    def check_equivalence(
        self,
        gold_sql: str,
        candidate_sql: str,
        affected_tables: Optional[List[str]] = None
    ) -> EquivalenceCheckResult:
        """
        Check if two DML statements are semantically equivalent.
        
        Args:
            gold_sql: The reference (gold) DML statement
            candidate_sql: The candidate DML statement to compare
            affected_tables: Tables to compare (overrides init value)
            
        Returns:
            EquivalenceCheckResult with detailed information
        """
        # Determine which tables to compare
        tables_to_compare = affected_tables or self.affected_tables
        if tables_to_compare is None:
            tables_to_compare = self._infer_affected_tables(gold_sql, candidate_sql)
        
        # Detect query type
        query_type = self._detect_dml_type(gold_sql)
        
        # Generate test databases
        test_databases = self._generate_dml_test_suite(gold_sql)
        
        temp_dbs = []  # Track temp DBs for cleanup
        
        try:
            for i, db_path in enumerate(test_databases):
                # Create two copies for comparison
                trial_a_path = db_path.replace(".sqlite", "_trial_a.sqlite")
                trial_b_path = db_path.replace(".sqlite", "_trial_b.sqlite")
                
                snapshot = DatabaseSnapshot(db_path)
                trial_a = snapshot.copy_database(trial_a_path)
                trial_b = snapshot.copy_database(trial_b_path)
                
                temp_dbs.extend([trial_a_path, trial_b_path])
                
                # Execute gold DML on Trial A
                gold_success, gold_msg = trial_a.execute_dml(gold_sql)
                if not gold_success:
                    return EquivalenceCheckResult(
                        is_equivalent=False,
                        result_type=EquivalenceResult.EXECUTION_ERROR,
                        details=f"Gold DML execution failed: {gold_msg}",
                        gold_sql=gold_sql,
                        candidate_sql=candidate_sql,
                        query_type=query_type,
                        databases_tested=i + 1,
                        failed_database=db_path,
                        gold_result=gold_msg
                    )
                
                # Execute candidate DML on Trial B
                cand_success, cand_msg = trial_b.execute_dml(candidate_sql)
                if not cand_success:
                    return EquivalenceCheckResult(
                        is_equivalent=False,
                        result_type=EquivalenceResult.EXECUTION_ERROR,
                        details=f"Candidate DML execution failed: {cand_msg}",
                        gold_sql=gold_sql,
                        candidate_sql=candidate_sql,
                        query_type=query_type,
                        databases_tested=i + 1,
                        failed_database=db_path,
                        candidate_result=cand_msg
                    )
                
                # Compare states
                state_a = trial_a.get_full_state(tables_to_compare)
                state_b = trial_b.get_full_state(tables_to_compare)
                
                states_equal, diff_details = DatabaseSnapshot.compare_states(
                    state_a, state_b
                )
                
                if not states_equal:
                    return EquivalenceCheckResult(
                        is_equivalent=False,
                        result_type=EquivalenceResult.NOT_EQUIVALENT,
                        details=f"Different database states after DML execution. Tables with differences: {list(diff_details.get('different_tables', {}).keys())}",
                        gold_sql=gold_sql,
                        candidate_sql=candidate_sql,
                        query_type=query_type,
                        databases_tested=i + 1,
                        failed_database=db_path,
                        gold_result=str(diff_details)[:200],
                        candidate_result=str(diff_details)[:200]
                    )
        
        finally:
            # Cleanup temporary trial databases
            for temp_db in temp_dbs:
                try:
                    os.unlink(temp_db)
                except:
                    pass
        
        # All databases matched
        return EquivalenceCheckResult(
            is_equivalent=True,
            result_type=EquivalenceResult.EQUIVALENT,
            details=f"Equivalent across {len(test_databases)} test databases. Tables compared: {tables_to_compare}",
            gold_sql=gold_sql,
            candidate_sql=candidate_sql,
            query_type=query_type,
            databases_tested=len(test_databases)
        )
    
    def _generate_dml_test_suite(self, dml_sql: str) -> List[str]:
        """
        Generate test databases for DML testing.
        
        For DML, we use simpler fuzzing focused on the affected tables.
        """
        databases = []
        
        # Extract values from the DML for smarter fuzzing
        query_values = self._extract_dml_values(dml_sql)
        
        # Generate fewer databases for DML (state comparison is expensive)
        num_dbs = min(self.max_distilled_dbs, 5)
        
        for i in range(num_dbs):
            db_path = self.fuzzer.generate_fuzzed_database(
                f"dml_test_db_{i}",
                query_values
            )
            databases.append(db_path)
        
        return databases
    
    def _detect_dml_type(self, sql: str) -> str:
        """Detect the type of DML statement."""
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("INSERT"):
            return "INSERT"
        elif sql_upper.startswith("UPDATE"):
            return "UPDATE"
        elif sql_upper.startswith("DELETE"):
            return "DELETE"
        else:
            return "UNKNOWN"
    
    def _infer_affected_tables(
        self, 
        gold_sql: str, 
        candidate_sql: str
    ) -> List[str]:
        """
        Infer which tables are affected by the DML statements.
        """
        tables = set()
        
        for sql in [gold_sql, candidate_sql]:
            # Extract table from INSERT
            insert_match = re.search(
                r'INSERT\s+INTO\s+(\w+)',
                sql,
                re.IGNORECASE
            )
            if insert_match:
                tables.add(insert_match.group(1))
            
            # Extract table from UPDATE
            update_match = re.search(
                r'UPDATE\s+(\w+)',
                sql,
                re.IGNORECASE
            )
            if update_match:
                tables.add(update_match.group(1))
            
            # Extract table from DELETE
            delete_match = re.search(
                r'DELETE\s+FROM\s+(\w+)',
                sql,
                re.IGNORECASE
            )
            if delete_match:
                tables.add(delete_match.group(1))
        
        return list(tables) if tables else None
    
    def _extract_dml_values(self, sql: str) -> List[Any]:
        """Extract literal values from a DML statement."""
        values = []
        
        # Extract strings
        string_pattern = r"'([^']*)'"
        for match in re.finditer(string_pattern, sql):
            values.append(match.group(1))
        
        # Extract numbers
        number_pattern = r'\b(\d+)\b'
        for match in re.finditer(number_pattern, sql):
            try:
                values.append(int(match.group(1)))
            except ValueError:
                pass
        
        return values
    
    def cleanup(self) -> None:
        """Clean up generated test databases."""
        import shutil
        if os.path.exists(self.test_suite_dir):
            shutil.rmtree(self.test_suite_dir)
