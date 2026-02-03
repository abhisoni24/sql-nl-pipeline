"""
DQL (SELECT) Equivalence Checker.

Implements semantic equivalence checking for SELECT statements using
denotation comparison across a distilled test suite.
"""

import sqlite3
from typing import Tuple, Dict, Any, List, Optional
import os

from .config import EquivalenceResult, EquivalenceCheckResult
from .testsuite_wrapper import TestSuiteGenerator


class DQLEquivalenceChecker:
    """
    Checks semantic equivalence of SELECT statements.
    
    Uses the TestSuiteEval methodology:
    1. Generate neighbor queries from the gold query
    2. Create fuzzed test databases
    3. Distill to a minimal set that distinguishes neighbors
    4. Compare denotations of gold and candidate across all test databases
    """
    
    def __init__(
        self,
        base_db_path: str,
        test_suite_dir: str,
        max_fuzz_iterations: int = 50,
        max_distilled_dbs: int = 10,
        order_matters: bool = False
    ):
        """
        Initialize the DQL equivalence checker.
        
        Args:
            base_db_path: Path to the seed database
            test_suite_dir: Directory to store generated test databases
            max_fuzz_iterations: Maximum fuzzing iterations
            max_distilled_dbs: Maximum databases in test suite
            order_matters: Whether result ordering matters for equivalence
        """
        self.base_db_path = base_db_path
        self.test_suite_dir = test_suite_dir
        self.order_matters = order_matters
        
        self.testsuite_gen = TestSuiteGenerator(
            base_db_path,
            test_suite_dir,
            max_fuzz_iterations,
            max_distilled_dbs
        )
    
    def check_equivalence(
        self,
        gold_sql: str,
        candidate_sql: str
    ) -> EquivalenceCheckResult:
        """
        Check if two SELECT statements are semantically equivalent.
        
        Args:
            gold_sql: The reference (gold) SQL query
            candidate_sql: The candidate SQL query to compare
            
        Returns:
            EquivalenceCheckResult with detailed information
        """
        # Generate test suite based on the gold query
        try:
            test_databases = self.testsuite_gen.generate_test_suite(gold_sql)
        except Exception as e:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Failed to generate test suite: {str(e)}",
                gold_sql=gold_sql,
                candidate_sql=candidate_sql,
                query_type="SELECT"
            )
        
        # Compare denotations across all test databases
        for db_path in test_databases:
            # Execute gold query
            gold_status, gold_result = self._execute_query(db_path, gold_sql)
            
            if gold_status == "error":
                return EquivalenceCheckResult(
                    is_equivalent=False,
                    result_type=EquivalenceResult.EXECUTION_ERROR,
                    details=f"Gold query execution failed: {gold_result}",
                    gold_sql=gold_sql,
                    candidate_sql=candidate_sql,
                    query_type="SELECT",
                    databases_tested=test_databases.index(db_path) + 1,
                    failed_database=db_path,
                    gold_result=str(gold_result)
                )
            
            # Execute candidate query
            cand_status, cand_result = self._execute_query(db_path, candidate_sql)
            
            if cand_status == "error":
                return EquivalenceCheckResult(
                    is_equivalent=False,
                    result_type=EquivalenceResult.EXECUTION_ERROR,
                    details=f"Candidate query execution failed: {cand_result}",
                    gold_sql=gold_sql,
                    candidate_sql=candidate_sql,
                    query_type="SELECT",
                    databases_tested=test_databases.index(db_path) + 1,
                    failed_database=db_path,
                    candidate_result=str(cand_result)
                )
            
            # Compare results
            if not self._compare_denotations(gold_result, cand_result):
                return EquivalenceCheckResult(
                    is_equivalent=False,
                    result_type=EquivalenceResult.NOT_EQUIVALENT,
                    details=f"Different denotations on database: {os.path.basename(db_path)}",
                    gold_sql=gold_sql,
                    candidate_sql=candidate_sql,
                    query_type="SELECT",
                    databases_tested=test_databases.index(db_path) + 1,
                    failed_database=db_path,
                    gold_result=str(gold_result[0][:5]) + "..." if len(gold_result[0]) > 5 else str(gold_result[0]),
                    candidate_result=str(cand_result[0][:5]) + "..." if len(cand_result[0]) > 5 else str(cand_result[0])
                )
        
        # All databases matched
        return EquivalenceCheckResult(
            is_equivalent=True,
            result_type=EquivalenceResult.EQUIVALENT,
            details=f"Equivalent across {len(test_databases)} test databases",
            gold_sql=gold_sql,
            candidate_sql=candidate_sql,
            query_type="SELECT",
            databases_tested=len(test_databases)
        )
    
    def _execute_query(
        self, 
        db_path: str, 
        query: str
    ) -> Tuple[str, Any]:
        """
        Execute a SELECT query on a database.
        
        Returns:
            (status, (result, col_count)) where status is "result" or "error"
        """
        try:
            conn = sqlite3.connect(db_path)
            conn.text_factory = lambda b: b.decode(errors='ignore')
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            col_count = len(cursor.description) if cursor.description else 0
            conn.close()
            return ("result", (result, col_count))
        except Exception as e:
            return ("error", str(e))
    
    def _compare_denotations(
        self,
        result1_data: Tuple[List[Tuple], int],
        result2_data: Tuple[List[Tuple], int]
    ) -> bool:
        """
        Compare two query results for equivalence.
        
        Handles:
        - Row order (if order_matters is False)
        - Column permutations (treats column order as not significant)
        - Type coercion for comparable values
        """
        rows1, cols1 = result1_data
        rows2, cols2 = result2_data
        
        # Check column count (ALWAYS)
        if cols1 != cols2:
            return False
            
        # Empty results (now checking columns first)
        if len(rows1) == 0 and len(rows2) == 0:
            return True
        
        # Different number of rows
        if len(rows1) != len(rows2):
            return False
            
        if self.order_matters:
            return rows1 == rows2
        else:
            # Compare as multisets with column permutation support
            return self._multiset_equal_with_column_permutation(rows1, rows2)
    
    def _multiset_equal_with_column_permutation(
        self, 
        result1: List[Tuple], 
        result2: List[Tuple]
    ) -> bool:
        """
        Check if two result sets are equal as multisets,
        allowing for column permutations.
        """
        if not result1 or not result2:
            return result1 == result2
        
        num_cols = len(result1[0])
        
        # First try exact match (no permutation needed)
        if self._multiset_equal(result1, result2):
            return True
        
        # For small number of columns, try all permutations
        if num_cols <= 6:
            from itertools import permutations
            
            for perm in permutations(range(num_cols)):
                permuted_result2 = [
                    tuple(row[i] for i in perm) 
                    for row in result2
                ]
                if self._multiset_equal(result1, permuted_result2):
                    return True
        else:
            # For larger columns, use heuristic based on column value sets
            cols1 = [sorted([str(row[i]) for row in result1]) for i in range(num_cols)]
            cols2 = [sorted([str(row[i]) for row in result2]) for i in range(num_cols)]
            
            if sorted(tuple(c) for c in cols1) == sorted(tuple(c) for c in cols2):
                return True
        
        return False
    
    def _multiset_equal(
        self, 
        result1: List[Tuple], 
        result2: List[Tuple]
    ) -> bool:
        """Check if two result sets are equal as multisets (exact column order)."""
        def normalize_row(row: Tuple) -> Tuple:
            return tuple(str(x) if x is not None else "NULL" for x in row)
        
        normalized1 = [normalize_row(row) for row in result1]
        normalized2 = [normalize_row(row) for row in result2]
        
        return sorted(normalized1) == sorted(normalized2)
    
    def cleanup(self) -> None:
        """Clean up generated test databases."""
        self.testsuite_gen.cleanup()
