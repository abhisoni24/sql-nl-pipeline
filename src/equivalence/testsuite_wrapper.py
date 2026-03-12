"""
TestSuiteEval Integration Wrapper.

Provides integration with the TestSuiteEval codebase for:
- Neighbor query generation
- Database fuzzing
- Test suite distillation
"""

import os
import sys
import sqlite3
import random
import shutil
from typing import List, Dict, Tuple, Any, Optional
from pathlib import Path

# We'll use our own implementations that are compatible with TestSuiteEval's approach
# but don't require their specific import structure

from .schema_adapter import (
    get_testsuite_compatible_info,
    get_table_names,
)


class NeighborGenerator:
    """
    Generates neighbor queries for a gold SQL query.
    
    Neighbors are syntactically similar queries that should produce
    different results if the database distinguishes them properly.
    """
    
    # Operator families for replacements
    AGG_OPS = {'count', 'min', 'max', 'avg', 'sum'}
    CMP_OPS = {'>', '<', '>=', '<=', '=', '!=', '<>'}
    LOGICAL_OPS = {'AND', 'OR'}
    ORDER_OPS = {'ASC', 'DESC'}
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.table_column_props, _, self.table_column_elements = \
            get_testsuite_compatible_info(db_path)
        
        # Extract column names for replacements
        self.column_names = set(
            col for (table, col) in self.table_column_props.keys()
        )
    
    def generate_neighbors(self, query: str) -> List[str]:
        """
        Generate neighbor queries by:
        1. Dropping spans (removing parts of the query)
        2. Replacing tokens with alternatives
        """
        neighbors = set()
        
        # Tokenize query (simple word-based tokenization)
        tokens = self._tokenize(query)
        
        # Strategy 1: Drop spans
        neighbors.update(self._drop_spans(tokens))
        
        # Strategy 2: Replace operators
        neighbors.update(self._replace_operators(tokens))
        
        # Strategy 3: Replace column names
        neighbors.update(self._replace_columns(tokens))
        
        # Strategy 4: Replace numeric values
        neighbors.update(self._replace_numbers(tokens))
        
        # Strategy 5: Replace string values
        neighbors.update(self._replace_strings(tokens))
        
        # Filter out invalid queries and the original
        original_normalized = self._normalize(query)
        valid_neighbors = []
        
        for neighbor in neighbors:
            if self._normalize(neighbor) != original_normalized:
                if self._is_valid_sql(neighbor):
                    valid_neighbors.append(neighbor)
        
        return valid_neighbors
    
    def _tokenize(self, query: str) -> List[str]:
        """Simple tokenization preserving structure."""
        # Add spaces around punctuation for easier parsing
        for char in '(),;':
            query = query.replace(char, f' {char} ')
        return query.split()
    
    def _join_tokens(self, tokens: List[str]) -> str:
        """Reconstruct query from tokens."""
        result = ' '.join(tokens)
        # Clean up spaces around punctuation
        for char in '(),;':
            result = result.replace(f' {char} ', char)
            result = result.replace(f' {char}', char)
            result = result.replace(f'{char} ', f'{char} ')
        return result.strip()
    
    def _normalize(self, query: str) -> str:
        """Normalize query for comparison."""
        return ' '.join(query.upper().split())
    
    def _is_valid_sql(self, query: str) -> bool:
        """Check if query is valid SQL (can execute on empty DB)."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"EXPLAIN {query}")
            conn.close()
            return True
        except:
            return False
    
    def _drop_spans(self, tokens: List[str]) -> set:
        """Generate neighbors by dropping contiguous spans."""
        results = set()
        n = len(tokens)
        
        # Don't drop essential keywords
        essential = {'SELECT', 'FROM', 'INSERT', 'UPDATE', 'DELETE', 'INTO', 'SET'}
        
        for start in range(n):
            for end in range(start + 1, min(start + 4, n + 1)):  # Limit span size
                span = tokens[start:end]
                
                # Don't drop if it contains essential keywords
                if any(t.upper() in essential for t in span):
                    continue
                
                # Don't drop the whole query
                remaining = tokens[:start] + tokens[end:]
                if len(remaining) < 3:
                    continue
                
                results.add(self._join_tokens(remaining))
        
        return results
    
    def _replace_operators(self, tokens: List[str]) -> set:
        """Replace comparison and logical operators."""
        results = set()
        
        for i, token in enumerate(tokens):
            upper_token = token.upper()
            
            # Replace comparison operators
            if token in self.CMP_OPS:
                for replacement in self.CMP_OPS:
                    if replacement != token:
                        new_tokens = tokens[:i] + [replacement] + tokens[i+1:]
                        results.add(self._join_tokens(new_tokens))
            
            # Replace logical operators
            if upper_token in self.LOGICAL_OPS:
                for replacement in self.LOGICAL_OPS:
                    if replacement != upper_token:
                        new_tokens = tokens[:i] + [replacement] + tokens[i+1:]
                        results.add(self._join_tokens(new_tokens))
            
            # Replace ordering
            if upper_token in self.ORDER_OPS:
                for replacement in self.ORDER_OPS:
                    if replacement != upper_token:
                        new_tokens = tokens[:i] + [replacement] + tokens[i+1:]
                        results.add(self._join_tokens(new_tokens))
            
            # Replace aggregates
            if upper_token in {op.upper() for op in self.AGG_OPS}:
                for replacement in self.AGG_OPS:
                    if replacement.upper() != upper_token:
                        new_tokens = tokens[:i] + [replacement.upper()] + tokens[i+1:]
                        results.add(self._join_tokens(new_tokens))
        
        return results
    
    def _replace_columns(self, tokens: List[str]) -> set:
        """Replace column names with other columns."""
        results = set()
        
        for i, token in enumerate(tokens):
            # Check if this looks like a column name
            clean_token = token.lower().rstrip(',)')
            if clean_token in {c.lower() for c in self.column_names}:
                for replacement in list(self.column_names)[:10]:  # Limit replacements
                    if replacement.lower() != clean_token:
                        new_token = token.replace(clean_token, replacement)
                        new_tokens = tokens[:i] + [new_token] + tokens[i+1:]
                        results.add(self._join_tokens(new_tokens))
        
        return results
    
    def _replace_numbers(self, tokens: List[str]) -> set:
        """Replace numeric literals."""
        results = set()
        
        for i, token in enumerate(tokens):
            try:
                value = int(token)
                # Generate variations
                variations = [
                    value + 1,
                    value - 1,
                    value * 2 if value != 0 else 1,
                    0 if value != 0 else 1,
                    -value if value > 0 else abs(value) + 1,
                ]
                for new_val in variations:
                    new_tokens = tokens[:i] + [str(new_val)] + tokens[i+1:]
                    results.add(self._join_tokens(new_tokens))
            except ValueError:
                pass
        
        return results
    
    def _replace_strings(self, tokens: List[str]) -> set:
        """Replace string literals."""
        results = set()
        
        for i, token in enumerate(tokens):
            if (token.startswith("'") and token.endswith("'")) or \
               (token.startswith('"') and token.endswith('"')):
                quote = token[0]
                content = token[1:-1]
                
                # Generate variations
                variations = [
                    f"{quote}{quote}",  # Empty string
                    f"{quote}{content[:len(content)//2]}{quote}" if len(content) > 1 else f"{quote}x{quote}",
                    f"{quote}%{content}%{quote}",  # Add wildcards
                    f"{quote}different_value{quote}",
                ]
                
                for new_val in variations:
                    new_tokens = tokens[:i] + [new_val] + tokens[i+1:]
                    results.add(self._join_tokens(new_tokens))
        
        return results


class DatabaseFuzzer:
    """
    Generates random test databases from a seed database.
    
    The fuzzer creates databases with varied data that still respect
    the schema constraints, helping to distinguish between queries.
    """
    
    def __init__(self, seed_db_path: str, output_dir: str):
        self.seed_db_path = seed_db_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.table_column_props, self.child2parent, self.table_column_elements = \
            get_testsuite_compatible_info(seed_db_path)
    
    def generate_fuzzed_database(
        self, 
        db_name: str,
        query_values: Optional[List[Any]] = None
    ) -> str:
        """
        Generate a fuzzed database based on the seed.
        
        Args:
            db_name: Name for the new database
            query_values: Optional values from the query to include in fuzzing
            
        Returns:
            Path to the generated database
        """
        output_path = os.path.join(self.output_dir, f"{db_name}.sqlite")
        
        # Copy seed database structure
        shutil.copy(self.seed_db_path, output_path)
        
        # Modify data in the copy
        self._fuzz_data(output_path, query_values or [])
        
        return output_path
    
    def _fuzz_data(self, db_path: str, query_values: List[Any]) -> None:
        """Apply fuzzing transformations to database data."""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        tables = get_table_names(db_path)
        
        for table in tables:
            # Strategy 1: Randomly delete some rows
            if random.random() < 0.3:
                cursor.execute(f'DELETE FROM "{table}" WHERE RANDOM() % 3 = 0')
            
            # Strategy 2: Duplicate some rows with modifications
            if random.random() < 0.3:
                try:
                    cursor.execute(f'SELECT * FROM "{table}" LIMIT 5')
                    rows = cursor.fetchall()
                    
                    cursor.execute(f'PRAGMA table_info("{table}")')
                    columns = [col[1] for col in cursor.fetchall()]
                    pk_col = columns[0] if columns else None
                    
                    for row in rows:
                        # Modify non-primary key values slightly
                        new_row = list(row)
                        if pk_col:
                            # Generate new primary key
                            cursor.execute(f'SELECT MAX("{pk_col}") FROM "{table}"')
                            max_id = cursor.fetchone()[0] or 0
                            new_row[0] = max_id + random.randint(1, 100)
                        
                        # Modify some values
                        for i in range(1, len(new_row)):
                            if isinstance(new_row[i], int) and random.random() < 0.5:
                                new_row[i] = new_row[i] + random.randint(-10, 10)
                            elif isinstance(new_row[i], str) and random.random() < 0.3:
                                new_row[i] = new_row[i] + "_modified"
                        
                        placeholders = ", ".join(["?" for _ in new_row])
                        try:
                            cursor.execute(
                                f'INSERT INTO "{table}" VALUES ({placeholders})',
                                new_row
                            )
                        except sqlite3.IntegrityError:
                            pass  # Skip constraint violations
                except Exception:
                    pass
            
            # Strategy 3: Update some numeric values based on query values
            if query_values:
                for val in query_values:
                    if isinstance(val, int):
                        # Add boundary values
                        for v in [val - 1, val, val + 1]:
                            try:
                                cursor.execute(f'PRAGMA table_info("{table}")')
                                int_cols = [
                                    col[1] for col in cursor.fetchall() 
                                    if 'INT' in col[2].upper()
                                ]
                                for col in int_cols[:1]:  # Update first int column
                                    cursor.execute(
                                        f'UPDATE "{table}" SET "{col}" = ? WHERE RANDOM() % 10 = 0',
                                        (v,)
                                    )
                            except:
                                pass
        
        conn.commit()
        conn.close()


class TestSuiteGenerator:
    """
    Generates distilled test suites for SQL equivalence checking.
    
    Uses the greedy algorithm from TestSuiteEval to select databases
    that can distinguish the gold query from its neighbors.
    """
    
    def __init__(
        self, 
        base_db_path: str, 
        output_dir: str,
        max_fuzz_iterations: int = 50,
        max_distilled_dbs: int = 10
    ):
        self.base_db_path = base_db_path
        self.output_dir = output_dir
        self.max_fuzz_iterations = max_fuzz_iterations
        self.max_distilled_dbs = max_distilled_dbs
        
        self.neighbor_gen = NeighborGenerator(base_db_path)
        self.fuzzer = DatabaseFuzzer(base_db_path, output_dir)
    
    def generate_test_suite(self, gold_query: str) -> List[str]:
        """
        Generate a distilled test suite for a gold query.
        
        Algorithm:
        1. Generate neighbor queries
        2. Iteratively generate fuzzed databases
        3. Keep databases that distinguish gold from neighbors
        4. Stop when all neighbors are distinguished or max iterations reached
        
        Returns:
            List of paths to test databases
        """
        # Generate neighbors
        neighbors = self.neighbor_gen.generate_neighbors(gold_query)
        
        if not neighbors:
            # If no neighbors, just return one fuzzed database
            db_path = self.fuzzer.generate_fuzzed_database("test_db_0")
            return [db_path]
        
        # Extract values from query for smarter fuzzing
        query_values = self._extract_values_from_query(gold_query)
        
        undistinguished = set(neighbors)
        test_suite = []
        
        for iteration in range(self.max_fuzz_iterations):
            if len(undistinguished) == 0:
                break
            
            if len(test_suite) >= self.max_distilled_dbs:
                break
            
            # Generate a fuzzed database
            db_path = self.fuzzer.generate_fuzzed_database(
                f"test_db_{iteration}",
                query_values
            )
            
            # Check which neighbors this database distinguishes
            gold_result = self._execute_query(db_path, gold_query)
            
            distinguished_any = False
            for neighbor in list(undistinguished):
                neighbor_result = self._execute_query(db_path, neighbor)
                
                if not self._results_equal(gold_result, neighbor_result):
                    undistinguished.discard(neighbor)
                    distinguished_any = True
            
            if distinguished_any:
                test_suite.append(db_path)
            else:
                # Clean up unused database
                try:
                    os.unlink(db_path)
                except:
                    pass
        
        # Ensure we have at least one database
        if not test_suite:
            db_path = self.fuzzer.generate_fuzzed_database("test_db_fallback")
            test_suite.append(db_path)
        
        return test_suite
    
    def _extract_values_from_query(self, query: str) -> List[Any]:
        """Extract literal values from a query."""
        values = []
        tokens = query.replace("'", " ' ").replace('"', ' " ').split()
        
        in_string = False
        string_buffer = []
        
        for token in tokens:
            if token in ("'", '"'):
                if in_string:
                    values.append(' '.join(string_buffer))
                    string_buffer = []
                in_string = not in_string
            elif in_string:
                string_buffer.append(token)
            else:
                try:
                    values.append(int(token))
                except ValueError:
                    try:
                        values.append(float(token))
                    except ValueError:
                        pass
        
        return values
    
    def _execute_query(self, db_path: str, query: str) -> Tuple[str, Any]:
        """Execute a query and return (status, result)."""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            conn.close()
            return ("result", result)
        except Exception as e:
            return ("error", str(e))
    
    def _results_equal(
        self, 
        result1: Tuple[str, Any], 
        result2: Tuple[str, Any],
        order_matters: bool = False
    ) -> bool:
        """Compare two query results for equality."""
        status1, data1 = result1
        status2, data2 = result2
        
        # If either failed, they're not equal unless both failed the same way
        if status1 != status2:
            return False
        
        if status1 == "error":
            return data1 == data2
        
        # Compare results
        if len(data1) != len(data2):
            return False
        
        if order_matters:
            return data1 == data2
        else:
            # Compare as multisets
            return sorted(data1, key=str) == sorted(data2, key=str)
    
    def cleanup(self) -> None:
        """Remove all generated test databases."""
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
