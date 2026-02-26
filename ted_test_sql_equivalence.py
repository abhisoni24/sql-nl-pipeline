"""
SQL Equivalence Tester.

Tests semantic equivalence of SQL statement pairs.
Uses sqlglot for parsing/validation and multiple comparison strategies.

Verification Pipeline:
1. Parse both SQL statements with sqlglot (validation)
2. Normalize both statements
3. Compare using multiple strategies:
   - AST comparison (after normalization)
   - String similarity (TED score)
   - Semantic heuristics

Usage:
    python test_sql_equivalence.py
    python test_sql_equivalence.py --input path/to/pairs.json
"""

import json
import sys
import os
import argparse
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

import sqlglot
from sqlglot import parse_one, exp
from sqlglot.optimizer import normalize

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.core.schema import USED_SQL_DIALECT


class EquivalenceResult(Enum):
    EQUIVALENT = "equivalent"
    NOT_EQUIVALENT = "not_equivalent"
    PARSE_ERROR = "parse_error"
    ERROR = "error"


@dataclass
class TestResult:
    pair_id: int
    result: EquivalenceResult
    expected_equivalent: bool
    is_correct: bool
    details: str
    sql1_valid: bool
    sql2_valid: bool


def validate_sql(sql: str, dialect: str = USED_SQL_DIALECT) -> Tuple[bool, Optional[exp.Expression], str]:
    """
    Validate SQL using sqlglot parsing.
    
    Returns:
        (is_valid, parsed_ast, error_message)
    """
    try:
        ast = parse_one(sql, dialect=dialect)
        return True, ast, ""
    except sqlglot.errors.ParseError as e:
        return False, None, f"Parse error: {str(e)}"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"


def normalize_ast(ast: exp.Expression) -> exp.Expression:
    """
    Normalize an AST for comparison.
    
    Applies standard normalizations:
    - Consistent formatting
    - Canonical ordering where applicable
    """
    try:
        # Try to use sqlglot's optimizer normalization
        return normalize.normalize(ast)
    except Exception:
        # If normalization fails, return as-is
        return ast


def compare_ast(ast1: exp.Expression, ast2: exp.Expression) -> Tuple[bool, str]:
    """
    Compare two ASTs for equivalence.
    """
    # Strategy 1: Direct AST equality after normalization
    try:
        norm1 = normalize_ast(ast1.copy())
        norm2 = normalize_ast(ast2.copy())
        
        if norm1 == norm2:
            return True, "AST match after normalization"
    except Exception:
        pass
    
    # Strategy 2: SQL string comparison after regeneration
    try:
        sql1_regen = ast1.sql(dialect=USED_SQL_DIALECT)
        sql2_regen = ast2.sql(dialect=USED_SQL_DIALECT)
        
        # Normalize whitespace and case
        sql1_norm = " ".join(sql1_regen.upper().split())
        sql2_norm = " ".join(sql2_regen.upper().split())
        
        if sql1_norm == sql2_norm:
            return True, "Regenerated SQL match"
    except Exception:
        pass
    
    # Strategy 3: Check structural equivalence
    try:
        # Compare expression types
        if type(ast1) != type(ast2):
            return False, f"Different expression types: {type(ast1).__name__} vs {type(ast2).__name__}"
        
        # For SELECT, compare key components
        if isinstance(ast1, exp.Select):
            # Check if same tables
            tables1 = set(t.name for t in ast1.find_all(exp.Table))
            tables2 = set(t.name for t in ast2.find_all(exp.Table))
            if tables1 != tables2:
                return False, f"Different tables: {tables1} vs {tables2}"
            
            # Check if same columns (by name, ignoring order)
            cols1 = set(ast1.named_selects)
            cols2 = set(ast2.named_selects)
            
            # If both have specific columns, compare as sets
            if cols1 and cols2 and cols1 != cols2:
                return False, f"Different columns: {cols1} vs {cols2}"
    except Exception:
        pass
    
    return False, "No equivalence strategy matched"


def compute_string_similarity(sql1: str, sql2: str) -> float:
    """
    Compute basic string similarity between two SQL statements.
    Uses Jaccard similarity on tokens.
    """
    def tokenize(sql: str) -> set:
        return set(sql.upper().replace(",", " ").replace("(", " ").replace(")", " ").split())
    
    tokens1 = tokenize(sql1)
    tokens2 = tokenize(sql2)
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    
    return len(intersection) / len(union) if union else 0.0


def check_semantic_equivalence(sql1: str, sql2: str) -> Tuple[EquivalenceResult, str]:
    """
    Main equivalence checking function.
    
    Returns:
        (result, explanation)
    """
    # Step 1: Validate both SQL statements
    valid1, ast1, err1 = validate_sql(sql1)
    valid2, ast2, err2 = validate_sql(sql2)
    
    if not valid1 or not valid2:
        errors = []
        if not valid1:
            errors.append(f"SQL1 invalid: {err1}")
        if not valid2:
            errors.append(f"SQL2 invalid: {err2}")
        return EquivalenceResult.PARSE_ERROR, "; ".join(errors)
    
    # Step 2: AST comparison
    is_equiv, explanation = compare_ast(ast1, ast2)
    if is_equiv:
        return EquivalenceResult.EQUIVALENT, explanation
    
    # Step 3: String similarity heuristic
    similarity = compute_string_similarity(sql1, sql2)
    
    # If very high similarity (>0.95), consider equivalent
    if similarity > 0.95:
        return EquivalenceResult.EQUIVALENT, f"High token similarity: {similarity:.2f}"
    
    # Step 4: Try additional semantic checks
    # Check known equivalent patterns
    sql1_upper = sql1.upper().strip().rstrip(";")
    sql2_upper = sql2.upper().strip().rstrip(";")
    
    # Normalize common variations
    normalizations = [
        ("INNER JOIN", "JOIN"),
        (" AS ", " "),
        ("  ", " "),
    ]
    
    norm1, norm2 = sql1_upper, sql2_upper
    for old, new in normalizations:
        norm1 = norm1.replace(old, new)
        norm2 = norm2.replace(old, new)
    
    if " ".join(norm1.split()) == " ".join(norm2.split()):
        return EquivalenceResult.EQUIVALENT, "Match after common normalizations"
    
    # If similarity is moderate, it's likely different
    if similarity < 0.5:
        return EquivalenceResult.NOT_EQUIVALENT, f"Low similarity: {similarity:.2f}. {explanation}"
    
    return EquivalenceResult.NOT_EQUIVALENT, f"Similarity: {similarity:.2f}. {explanation}"


def test_equivalence_pairs(pairs: List[Dict]) -> List[TestResult]:
    """
    Test all equivalence pairs.
    """
    results = []
    
    for pair in pairs:
        pair_id = pair["id"]
        sql1 = pair["sql1"]
        sql2 = pair["sql2"]
        expected = pair["should_be_equivalent"]
        
        # Validate each SQL
        valid1, _, _ = validate_sql(sql1)
        valid2, _, _ = validate_sql(sql2)
        
        # Check equivalence
        result, details = check_semantic_equivalence(sql1, sql2)
        
        # Determine if prediction matches expected
        actual_equivalent = (result == EquivalenceResult.EQUIVALENT)
        is_correct = (actual_equivalent == expected)
        
        results.append(TestResult(
            pair_id=pair_id,
            result=result,
            expected_equivalent=expected,
            is_correct=is_correct,
            details=details,
            sql1_valid=valid1,
            sql2_valid=valid2
        ))
    
    return results


def print_report(results: List[TestResult], pairs: List[Dict]):
    """
    Print a detailed test report.
    """
    print("\n" + "=" * 70)
    print("SQL EQUIVALENCE TEST REPORT")
    print("=" * 70)
    
    total = len(results)
    correct = sum(1 for r in results if r.is_correct)
    parse_errors = sum(1 for r in results if r.result == EquivalenceResult.PARSE_ERROR)
    
    print(f"\nTotal pairs tested: {total}")
    print(f"Correctly classified: {correct} ({100*correct/total:.1f}%)")
    print(f"Parse errors: {parse_errors}")
    
    # Breakdown by expected type
    expected_equiv = [r for r in results if r.expected_equivalent]
    expected_not_equiv = [r for r in results if not r.expected_equivalent]
    
    print(f"\nExpected Equivalent pairs: {len(expected_equiv)}")
    print(f"  - Correctly identified: {sum(1 for r in expected_equiv if r.is_correct)}")
    
    print(f"\nExpected Non-Equivalent pairs: {len(expected_not_equiv)}")
    print(f"  - Correctly identified: {sum(1 for r in expected_not_equiv if r.is_correct)}")
    
    # Show failures
    failures = [r for r in results if not r.is_correct]
    if failures:
        print(f"\n" + "-" * 70)
        print(f"MISCLASSIFIED PAIRS ({len(failures)}):")
        print("-" * 70)
        
        for r in failures[:10]:  # Show first 10
            pair = next(p for p in pairs if p["id"] == r.pair_id)
            print(f"\n[ID {r.pair_id}] Expected: {'equivalent' if r.expected_equivalent else 'not-equivalent'}")
            print(f"  Got: {r.result.value}")
            print(f"  Details: {r.details}")
            print(f"  SQL1: {pair['sql1'][:60]}...")
            print(f"  SQL2: {pair['sql2'][:60]}...")
    
    # Show parse errors
    if parse_errors > 0:
        print(f"\n" + "-" * 70)
        print("PARSE ERRORS:")
        print("-" * 70)
        
        for r in results:
            if r.result == EquivalenceResult.PARSE_ERROR:
                pair = next(p for p in pairs if p["id"] == r.pair_id)
                print(f"\n[ID {r.pair_id}] {r.details}")
                if not r.sql1_valid:
                    print(f"  SQL1: {pair['sql1']}")
                if not r.sql2_valid:
                    print(f"  SQL2: {pair['sql2']}")
    
    print("\n" + "=" * 70)
    
    return correct, total


def main():
    parser = argparse.ArgumentParser(description="Test SQL semantic equivalence")
    parser.add_argument("--input", default="./dataset/social_media/sql_equivalence_pairs.json",
                       help="Path to equivalence pairs JSON")
    parser.add_argument("--output", default=None,
                       help="Path to save results JSON")
    args = parser.parse_args()
    
    # Load pairs
    print(f"Loading pairs from: {args.input}")
    with open(args.input) as f:
        pairs = json.load(f)
    
    print(f"Loaded {len(pairs)} pairs")
    
    # Test equivalence
    print("Testing equivalence...")
    results = test_equivalence_pairs(pairs)
    
    # Print report
    correct, total = print_report(results, pairs)
    
    # Save results if requested
    if args.output:
        output_data = []
        for r in results:
            pair = next(p for p in pairs if p["id"] == r.pair_id)
            output_data.append({
                **pair,
                "result": r.result.value,
                "is_correct": r.is_correct,
                "details": r.details
            })
        
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")
    
    # Return exit code based on success rate
    return 0 if correct == total else 1


if __name__ == "__main__":
    sys.exit(main())
