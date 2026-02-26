#!/usr/bin/env python3
"""
SQL Equivalence Testing Script.

Tests semantic equivalence of SQL statement pairs using the TestSuiteEval
methodology for SELECT statements and state delta comparison for DML.

Usage:
    Single pair mode:
        python run_equivalence_test.py --gold "SELECT * FROM users" --candidate "SELECT * FROM users WHERE 1=1"
    
    Batch mode:
        python run_equivalence_test.py --input dataset/current/sql_equivalence_pairs.json --output results.json
    
    With custom database:
        python run_equivalence_test.py --db-path ./my_db.sqlite --input pairs.json
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import asdict

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.core.schema_loader import load_from_yaml
from src.equivalence import SQLEquivalenceEngine, EquivalenceConfig
from src.equivalence.schema_adapter import create_database_from_schema
from src.equivalence.seed_database import seed_database


def create_engine(args) -> SQLEquivalenceEngine:
    """Create and configure the equivalence engine."""
    schema_path = getattr(args, 'schema', 'schemas/social_media.yaml')
    schema_cfg = load_from_yaml(schema_path)
    schema = schema_cfg.get_legacy_schema()
    foreign_keys = schema_cfg.get_fk_pairs()

    db_path = args.db_path or f"./test_dbs/{schema_cfg.schema_name}.sqlite"
    test_suite_dir = args.test_suite_dir or "./test_dbs"
    
    # Ensure directories exist
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    os.makedirs(test_suite_dir, exist_ok=True)
    
    # Create database if it doesn't exist
    if not os.path.exists(db_path):
        print(f"Creating database at: {db_path}")
        create_database_from_schema(db_path, schema, foreign_keys, overwrite=True)
        
        print("Seeding database with sample data...")
        row_counts = seed_database(db_path, schema, foreign_keys)
        for table, count in row_counts.items():
            print(f"  {table}: {count} rows")
    
    # Create config
    config = EquivalenceConfig(
        base_db_path=db_path,
        test_suite_dir=test_suite_dir,
        max_fuzz_iterations=args.max_fuzz_iterations,
        max_distilled_dbs=args.max_distilled_dbs,
        order_matters=args.order_matters,
        cleanup_temp_dbs=not args.keep_temp_dbs,
        schema=schema,
        foreign_keys=foreign_keys,
    )
    
    return SQLEquivalenceEngine(config)


def test_single_pair(engine: SQLEquivalenceEngine, gold: str, candidate: str) -> Dict:
    """Test a single SQL pair."""
    result = engine.check_equivalence(gold, candidate)
    return asdict(result)


def test_batch(
    engine: SQLEquivalenceEngine, 
    pairs: List[Dict],
    verbose: bool = False
) -> List[Dict]:
    """Test a batch of SQL pairs."""
    results = []
    
    total = len(pairs)
    correct = 0
    errors = 0
    
    for i, pair in enumerate(pairs):
        pair_id = pair.get("id", i + 1)
        gold_sql = pair["sql1"]
        candidate_sql = pair["sql2"]
        expected = pair.get("should_be_equivalent", True)
        
        if verbose:
            print(f"[{i+1}/{total}] Testing pair {pair_id}...", end=" ")
        
        try:
            result = engine.check_equivalence(gold_sql, candidate_sql)
            
            is_correct = (result.is_equivalent == expected)
            if is_correct:
                correct += 1
            
            result_dict = {
                **pair,
                "actual_equivalent": result.is_equivalent,
                "result_type": result.result_type.value,
                "is_correct": is_correct,
                "details": result.details,
                "databases_tested": result.databases_tested
            }
            
            if verbose:
                status = "✓" if is_correct else "✗"
                print(f"{status} ({result.result_type.value})")
                
        except Exception as e:
            errors += 1
            result_dict = {
                **pair,
                "actual_equivalent": None,
                "result_type": "error",
                "is_correct": False,
                "details": str(e),
                "databases_tested": 0
            }
            
            if verbose:
                print(f"ERROR: {str(e)[:50]}")
        
        results.append(result_dict)
    
    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Total pairs: {total}")
    print(f"Correct: {correct} ({100*correct/total:.1f}%)")
    print(f"Errors: {errors}")
    print(f"{'='*60}")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Test SQL semantic equivalence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--gold", 
        type=str,
        help="Gold SQL statement (single pair mode)"
    )
    mode_group.add_argument(
        "--input", 
        type=str,
        help="Path to JSON file with equivalence pairs (batch mode)"
    )
    
    # Additional arguments for single mode
    parser.add_argument(
        "--candidate",
        type=str,
        help="Candidate SQL statement (required with --gold)"
    )
    
    # Output
    parser.add_argument(
        "--output",
        type=str,
        help="Path to save results JSON"
    )
    
    # Database configuration
    parser.add_argument(
        "--schema",
        type=str,
        default='schemas/social_media.yaml',
        help="Path to schema YAML file (default: schemas/social_media.yaml)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to base database (default: ./test_dbs/social_media.sqlite)"
    )
    parser.add_argument(
        "--test-suite-dir",
        type=str,
        default=None,
        help="Directory for test databases (default: ./test_dbs)"
    )
    
    # Testing parameters
    parser.add_argument(
        "--max-fuzz-iterations",
        type=int,
        default=50,
        help="Maximum fuzzing iterations (default: 50)"
    )
    parser.add_argument(
        "--max-distilled-dbs",
        type=int,
        default=10,
        help="Maximum databases in test suite (default: 10)"
    )
    parser.add_argument(
        "--order-matters",
        action="store_true",
        help="Consider result ordering for SELECT equivalence"
    )
    parser.add_argument(
        "--keep-temp-dbs",
        action="store_true",
        help="Keep temporary test databases after running"
    )
    
    # Verbosity
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.gold and not args.candidate:
        parser.error("--candidate is required when using --gold")
    
    # Create engine
    print("Initializing SQL Equivalence Engine...")
    engine = create_engine(args)
    
    try:
        if args.gold:
            # Single pair mode
            print(f"\nGold SQL: {args.gold}")
            print(f"Candidate SQL: {args.candidate}")
            print()
            
            result = test_single_pair(engine, args.gold, args.candidate)
            
            print(f"Result: {result['result_type']}")
            print(f"Equivalent: {result['is_equivalent']}")
            print(f"Details: {result['details']}")
            print(f"Databases tested: {result['databases_tested']}")
            
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(result, f, indent=2)
                print(f"\nResults saved to: {args.output}")
            
            # Return exit code based on equivalence
            return 0 if result['is_equivalent'] else 1
            
        else:
            # Batch mode
            print(f"Loading pairs from: {args.input}")
            with open(args.input) as f:
                pairs = json.load(f)
            
            print(f"Loaded {len(pairs)} pairs")
            print()
            
            results = test_batch(engine, pairs, verbose=args.verbose)
            
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(results, f, indent=2)
                print(f"\nResults saved to: {args.output}")
            
            # Calculate accuracy
            correct = sum(1 for r in results if r.get("is_correct", False))
            return 0 if correct == len(pairs) else 1
    
    finally:
        # Cleanup if requested
        if not args.keep_temp_dbs:
            print("\nCleaning up temporary databases...")
            engine.cleanup()


if __name__ == "__main__":
    sys.exit(main())
