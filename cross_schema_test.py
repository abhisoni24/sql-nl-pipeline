"""
Cross-Schema SQL Generation and Validation.

Generates SQL queries from any YAML schema and runs structural validation
to prove the pipeline is fully schema-agnostic.

Usage:
    python cross_schema_test.py schemas/hospital.yaml
    python cross_schema_test.py schemas/bank.yaml
    python cross_schema_test.py schemas/social_media.yaml
"""

import json
import sys
import os
import argparse
import random

sys.path.insert(0, os.path.abspath('.'))

from sqlglot import exp, parse_one
from src.core.schema_loader import load_from_yaml
from src.core.generator import SQLQueryGenerator


def generate_dataset(schema_path, num_per_complexity=20):
    """Generate SQL dataset from a YAML schema."""
    cfg = load_from_yaml(schema_path)

    gen = SQLQueryGenerator(cfg)
    dataset = gen.generate_dataset(num_per_complexity=num_per_complexity)
    return dataset, cfg


def validate_dataset(dataset, cfg):
    """Run structural validation checks on the generated dataset."""
    schema = cfg.get_legacy_schema()
    fks = cfg.get_fk_pairs()
    known_tables = set(schema.keys())
    known_complexities = {"simple", "join", "advanced", "union", "insert", "update", "delete"}
    dialect = cfg.dialect

    passed = 0
    failed = 0
    failures = []

    for record in dataset:
        rec_id = record["id"]
        comp = record["complexity"]
        sql = record["sql"]
        tables = record.get("tables", [])

        # --- Check 1: Required keys ---
        for key in ("id", "complexity", "sql", "tables"):
            if key in record:
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] Missing key: {key}")

        # --- Check 2: Complexity is known ---
        if comp in known_complexities:
            passed += 1
        else:
            failed += 1
            failures.append(f"[{rec_id}] Unknown complexity: {comp}")

        # --- Check 3: SQL is parseable ---
        try:
            ast = parse_one(sql, dialect=dialect)
            passed += 1
        except Exception as e:
            failed += 1
            failures.append(f"[{rec_id}] Parse error: {e}")
            continue

        # --- Check 4: SQL is non-empty ---
        if len(sql.strip()) > 0:
            passed += 1
        else:
            failed += 1
            failures.append(f"[{rec_id}] Empty SQL")

        # --- Check 5: Tables are from the schema ---
        all_tables_valid = True
        for t in tables:
            if t in known_tables:
                passed += 1
            else:
                # Aliases like "derived_table" are not real table names
                # Skip validation for derived/alias names
                if t.startswith("derived") or t.startswith("inner_") or t.startswith("sub_"):
                    passed += 1  # These are subquery aliases, not table names
                else:
                    failed += 1
                    all_tables_valid = False
                    failures.append(f"[{rec_id}] Unknown table: '{t}' (known: {sorted(known_tables)})")

        # --- Check 6: No Python repr artifacts ---
        bad_patterns = ["<class", "None", "True", "False"]
        for pat in bad_patterns:
            # Only flag "None" / "True" / "False" if they appear as standalone tokens
            # not inside strings
            if pat in ("None", "True", "False"):
                # Check if it appears outside of string literals
                stripped = sql
                for literal in ast.find_all(exp.Literal):
                    stripped = stripped.replace(str(literal), "")
                if pat in stripped:
                    failed += 1
                    failures.append(f"[{rec_id}] Python repr artifact: {pat}")
                else:
                    passed += 1
            elif pat in sql:
                failed += 1
                failures.append(f"[{rec_id}] Python repr artifact: {pat}")
            else:
                passed += 1

        # --- Type-specific checks ---
        if comp == "simple":
            # Must be a SELECT
            if isinstance(ast, exp.Select):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] simple: not a SELECT")
            # No JOINs
            if not ast.find(exp.Join):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] simple: has JOIN")

        elif comp == "join":
            # Must have a JOIN
            if ast.find(exp.Join):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] join: no JOIN found")

        elif comp == "union":
            # Must be a Union node
            if isinstance(ast, exp.Union):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] union: not a Union node")

        elif comp == "insert":
            if isinstance(ast, exp.Insert):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] insert: not an Insert node")

        elif comp == "update":
            if isinstance(ast, exp.Update):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] update: not an Update node")

        elif comp == "delete":
            if isinstance(ast, exp.Delete):
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] delete: not a Delete node")

        elif comp == "advanced":
            # Should have at least one of: subquery, self-join, EXISTS
            has_subquery = bool(ast.find(exp.Subquery))
            has_in = bool(ast.find(exp.In))
            has_exists = bool(ast.find(exp.Exists))
            has_self_join = False
            joins = list(ast.find_all(exp.Join))
            if joins:
                from_tables = [t.name for t in ast.find_all(exp.Table)]
                if len(from_tables) >= 2 and len(set(from_tables)) < len(from_tables):
                    has_self_join = True
            if has_subquery or has_in or has_exists or has_self_join:
                passed += 1
            else:
                failed += 1
                failures.append(f"[{rec_id}] advanced: no subquery/self-join/exists pattern")

    return passed, failed, failures


def main():
    parser = argparse.ArgumentParser(description="Cross-schema SQL generation and validation")
    parser.add_argument("schema", help="Path to YAML schema file")
    parser.add_argument("--num", type=int, default=20, help="Number of queries per complexity type")
    parser.add_argument("--output", help="Output JSON file path (optional)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show failures")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  Cross-Schema SQL Generation Test: {args.schema}")
    print(f"{'='*70}\n")

    # Generate dataset
    print(f"Loading schema from: {args.schema}")
    dataset, cfg = generate_dataset(args.schema, num_per_complexity=args.num)
    print(f"Schema: {cfg.schema_name} ({len(cfg.tables)} tables, {len(cfg.foreign_keys)} FKs)")
    print(f"Generated: {len(dataset)} queries\n")

    # Show complexity breakdown
    from collections import Counter
    comp_counts = Counter(r["complexity"] for r in dataset)
    print("Queries by complexity:")
    for comp, count in sorted(comp_counts.items()):
        print(f"  {comp:12s}: {count}")

    # Save dataset
    if args.output:
        output_path = args.output
    else:
        output_path = f"dataset/{cfg.schema_name}/raw_queries.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(dataset, f, indent=2)
    print(f"\nSaved to: {output_path}")

    # Validate
    print(f"\nRunning structural validation...")
    passed, failed, failures = validate_dataset(dataset, cfg)

    print(f"\n{'='*70}")
    print(f"  Validation Results: {cfg.schema_name}")
    print(f"{'='*70}")
    print(f"  Total checks : {passed + failed}")
    print(f"  Passed       : {passed}")
    print(f"  Failed       : {failed}")
    print(f"{'='*70}")

    if failures and args.verbose:
        print(f"\nFailures:")
        for f_msg in failures[:20]:
            print(f"  ✗ {f_msg}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more")

    # Show sample queries
    print(f"\nSample queries from {cfg.schema_name}:")
    for comp in ["simple", "join", "insert", "delete"]:
        samples = [r for r in dataset if r["complexity"] == comp]
        if samples:
            sample = random.choice(samples)
            print(f"\n  [{comp}] {sample['sql'][:120]}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
