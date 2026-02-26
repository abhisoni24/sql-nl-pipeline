"""
Step 1: Generate SQL dataset.

Reads a schema definition (YAML or legacy) and produces raw SQL queries
across all complexity types (simple, join, advanced, union, insert, update, delete).

Usage
-----
  # Schema-driven (recommended)
  python 01_generate_sql_dataset.py --schema schemas/social_media.yaml

  # With custom output path and query count
  python 01_generate_sql_dataset.py --schema schemas/bank.yaml -n 100 -o dataset/bank/raw_queries.json

  # Legacy mode (defaults to social_media via hardcoded src/core/schema.py)
  python 01_generate_sql_dataset.py
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.core.generator import SQLQueryGenerator


def main():
    parser = argparse.ArgumentParser(
        description="Generate raw SQL queries for a given schema."
    )
    parser.add_argument(
        "--schema", "-s", default=None,
        help="Path to a schema file — YAML (e.g. schemas/social_media.yaml) or "
             "SQLite (e.g. database.sqlite). "
             "If omitted, falls back to the legacy hardcoded social_media schema."
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output JSON file path. Defaults to dataset/<schema_name>/raw_queries.json"
    )
    parser.add_argument(
        "--num-per-complexity", "-n", type=int, default=50,
        help="Number of queries to generate per complexity type (default: 50)"
    )
    args = parser.parse_args()

    # ── Load schema ──────────────────────────────────────────────────
    if args.schema:
        from src.core.schema_loader import load_schema
        schema_cfg = load_schema(args.schema)
        schema = schema_cfg.get_legacy_schema()
        foreign_keys = schema_cfg.get_fk_pairs()
        schema_name = schema_cfg.schema_name
        dialect = schema_cfg.dialect
        schema_source = args.schema
        print(f"Loaded schema '{schema_name}' from {args.schema}")
    else:
        from src.core.schema import SCHEMA, FOREIGN_KEYS
        schema = SCHEMA
        foreign_keys = FOREIGN_KEYS
        schema_name = "social_media"
        dialect = "sqlite"
        schema_source = "src/core/schema.py (legacy)"
        print("Using legacy hardcoded social_media schema")

    # ── Determine output path ────────────────────────────────────────
    if args.output:
        output_file = args.output
    else:
        output_file = f"./dataset/{schema_name}/raw_queries.json"

    # ── Derive composite PK tables ────────────────────────────────────
    if args.schema:
        composite_pks = {}
        for tname, tdef in schema_cfg.tables.items():
            if "id" not in tdef.columns:
                fk_cols = {c.name for c in tdef.columns.values() if c.is_fk}
                if fk_cols:
                    composite_pks[tname] = fk_cols
        type_sets = schema_cfg.get_type_sets()
    else:
        composite_pks = None   # legacy fallback inside generator
        type_sets = None

    # ── Generate queries ─────────────────────────────────────────────
    generator = SQLQueryGenerator(schema, foreign_keys, type_sets=type_sets, composite_pks=composite_pks)
    print(f"Generating {args.num_per_complexity} queries per complexity type...")
    records = generator.generate_dataset(num_per_complexity=args.num_per_complexity)
    print(f"Successfully generated {len(records)} queries.")

    # ── Wrap in metadata envelope ────────────────────────────────────
    output_data = {
        "metadata": {
            "schema_name": schema_name,
            "dialect": dialect,
            "schema_source": schema_source,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "num_records": len(records),
            "num_per_complexity": args.num_per_complexity,
            "pipeline_step": "01_generate_sql_dataset",
        },
        "records": records,
    }

    # ── Save ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"Saved to {output_file}")


if __name__ == "__main__":
    main()
