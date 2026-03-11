"""
Step 1: Generate SQL dataset.

Reads a schema definition (YAML or SQLite) and produces raw SQL queries
across all complexity types (simple, join, advanced, union, insert, update, delete).

Usage
-----
  python 01_generate_sql_dataset.py --schema schemas/social_media.yaml

  # With custom output path and query count
  python 01_generate_sql_dataset.py --schema schemas/bank.yaml -n 100 -o dataset/bank/raw_queries.json
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
        "--schema", "-s", required=True,
        help="Path to a schema file — YAML (e.g. schemas/social_media.yaml) or "
             "SQLite (e.g. database.sqlite)."
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
    from src.core.schema_loader import load_schema
    schema_cfg = load_schema(args.schema)
    schema_name = schema_cfg.schema_name
    dialect = schema_cfg.dialect
    schema_source = args.schema
    print(f"Loaded schema '{schema_name}' from {args.schema}")

    # ── Determine output path ────────────────────────────────────────
    if args.output:
        output_file = args.output
    else:
        output_file = f"./dataset/{schema_name}/raw_queries.json"

    # ── Generate queries ─────────────────────────────────────────────
    generator = SQLQueryGenerator(schema_cfg)
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
