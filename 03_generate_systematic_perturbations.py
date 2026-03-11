"""
Step 3: Generate systematic prompt perturbations using deterministic rules.

Uses the modular perturbation registry (``src.perturbations.registry``) to
auto-discover all registered ``PerturbationStrategy`` subclasses.

Usage
-----
  # Schema-driven (recommended)
  python 03_generate_systematic_perturbations.py --schema schemas/bank.yaml

  # With explicit I/O
  python 03_generate_systematic_perturbations.py \\
      -i dataset/bank/nl_prompts.json \\
      -o dataset/bank/systematic_perturbations.json \\
      --schema schemas/bank.yaml

  # Legacy (social_media defaults)
  python 03_generate_systematic_perturbations.py
"""

import json
import sys
import os
import random
from datetime import datetime, timezone
from typing import Dict, Any, List

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sqlglot import parse_one
from src.perturbations.registry import all_strategies


# ── Helpers ──────────────────────────────────────────────────────────────

def _load_records(path):
    """Load records from a JSON file, supporting both bare-list and metadata-wrapped formats."""
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "records" in data:
        return data["records"], data.get("metadata", {})
    return data, {}


def _resolve_dialect(schema_path=None, upstream_meta=None):
    """Determine the SQL dialect to use, with fallback chain."""
    if schema_path:
        from src.core.schema_loader import load_schema
        cfg = load_schema(schema_path)
        return cfg.dialect, cfg.schema_name
    if upstream_meta and upstream_meta.get("dialect"):
        return upstream_meta["dialect"], upstream_meta.get("schema_name", "unknown")
    return "sqlite", "unknown"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate systematic perturbations.")
    parser.add_argument("--input", "-i", default=None,
                        help="Path to the NL query JSON file (default: dataset/<schema>/nl_prompts.json)")
    parser.add_argument("--output", "-o", default=None,
                        help="Path for the output perturbation JSON file (default: dataset/<schema>/systematic_perturbations.json)")
    parser.add_argument("--schema", "-s", default=None,
                        help="Path to a YAML schema file (provides dialect and schema name)")
    args = parser.parse_args()

    # ── Resolve schema info ──────────────────────────────────────────
    schema_name = "social_media"
    dialect = "sqlite"
    cfg = None
    dictionary = None
    if args.schema:
        from src.core.schema_loader import load_schema
        cfg = load_schema(args.schema)
        schema_name = cfg.schema_name
        dialect = cfg.dialect
        print(f"Schema: '{schema_name}', dialect: '{dialect}'")

        # Auto-load dictionary if a matching _dictionary.yaml exists
        dict_path = args.schema.replace('.yaml', '_dictionary.yaml')
        if os.path.exists(dict_path):
            from src.core.dictionary_builder import load_dictionary
            dictionary = load_dictionary(dict_path)
            syn_count = len(dictionary.table_synonyms) + len(dictionary.column_synonyms)
            print(f"Dictionary: loaded {syn_count} synonym entries from {dict_path}")

    INPUT_FILE = args.input or f"./dataset/{schema_name}/nl_prompts.json"
    OUTPUT_FILE = args.output or f"./dataset/{schema_name}/systematic_perturbations.json"

    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    records, upstream_meta = _load_records(INPUT_FILE)

    # If no --schema, try to get dialect from upstream metadata
    if not args.schema and upstream_meta:
        dialect = upstream_meta.get("dialect", dialect)
        schema_name = upstream_meta.get("schema_name", schema_name)

    strategies = all_strategies()
    output_records = []

    print(f"Processing {len(records)} queries for systematic perturbations...")
    print(f"Registered strategies ({len(strategies)}): {sorted(strategies.keys())}")

    for i, query_item in enumerate(records):
        sql = query_item["sql"]
        baseline_nl = query_item.get("nl_prompt", "")

        output_item = {
            "id": query_item.get("id", i + 1),
            "sql": sql,
            "generated_perturbations": {
                "original": {"nl_prompt": baseline_nl},
                "single_perturbations": [],
                "metadata": {},
            },
        }

        try:
            ast = parse_one(sql, dialect=dialect)
        except Exception:
            continue

        context = {"seed": 42 + i}
        if cfg is not None:
            context["schema_config"] = cfg
        if dictionary is not None:
            context["dictionary"] = dictionary
        rng = random.Random(42 + i)

        applicable_count = 0
        for name, strategy in strategies.items():
            is_app = strategy.is_applicable(ast, baseline_nl, context)
            entry = {"perturbation_name": name, "applicable": is_app, "perturbed_nl_prompt": None}

            if is_app:
                try:
                    perturbed = strategy.apply(baseline_nl, ast, rng, context)

                    # FINAL DIFF CHECK: Only mark truly applicable if it actually changed the string
                    if perturbed == baseline_nl:
                        entry["applicable"] = False
                        entry["perturbed_nl_prompt"] = None
                    else:
                        entry["perturbed_nl_prompt"] = perturbed
                        entry["changes_made"] = strategy.description
                        applicable_count += 1

                        # Post-validation: did the specific perturbation effect fire?
                        applied, detail = strategy.was_applied(baseline_nl, perturbed, context)
                        entry["was_applied"] = applied
                        if detail:
                            entry["was_applied_detail"] = detail
                except Exception:
                    entry["applicable"] = False

            output_item["generated_perturbations"]["single_perturbations"].append(entry)

        output_item["generated_perturbations"]["metadata"]["total_applicable"] = applicable_count
        output_records.append(output_item)

        if (i + 1) % 50 == 0:
            print(f"Progress: {i + 1} queries...")

    # ── Wrap in metadata envelope ────────────────────────────────────
    output_data = {
        "metadata": {
            "schema_name": schema_name,
            "dialect": dialect,
            "schema_source": args.schema or upstream_meta.get("schema_source", "src/core/schema.py (legacy)"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "num_records": len(output_records),
            "num_strategies": len(strategies),
            "strategies": sorted(strategies.keys()),
            "pipeline_step": "03_generate_systematic_perturbations",
            "upstream": upstream_meta or None,
        },
        "records": output_records,
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"Dataset generated at {OUTPUT_FILE}")


if __name__ == "__main__":
    main()