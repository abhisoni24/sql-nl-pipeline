"""
Step 2: Generate natural language prompts from SQL queries.

Parses each SQL statement and renders a natural-language prompt using the
NL renderer. Supports two rendering modes:

  - **Legacy** (default): Direct ``render()`` producing final NL text using
    hardcoded synonym banks.
  - **Two-pass** (``--two-pass``): ``render_template()`` → ``TemplateResolver.resolve()``
    using a ``LinguisticDictionary`` built from a schema YAML, enabling
    schema-agnostic NL rendering.

Usage
-----
  # Schema-driven (recommended)
  python 02_generate_nl_prompts.py --schema schemas/bank.yaml --two-pass \\
      --dictionary schemas/bank_dictionary.yaml

  # With explicit I/O
  python 02_generate_nl_prompts.py -i dataset/bank/raw_queries.json \\
      -o dataset/bank/nl_prompts.json --schema schemas/bank.yaml --two-pass

  # Legacy mode (social_media defaults)
  python 02_generate_nl_prompts.py
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer


# ── Helpers ──────────────────────────────────────────────────────────────

def _load_records(path):
    """Load records from a JSON file, supporting both bare-list and metadata-wrapped formats."""
    with open(path) as f:
        data = json.load(f)
    if isinstance(data, dict) and "records" in data:
        return data["records"], data.get("metadata", {})
    # Legacy bare list
    return data, {}


def _resolve_dialect(schema_path=None, upstream_meta=None):
    """Determine the SQL dialect to use, with fallback chain."""
    if schema_path:
        from src.core.schema_loader import load_schema
        cfg = load_schema(schema_path)
        return cfg.dialect, cfg.schema_name
    if upstream_meta and upstream_meta.get("dialect"):
        return upstream_meta["dialect"], upstream_meta.get("schema_name", "unknown")
    # Fall back to legacy
    return "sqlite", "social_media"


def generate_nl_prompts(input_file, output_file, two_pass=False,
                        schema_path=None, dictionary_path=None):
    """Generate NL prompts with variations for all SQL queries in the dataset."""

    # ── Load input ───────────────────────────────────────────────────
    print(f"Loading {input_file}...")
    records, upstream_meta = _load_records(input_file)
    print(f"Loaded {len(records)} queries.")

    # ── Resolve dialect ──────────────────────────────────────────────
    dialect, schema_name = _resolve_dialect(schema_path, upstream_meta)
    print(f"Using dialect '{dialect}' for schema '{schema_name}'")

    # ── Load FK pairs for the renderer ───────────────────────────────
    fk_pairs = None
    if schema_path:
        from src.core.schema_loader import load_schema as _load_schema
        _cfg = _load_schema(schema_path)
        fk_pairs = _cfg.get_fk_pairs()

    # ── Initialize renderer ──────────────────────────────────────────
    renderer = SQLToNLRenderer(foreign_keys=fk_pairs)

    resolver = None
    if two_pass:
        from src.core.template_resolver import TemplateResolver

        if dictionary_path:
            from src.core.dictionary_builder import load_dictionary
            dictionary = load_dictionary(dictionary_path)
            resolver = TemplateResolver(dictionary, seed=42)
            print(f"Two-pass mode: loaded verified dictionary from {dictionary_path}")
        else:
            if not schema_path:
                schema_path = "schemas/social_media.yaml"
                print(f"No --schema provided, defaulting to {schema_path}")
            from src.core.schema_loader import load_schema
            from src.core.dictionary_builder import build_dictionary
            schema_cfg = load_schema(schema_path)
            dictionary = build_dictionary(schema_cfg, use_wordnet=True)
            resolver = TemplateResolver(dictionary, seed=42)
            print(f"WARNING: Building dictionary on-the-fly (synonyms not reviewed).")
            print(f"  Consider using: python generate_dictionary.py --schema {schema_path}")
            print(f"  Then pass the reviewed YAML via: --dictionary <path>")
            print(f"Two-pass mode: using schema '{schema_cfg.schema_name}' with dictionary resolver")

    # ── Process each query ───────────────────────────────────────────
    mode_label = "two-pass" if two_pass else "legacy"
    print(f"Generating natural language prompts ({mode_label} mode)...")
    success_count = 0
    error_count = 0

    for i, query_data in enumerate(records):
        sql = query_data["sql"]
        try:
            ast = parse_one(sql, dialect=dialect)
            renderer.config.seed = 42 + i

            if two_pass and resolver is not None:
                template = renderer.render_template(ast)
                resolver.rng.seed(42 + i)
                vanilla_prompt = resolver.resolve(template)
                query_data["ir_template"] = template
            else:
                vanilla_prompt = renderer.render(ast)

            query_data["nl_prompt"] = vanilla_prompt
            success_count += 1
        except Exception as e:
            print(f"Error processing query {i}: {sql[:50]}... - {e}")
            query_data["nl_prompt"] = "[Error: Could not generate NL prompt]"
            error_count += 1

    print(f"Successfully rendered {success_count} NL prompts, {error_count} errors.")

    # ── Build output with metadata ───────────────────────────────────
    output_data = {
        "metadata": {
            "schema_name": schema_name,
            "dialect": dialect,
            "schema_source": schema_path or "src/core/schema.py (legacy)",
            "dictionary_source": dictionary_path,
            "rendering_mode": mode_label,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "num_records": len(records),
            "pipeline_step": "02_generate_nl_prompts",
            "upstream": upstream_meta or None,
        },
        "records": records,
    }

    # ── Save ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"Saved to {output_file}")

    # ── Print samples ────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("Sample Natural Language Prompts:")
    print("=" * 80)
    for i in range(min(3, len(records))):
        print(f"\nQuery {i + 1}:")
        print(f"SQL: {records[i]['sql']}")
        print(f"NL:  {records[i]['nl_prompt']}")
        if "ir_template" in records[i]:
            print(f"IR:  {records[i]['ir_template']}")
    print("=" * 80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate NL prompts from SQL queries")
    parser.add_argument("--input", "-i", default=None,
                        help="Input raw SQL dataset JSON (default: dataset/<schema>/raw_queries.json)")
    parser.add_argument("--output", "-o", default=None,
                        help="Output NL-augmented dataset JSON (default: dataset/<schema>/nl_prompts.json)")
    parser.add_argument("--two-pass", action="store_true",
                        help="Use two-pass rendering (render_template + TemplateResolver)")
    parser.add_argument("--schema", "-s", default=None,
                        help="Path to schema YAML (used for dialect resolution and two-pass fallback)")
    parser.add_argument("--dictionary", "-d", default=None,
                        help="Path to a pre-verified dictionary YAML (use with --two-pass)")
    args = parser.parse_args()

    # ── Derive defaults from --schema if provided ────────────────────
    schema_name = "social_media"
    if args.schema:
        from src.core.schema_loader import load_schema
        schema_name = load_schema(args.schema).schema_name

    input_file = args.input or f"./dataset/{schema_name}/raw_queries.json"
    output_file = args.output or f"./dataset/{schema_name}/nl_prompts.json"

    generate_nl_prompts(
        input_file=input_file,
        output_file=output_file,
        two_pass=args.two_pass,
        schema_path=args.schema,
        dictionary_path=args.dictionary,
    )
