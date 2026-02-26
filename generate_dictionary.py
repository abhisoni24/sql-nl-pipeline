"""
Dictionary Generator — Stage 1 of the two-pass NL pipeline.

Generates a LinguisticDictionary from a schema YAML, saves it as a YAML file
for human review, and optionally prints a summary.

Usage:
    # Generate for one schema
    python generate_dictionary.py --schema schemas/social_media.yaml

    # Generate for all three schemas
    python generate_dictionary.py --schema schemas/social_media.yaml schemas/bank.yaml schemas/hospital.yaml

    # Custom output directory
    python generate_dictionary.py --schema schemas/bank.yaml --outdir dictionaries/

The saved YAML contains table_synonyms, column_synonyms, and table_categories.
Review and edit the file (remove bad WordNet suggestions, add domain terms),
then pass it to 02_generate_nl_prompts.py via --dictionary.
"""

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.core.schema_loader import load_from_yaml
from src.core.dictionary_builder import build_dictionary, save_dictionary


def generate_and_save(schema_path: str, outdir: str) -> str:
    """Build a dictionary from a schema YAML and save it for review.

    Returns the path of the saved dictionary YAML.
    """
    schema_cfg = load_from_yaml(schema_path)
    dictionary = build_dictionary(schema_cfg, use_wordnet=True)

    os.makedirs(outdir, exist_ok=True)
    dict_filename = f"{schema_cfg.schema_name}_dictionary.yaml"
    dict_path = os.path.join(outdir, dict_filename)
    save_dictionary(dictionary, dict_path)

    # Print summary
    n_tables = len(dictionary.table_synonyms)
    n_columns = len(dictionary.column_synonyms)
    total_table_syns = sum(len(v) for v in dictionary.table_synonyms.values())
    total_col_syns = sum(len(v) for v in dictionary.column_synonyms.values())

    print(f"\n{'='*60}")
    print(f"Schema:     {schema_cfg.schema_name}")
    print(f"Saved to:   {dict_path}")
    print(f"Tables:     {n_tables} (total synonyms: {total_table_syns})")
    print(f"Columns:    {n_columns} (total synonyms: {total_col_syns})")
    print(f"{'='*60}")

    # Show table synonyms
    print("\nTable synonyms:")
    for tname, syns in sorted(dictionary.table_synonyms.items()):
        print(f"  {tname}: {syns}")

    # Show column synonyms
    print("\nColumn synonyms:")
    for cname, syns in sorted(dictionary.column_synonyms.items()):
        print(f"  {cname}: {syns}")

    # Show categories
    print("\nTable categories:")
    for tname, cat in sorted(dictionary.table_categories.items()):
        print(f"  {tname}: {cat}")

    print()
    return dict_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate synonym dictionaries from schema YAMLs for review"
    )
    parser.add_argument(
        "--schema", "-s", nargs="+", required=True,
        help="Path(s) to schema YAML file(s)"
    )
    parser.add_argument(
        "--outdir", "-o", default="schemas",
        help="Directory to save dictionary YAMLs (default: schemas/)"
    )
    args = parser.parse_args()

    saved_paths = []
    for schema_path in args.schema:
        if not os.path.isfile(schema_path):
            print(f"ERROR: Schema file not found: {schema_path}", file=sys.stderr)
            continue
        path = generate_and_save(schema_path, args.outdir)
        saved_paths.append(path)

    if saved_paths:
        print(f"\n{'='*60}")
        print("Review the generated dictionary YAML files:")
        for p in saved_paths:
            print(f"  {p}")
        print("\nEdit any bad synonyms, then use with 02_generate_nl_prompts.py:")
        print("  python 02_generate_nl_prompts.py --two-pass --dictionary <path>")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()
