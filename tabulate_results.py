"""
Generate tabulated results from evaluated experiment outputs.

Produces two CSV tables from evaluated_results_aggregated.jsonl:

  Table 1 – Dataset Dimensions
    Counts per (database, perturbation_type, complexity_type).
    Columns: Database, perturb_type, simple, advanced, join, union,
             insert, update, delete, total

  Table 2 – Accuracy Results
    Pass/total tuples per (database, perturbation_type, complexity_type, model).
    Columns: Database, perturb_type, simple, advanced, join, union,
             insert, update, delete, total, model_name

Only baseline (nl_prompt) and systematic perturbation records are included.
LLM-generated perturbations are excluded.

Usage
-----
  python tabulate_results.py --results <path_to_aggregated.jsonl>
  python tabulate_results.py  # auto-discovers latest run
"""

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

# ── Constants ────────────────────────────────────────────────────────────

COMPLEXITY_COLUMNS = ["simple", "advanced", "join", "union",
                      "insert", "update", "delete"]

# Row label for baseline original prompts
BASELINE_LABEL = "nl_prompt"

# Ordered perturbation types for consistent row ordering
PERTURBATION_ORDER = [
    BASELINE_LABEL,
    "anchored_pronoun_references",
    "comment_annotations",
    "incomplete_join_spec",
    "mixed_sql_nl",
    "omit_obvious_operation_markers",
    "operator_aggregate_variation",
    "phrasal_and_idiomatic_action_substitution",
    "punctuation_variation",
    "table_column_synonyms",
    "temporal_expression_variation",
    "typos",
    "urgency_qualifiers",
    "verbosity_variation",
]


# ── Helpers ──────────────────────────────────────────────────────────────

def find_latest_results():
    """Auto-discover the latest evaluated_results_aggregated.jsonl."""
    runs_dir = Path(__file__).parent / "experiment_workspace" / "runs"
    if not runs_dir.exists():
        return None
    run_dirs = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir()],
        reverse=True,
    )
    for rd in run_dirs:
        candidate = rd / "outputs" / "evaluated_results_aggregated.jsonl"
        if candidate.exists():
            return str(candidate)
    return None


def load_records(path):
    """Load JSONL records, filtering to baseline + systematic only."""
    records = []
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if r["perturbation_source"] in ("baseline", "systematic"):
                # Normalise: baseline original → nl_prompt label,
                # systematic original is a duplicate of baseline → skip
                if r["perturbation_source"] == "systematic" and r["perturbation_type"] == "original":
                    continue
                records.append(r)
    return records


def perturb_label(record):
    """Map a record to its perturb_type row label."""
    if record["perturbation_source"] == "baseline":
        return BASELINE_LABEL
    return record["perturbation_type"]


# ── Table 1: Dataset Dimensions ─────────────────────────────────────────

def build_table1(records):
    """Count records per (database, perturb_type, complexity)."""
    # counts[(db, ptype)][complexity] = count
    counts = defaultdict(lambda: defaultdict(int))
    for r in records:
        key = (r["schema_name"], perturb_label(r))
        counts[key][r["complexity"]] += 1

    databases = sorted({r["schema_name"] for r in records})

    rows = []
    for db in databases:
        for ptype in PERTURBATION_ORDER:
            key = (db, ptype)
            row = {"Database": db, "perturb_type": ptype}
            total = 0
            for c in COMPLEXITY_COLUMNS:
                val = counts[key].get(c, 0)
                row[c] = val
                total += val
            row["total"] = total
            rows.append(row)

    return rows


# ── Table 2: Accuracy Results ───────────────────────────────────────────

def build_table2(records):
    """Compute (passed, total) per (database, perturb_type, complexity, model)."""
    # acc[(db, ptype, model)][complexity] = [passed, total]
    acc = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for r in records:
        key = (r["schema_name"], perturb_label(r), r["model_name"])
        bucket = acc[key][r["complexity"]]
        bucket[1] += 1
        if r.get("is_equivalent"):
            bucket[0] += 1

    databases = sorted({r["schema_name"] for r in records})
    models = sorted({r["model_name"] for r in records})

    rows = []
    for db in databases:
        for model in models:
            for ptype in PERTURBATION_ORDER:
                key = (db, ptype, model)
                row = {"Database": db, "perturb_type": ptype}
                total_passed, total_count = 0, 0
                for c in COMPLEXITY_COLUMNS:
                    p, t = acc[key].get(c, [0, 0])
                    row[c] = f"({p},{t})"
                    total_passed += p
                    total_count += t
                row["total"] = f"({total_passed},{total_count})"
                row["model_name"] = model
                rows.append(row)

    return rows


# ── Write CSV ────────────────────────────────────────────────────────────

def write_csv(rows, path, columns):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {path}  ({len(rows)} rows)")


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Tabulate evaluated experiment results into CSV tables."
    )
    parser.add_argument(
        "--results", "-r", default=None,
        help="Path to evaluated_results_aggregated.jsonl (auto-discovers latest if omitted)",
    )
    parser.add_argument(
        "--outdir", "-o", default=None,
        help="Output directory for CSVs (defaults to same dir as results file)",
    )
    args = parser.parse_args()

    results_path = args.results or find_latest_results()
    if not results_path or not os.path.exists(results_path):
        print("Error: Could not find evaluated_results_aggregated.jsonl")
        print("Provide the path with --results <path>")
        sys.exit(1)

    outdir = args.outdir or os.path.dirname(results_path)
    os.makedirs(outdir, exist_ok=True)

    print(f"Loading results from: {results_path}")
    records = load_records(results_path)
    print(f"Loaded {len(records)} records (baseline + systematic, excluding systematic original)")

    # Table 1
    print("\nBuilding Table 1: Dataset Dimensions ...")
    t1_rows = build_table1(records)
    t1_cols = ["Database", "perturb_type"] + COMPLEXITY_COLUMNS + ["total"]
    t1_path = os.path.join(outdir, "table1_dataset_dimensions.csv")
    write_csv(t1_rows, t1_path, t1_cols)

    # Table 2
    print("Building Table 2: Accuracy Results ...")
    t2_rows = build_table2(records)
    t2_cols = ["Database", "perturb_type"] + COMPLEXITY_COLUMNS + ["total", "model_name"]
    t2_path = os.path.join(outdir, "table2_accuracy_results.csv")
    write_csv(t2_rows, t2_path, t2_cols)

    print("\nDone.")


if __name__ == "__main__":
    main()
