"""
Generate accuracy plots from tabulated experiment results.

Reads Table 2 (accuracy results CSV) produced by tabulate_results.py and
generates the following plots:

  Plot Set 1 — Accuracy by Perturbation Type per Complexity (line graphs)
    One plot per complexity type. X-axis = perturbation types, lines = LLMs.

  Plot 2 — Accuracy by Complexity Type (baseline nl_prompt only)
    Grouped bar chart. X-axis = complexity types, bars = LLMs.

  Plot 3 — Accuracy by Complexity Type (systematic perturbations only)
    Same layout, but aggregated across only the 13 perturbation types.

  Plot 4 — Accuracy by Complexity Type (baseline + perturbations combined)
    Same layout, all data combined.

  Plot 5a — DQL vs DML Accuracy (baseline only)
  Plot 5b — DQL vs DML Accuracy (baseline + perturbations)
    Grouped bar chart. X-axis = DQL/DML, bars = LLMs.

Usage
-----
  python plot_results.py --table2 <path_to_table2.csv>
  python plot_results.py  # auto-discovers from latest run
"""

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib
import numpy as np

matplotlib.rcParams.update({"font.size": 11})

# ── Constants ────────────────────────────────────────────────────────────

COMPLEXITY_COLUMNS = ["simple", "advanced", "join", "union",
                      "insert", "update", "delete"]

DQL_TYPES = {"simple", "advanced", "join", "union"}
DML_TYPES = {"insert", "update", "delete"}

BASELINE_LABEL = "nl_prompt"

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

# Short labels for perturbation types on x-axis
SHORT_LABELS = {
    "nl_prompt": "baseline",
    "anchored_pronoun_references": "pronoun_ref",
    "comment_annotations": "comments",
    "incomplete_join_spec": "inc_join",
    "mixed_sql_nl": "mixed_sql",
    "omit_obvious_operation_markers": "omit_ops",
    "operator_aggregate_variation": "op_agg_var",
    "phrasal_and_idiomatic_action_substitution": "phrasal",
    "punctuation_variation": "punct_var",
    "table_column_synonyms": "tbl_synonyms",
    "temporal_expression_variation": "temporal",
    "typos": "typos",
    "urgency_qualifiers": "urgency",
    "verbosity_variation": "verbosity",
}

# Distinct colors/markers for LLMs
_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
           "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
           "#393b79", "#637939"]
_MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*", "h", "p", "<", ">"]


# ── Helpers ──────────────────────────────────────────────────────────────

def find_latest_table2():
    """Auto-discover table2_accuracy_results.csv from the latest run."""
    runs_dir = Path(__file__).parent / "experiment_workspace" / "runs"
    if not runs_dir.exists():
        return None
    for rd in sorted(runs_dir.iterdir(), reverse=True):
        candidate = rd / "outputs" / "table2_accuracy_results.csv"
        if candidate.exists():
            return str(candidate)
    return None


def parse_tuple(s):
    """Parse '(R,C)' string into (passed, total) ints."""
    m = re.match(r"\((\d+),(\d+)\)", s.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    return 0, 0


def accuracy(passed, total):
    """Compute accuracy as a percentage, returning 0 if total is 0."""
    return (passed / total * 100) if total > 0 else 0.0


def load_table2(path):
    """Load table2 CSV into a list of dicts with parsed tuples."""
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def short_model(name):
    """Shorten model name for plot legends."""
    return name.split("/")[-1]


def _save(fig, outdir, name):
    path = os.path.join(outdir, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


# ── Plot Set 1: Accuracy by Perturbation Type per Complexity ─────────

def plot_accuracy_by_perturbation_per_complexity(rows, models, outdir):
    """One line graph per complexity type. X = perturbation types, lines = LLMs."""
    print("\nPlot Set 1: Accuracy by Perturbation Type per Complexity ...")

    for complexity in COMPLEXITY_COLUMNS:
        fig, ax = plt.subplots(figsize=(14, 6))

        for mi, model in enumerate(models):
            x_positions = []
            y_vals = []
            for pi, ptype in enumerate(PERTURBATION_ORDER):
                # Aggregate across all databases for this (model, ptype, complexity)
                total_p, total_c = 0, 0
                for r in rows:
                    if r["model_name"] == model and r["perturb_type"] == ptype:
                        p, c = parse_tuple(r[complexity])
                        total_p += p
                        total_c += c
                if total_c > 0:
                    x_positions.append(pi)
                    y_vals.append(accuracy(total_p, total_c))

            if y_vals:
                ax.plot(x_positions, y_vals,
                        marker=_MARKERS[mi % len(_MARKERS)],
                        color=_COLORS[mi % len(_COLORS)],
                        label=short_model(model), linewidth=1.5, markersize=6)

        ax.set_xticks(range(len(PERTURBATION_ORDER)))
        ax.set_xticklabels([SHORT_LABELS.get(p, p) for p in PERTURBATION_ORDER],
                           rotation=45, ha="right", fontsize=9)
        ax.set_ylabel("Accuracy (%)")
        ax.set_xlabel("Perturbation Type")
        ax.set_title(f"LLM Accuracy by Perturbation Type — {complexity.upper()} queries")
        ax.legend(fontsize=8, loc="best")
        ax.set_ylim(-5, 105)
        ax.grid(axis="y", alpha=0.3)

        _save(fig, outdir, f"plot1_perturbation_accuracy_{complexity}.png")


# ── Plot 2: Accuracy by Complexity (baseline only) ────────────────────

def _plot_accuracy_by_complexity(rows, models, outdir, filter_fn, title, filename):
    """Grouped bar chart: X = complexity types, bars = LLMs."""
    # Aggregate: acc[model][complexity] = (passed, total)
    acc = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for r in rows:
        if not filter_fn(r):
            continue
        for c in COMPLEXITY_COLUMNS:
            p, t = parse_tuple(r[c])
            acc[r["model_name"]][c][0] += p
            acc[r["model_name"]][c][1] += t

    fig, ax = plt.subplots(figsize=(12, 6))
    n_models = len(models)
    n_complex = len(COMPLEXITY_COLUMNS)
    bar_width = 0.8 / max(n_models, 1)
    x = np.arange(n_complex)

    for mi, model in enumerate(models):
        vals = []
        for c in COMPLEXITY_COLUMNS:
            p, t = acc[model][c]
            vals.append(accuracy(p, t))
        offset = (mi - n_models / 2 + 0.5) * bar_width
        ax.bar(x + offset, vals, bar_width,
               label=short_model(model), color=_COLORS[mi % len(_COLORS)])

    ax.set_xticks(x)
    ax.set_xticklabels([c.upper() for c in COMPLEXITY_COLUMNS])
    ax.set_ylabel("Accuracy (%)")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="best")
    ax.set_ylim(0, 105)
    ax.grid(axis="y", alpha=0.3)

    _save(fig, outdir, filename)


def plot_accuracy_by_complexity_baseline(rows, models, outdir):
    print("Plot 2: Accuracy by Complexity (baseline only) ...")
    _plot_accuracy_by_complexity(
        rows, models, outdir,
        filter_fn=lambda r: r["perturb_type"] == BASELINE_LABEL,
        title="LLM Accuracy by Complexity Type — Baseline (nl_prompt) Only",
        filename="plot2_complexity_baseline.png",
    )


def plot_accuracy_by_complexity_perturbations(rows, models, outdir):
    print("Plot 3: Accuracy by Complexity (perturbations only) ...")
    _plot_accuracy_by_complexity(
        rows, models, outdir,
        filter_fn=lambda r: r["perturb_type"] != BASELINE_LABEL,
        title="LLM Accuracy by Complexity Type — Systematic Perturbations Only",
        filename="plot3_complexity_perturbations.png",
    )


def plot_accuracy_by_complexity_all(rows, models, outdir):
    print("Plot 4: Accuracy by Complexity (all combined) ...")
    _plot_accuracy_by_complexity(
        rows, models, outdir,
        filter_fn=lambda r: True,
        title="LLM Accuracy by Complexity Type — Baseline + Perturbations",
        filename="plot4_complexity_all.png",
    )


# ── Plot 5: DQL vs DML ───────────────────────────────────────────────

def _plot_dql_dml(rows, models, outdir, filter_fn, title, filename):
    """Grouped bar chart: X = DQL/DML, bars = LLMs."""
    acc = defaultdict(lambda: {"DQL": [0, 0], "DML": [0, 0]})
    for r in rows:
        if not filter_fn(r):
            continue
        for c in COMPLEXITY_COLUMNS:
            p, t = parse_tuple(r[c])
            cat = "DQL" if c in DQL_TYPES else "DML"
            acc[r["model_name"]][cat][0] += p
            acc[r["model_name"]][cat][1] += t

    fig, ax = plt.subplots(figsize=(8, 6))
    categories = ["DQL", "DML"]
    n_models = len(models)
    bar_width = 0.8 / max(n_models, 1)
    x = np.arange(len(categories))

    for mi, model in enumerate(models):
        vals = [accuracy(*acc[model][cat]) for cat in categories]
        offset = (mi - n_models / 2 + 0.5) * bar_width
        ax.bar(x + offset, vals, bar_width,
               label=short_model(model), color=_COLORS[mi % len(_COLORS)])

    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_ylabel("Accuracy (%)")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="best")
    ax.set_ylim(0, 105)
    ax.grid(axis="y", alpha=0.3)

    _save(fig, outdir, filename)


def plot_dql_dml_baseline(rows, models, outdir):
    print("Plot 5a: DQL vs DML (baseline only) ...")
    _plot_dql_dml(
        rows, models, outdir,
        filter_fn=lambda r: r["perturb_type"] == BASELINE_LABEL,
        title="DQL vs DML Accuracy — Baseline Only",
        filename="plot5a_dql_dml_baseline.png",
    )


def plot_dql_dml_all(rows, models, outdir):
    print("Plot 5b: DQL vs DML (baseline + perturbations) ...")
    _plot_dql_dml(
        rows, models, outdir,
        filter_fn=lambda r: True,
        title="DQL vs DML Accuracy — Baseline + Perturbations",
        filename="plot5b_dql_dml_all.png",
    )


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate accuracy plots from tabulated results."
    )
    parser.add_argument(
        "--table2", "-t", default=None,
        help="Path to table2_accuracy_results.csv (auto-discovers latest if omitted)",
    )
    parser.add_argument(
        "--outdir", "-o", default=None,
        help="Output directory for plots (defaults to plots/ next to table2)",
    )
    args = parser.parse_args()

    table2_path = args.table2 or find_latest_table2()
    if not table2_path or not os.path.exists(table2_path):
        print("Error: Could not find table2_accuracy_results.csv")
        print("Provide the path with --table2 <path>")
        sys.exit(1)

    outdir = args.outdir or os.path.join(os.path.dirname(table2_path), "plots")
    os.makedirs(outdir, exist_ok=True)

    print(f"Loading: {table2_path}")
    rows = load_table2(table2_path)
    models = sorted(set(r["model_name"] for r in rows))
    print(f"Models ({len(models)}): {[short_model(m) for m in models]}")
    print(f"Output: {outdir}")

    # Generate all plots
    plot_accuracy_by_perturbation_per_complexity(rows, models, outdir)
    plot_accuracy_by_complexity_baseline(rows, models, outdir)
    plot_accuracy_by_complexity_perturbations(rows, models, outdir)
    plot_accuracy_by_complexity_all(rows, models, outdir)
    plot_dql_dml_baseline(rows, models, outdir)
    plot_dql_dml_all(rows, models, outdir)

    print(f"\nAll plots saved to: {outdir}")


if __name__ == "__main__":
    main()
