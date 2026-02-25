#!/usr/bin/env python3
"""
Generate a failure analysis report from evaluated experiment results.

For each section (baseline vanilla, then each perturbation type), this script
picks 2 failure examples per model per complexity type and writes a formatted
markdown report.
"""

import json
import sys
from collections import defaultdict
from pathlib import Path

INPUT_FILE = "/Users/obby/Downloads/newOutputs/evaluated_results_aggregated.jsonl"
OUTPUT_DIR = Path("/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run")

# ─── Load data ────────────────────────────────────────────────────────────────
records = []
with open(INPUT_FILE) as f:
    for line in f:
        records.append(json.loads(line))

print(f"Loaded {len(records)} records")

# ─── Collect failures ─────────────────────────────────────────────────────────
failures = [r for r in records if not r.get("is_equivalent", True)]
print(f"Total failures: {len(failures)}")

# ─── Identify dimensions ──────────────────────────────────────────────────────
models = sorted({r["model_name"] for r in records})
# Complexity order
COMPLEXITY_ORDER = ["simple", "join", "union", "advanced", "insert", "update", "delete", "unknown"]
complexities_in_data = {r["complexity"] for r in records}
complexities = [c for c in COMPLEXITY_ORDER if c in complexities_in_data]

# Perturbation types (excluding 'original' which is baseline)
perturbation_types = sorted({r["perturbation_type"] for r in records if r["perturbation_type"] != "original"})

# ─── Helper: format one failure example ───────────────────────────────────────
def format_example(r, idx):
    lines = []
    lines.append(f"**Example {idx}** (query_id: {r.get('query_id', 'N/A')})")
    lines.append("")
    lines.append(f"| Field | Value |")
    lines.append(f"|-------|-------|")
    lines.append(f"| **Model** | `{r['model_name']}` |")
    lines.append(f"| **Complexity** | `{r.get('complexity', 'N/A')}` |")
    lines.append(f"| **Perturbation Type** | `{r.get('perturbation_type', 'N/A')}` |")
    lines.append(f"| **Perturbation Source** | `{r.get('perturbation_source', 'N/A')}` |")
    lines.append("")
    lines.append(f"**NL Prompt:**")
    lines.append(f"> {r.get('input_prompt', 'N/A')}")
    lines.append("")
    lines.append(f"**Gold SQL:**")
    lines.append(f"```sql\n{r.get('gold_sql', 'N/A')}\n```")
    lines.append("")
    lines.append(f"**Generated SQL:**")
    lines.append(f"```sql\n{r.get('generated_sql', 'N/A')}\n```")
    lines.append("")
    lines.append(f"**Equivalence Details:** {r.get('equivalence_details', 'N/A')}")
    lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


# ─── Helper: pick 2 failures per model per complexity ─────────────────────────
def pick_failures(pool):
    """Given a list of failure records, return dict[(model, complexity)] -> list of up to 2 records."""
    grouped = defaultdict(list)
    for r in pool:
        key = (r["model_name"], r.get("complexity", "unknown"))
        grouped[key].append(r)
    result = {}
    for key, recs in grouped.items():
        result[key] = recs[:2]
    return result


# ─── Build the report ─────────────────────────────────────────────────────────
report_lines = []
report_lines.append("# Failure Analysis Report")
report_lines.append("")
report_lines.append(f"**Generated from:** `{INPUT_FILE}`  ")
report_lines.append(f"**Total records:** {len(records)}  ")
report_lines.append(f"**Total failures:** {len(failures)}  ")
report_lines.append(f"**Models:** {len(models)}  ")
report_lines.append(f"**Complexity types:** {', '.join(complexities)}  ")
report_lines.append("")
report_lines.append("---")
report_lines.append("")

# ═══════════════════════════════════════════════════════════════════════════════
# PART 1: BASELINE VANILLA (perturbation_type == 'original')
# ═══════════════════════════════════════════════════════════════════════════════
report_lines.append("# Part 1: Baseline Vanilla Prompts (`original`)")
report_lines.append("")
report_lines.append("These are failures on the unperturbed, baseline natural language prompts.")
report_lines.append("")

baseline_failures = [r for r in failures if r.get("perturbation_type") == "original"]
report_lines.append(f"**Baseline failures:** {len(baseline_failures)}")
report_lines.append("")

if not baseline_failures:
    report_lines.append("_No failures found for baseline vanilla prompts._")
    report_lines.append("")
else:
    selected = pick_failures(baseline_failures)
    for model in models:
        short_model = model.split("/")[-1]
        report_lines.append(f"## Model: `{short_model}`")
        report_lines.append("")
        model_has_examples = False
        for complexity in complexities:
            key = (model, complexity)
            examples = selected.get(key, [])
            if not examples:
                continue
            model_has_examples = True
            report_lines.append(f"### Complexity: `{complexity}`")
            report_lines.append("")
            for i, ex in enumerate(examples, 1):
                report_lines.append(format_example(ex, i))
        if not model_has_examples:
            report_lines.append("_No failures for this model on baseline prompts._")
            report_lines.append("")

report_lines.append("")
report_lines.append("---")
report_lines.append("")

# ═══════════════════════════════════════════════════════════════════════════════
# PART 2: PER PERTURBATION TYPE
# ═══════════════════════════════════════════════════════════════════════════════
report_lines.append("# Part 2: Failures by Perturbation Type")
report_lines.append("")
report_lines.append("For each perturbation type, 2 failure examples per model per complexity type are shown.")
report_lines.append("")

for pt in perturbation_types:
    pt_failures = [r for r in failures if r.get("perturbation_type") == pt]
    report_lines.append(f"## Perturbation: `{pt}`")
    report_lines.append("")
    report_lines.append(f"**Failures in this perturbation:** {len(pt_failures)}")
    report_lines.append("")

    if not pt_failures:
        report_lines.append("_No failures found for this perturbation type._")
        report_lines.append("")
        continue

    selected = pick_failures(pt_failures)
    for model in models:
        short_model = model.split("/")[-1]
        report_lines.append(f"### Model: `{short_model}`")
        report_lines.append("")
        model_has_examples = False
        for complexity in complexities:
            key = (model, complexity)
            examples = selected.get(key, [])
            if not examples:
                continue
            model_has_examples = True
            report_lines.append(f"#### Complexity: `{complexity}`")
            report_lines.append("")
            for i, ex in enumerate(examples, 1):
                report_lines.append(format_example(ex, i))
        if not model_has_examples:
            report_lines.append(f"_No failures for `{short_model}` on `{pt}` prompts._")
            report_lines.append("")

    report_lines.append("")
    report_lines.append("---")
    report_lines.append("")

# ─── Write the report ─────────────────────────────────────────────────────────
output_file = OUTPUT_DIR / "failure_analysis_report.md"
with open(output_file, "w") as f:
    f.write("\n".join(report_lines))

print(f"\nReport written to: {output_file}")

# Print a quick summary of how many examples were included
total_examples = 0
for pt_label in ["original"] + perturbation_types:
    pool = [r for r in failures if r.get("perturbation_type") == pt_label]
    selected = pick_failures(pool)
    count = sum(len(v) for v in selected.values())
    total_examples += count
    print(f"  {pt_label:45} -> {count:4} examples selected")

print(f"\nTotal failure examples in report: {total_examples}")
