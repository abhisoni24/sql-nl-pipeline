"""
Test Suite: comment_annotations (Perturbation ID 7)
=====================================================
Validates the 'comment_annotations' perturbation across all 3,500 records.

Perturbation contract
---------------------
  Purpose  : Add SQL-style comment annotations (-- comment) or parenthetical
             notes after the NL prompt.
  Applicable: ALWAYS — all 3,500 records.
  Rules    :
    1. Append a comment/annotation AFTER the main prompt text.
    2. Comment starts with '--' (SQL comment style) OR is a parenthetical '(...)'.
    3. Comment text may include: urgency notes, context, purpose hints.
    4. The core prompt is UNCHANGED (comment is purely additive).
    5. Comment is separated by a period or space, at end of string.

Observed annotation patterns:
  '-- urgent request'
  '-- data needed asap'
  '(for analysis)'
  '-- please process quickly'

Checks (14 named checks)
--------------------------------------
APPLICABILITY
  1. always_applicable     – every record: applicable=True
  2. string_when_applicable – non-empty string
  3. no_object_repr         – no [None], Subquery(, etc.

ANNOTATION STRUCTURE
  4. has_annotation         – perturbed ends with or has a '--' or '(' annotation
  5. annotation_at_end      – the annotation (-- or parenthetical) appears near the end
  6. baseline_prefix_intact – the original baseline text appears as a prefix in perturbed
                              (comment is purely additive)
  7. annotation_non_empty   – the annotation text after '--' is non-empty

CONTENT PRESERVATION
  8.  columns_preserved         – column names from baseline in perturbed
  9.  table_still_present       – at least one schema table in perturbed
  10. condition_values_preserved – SQL string literals and numbers preserved
  11. ordering_cue_preserved     – ordering cue preserved if in baseline
  12. no_control_chars           – no raw tab/newline

COMPLEXITY-SPECIFIC
  13. join_relationship_preserved   – join: coupling or both tables present
  14. union_connector_preserved     – union: connector present
"""

import re
import sys
from pathlib import Path

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "comment_annotations"
DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"
UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}


def check_record(r, comp, result):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "always_applicable", "Perturbation entry missing"); return
    applicable = sp.get("applicable")
    if applicable is not True:
        result.fail(rid, comp, "always_applicable", f"Expected True, got {applicable!r}"); return
    result.ok("always_applicable")

    baseline_nl = baseline(r)
    base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")

    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "prompt missing or wrong type"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"Object repr '{pat}': {perturbed[:100]}"); return
    result.ok("no_object_repr")

    # 4. has_annotation — presence of '--' or '(..)'
    has_sql_comment = "--" in perturbed
    has_paren_note  = bool(re.search(r"\([^)]{3,}\)\s*$", perturbed))
    if not has_sql_comment and not has_paren_note:
        result.fail(rid, comp, "has_annotation", f"No '--' or '( )' annotation found: {perturbed[:120]}")
    else:
        result.ok("has_annotation")

    # 5. annotation_at_end — annotation in second half of string (for UNION: mid-string is ok)
    half = len(perturbed) // 2
    annotation_pos = perturbed.rfind("--") if "--" in perturbed else perturbed.rfind("(")
    if comp == "union":
        result.ok("annotation_at_end")  # Union: annotation can appear between clauses
    elif annotation_pos < half:
        result.fail(rid, comp, "annotation_at_end",
                    f"Annotation at pos {annotation_pos}/{len(perturbed)}: {perturbed[:120]}")
    else:
        result.ok("annotation_at_end")

    # 6. baseline_prefix_intact — for non-UNION: baseline is a strict prefix of perturbed
    # For UNION: annotation is inserted between clauses, so skip strict prefix check.
    bl_stripped = baseline_nl.rstrip(".").strip()
    if comp == "union":
        # Just verify the perturbed is longer than baseline (content was added)
        if len(perturbed) < len(baseline_nl):
            result.fail(rid, comp, "baseline_prefix_intact",
                        f"Perturbed shorter than baseline: {perturbed[:120]}")
        else:
            result.ok("baseline_prefix_intact")
    elif not perturbed.startswith(bl_stripped):
        result.fail(rid, comp, "baseline_prefix_intact",
                    f"Baseline not a prefix of perturbed: {perturbed[:120]}")
    else:
        result.ok("baseline_prefix_intact")

    # 7. annotation_non_empty
    m = re.search(r"--\s*(.+)", perturbed)
    if has_sql_comment and (not m or not m.group(1).strip()):
        result.fail(rid, comp, "annotation_non_empty", f"Empty annotation: {perturbed[:120]}")
    else:
        result.ok("annotation_non_empty")

    # 8. columns_preserved
    for col in known_columns():
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' missing: {perturbed[:120]}"); break
    else:
        result.ok("columns_preserved")

    # 9. table_still_present
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base and not any(table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present", f"No table found: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 10. condition_values_preserved
    lits = sql_literals(baseline_nl); nums = numbers(baseline_nl); ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved", f"Literal '{lit}' lost"); ok=False; break
    if ok:
        for num in nums:
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved", f"Number '{num}' lost"); break
        else:
            result.ok("condition_values_preserved")

    # 11. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost: {perturbed[:100]}")
        else:
            result.ok("ordering_cue_preserved")

    # 12. no_control_chars
    if re.search(r"[\t\n\r]", perturbed):
        result.fail(rid, comp, "no_control_chars", f"Control char: {repr(perturbed[:80])}")
    else:
        result.ok("no_control_chars")

    # 13. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent: {perturbed[:120]}")
        else:
            result.ok("join_relationship_preserved")

    # 14. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved", f"Connector absent: {perturbed[:120]}")
        else:
            result.ok("union_connector_preserved")


def main():
    import argparse
    p = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}' perturbations.")
    p.add_argument("--input", "-i", default=str(ROOT / DEFAULT_INPUT_FILE))
    add_common_args(p)
    p.add_argument("--verbose", "-v", action="store_true")
    a = p.parse_args()
    init_from_args(a)
    result = run_tests(a.input, PERTURBATION_NAME, check_record, verbose=a.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)


if __name__ == "__main__":
    main()
