"""
Test Suite: urgency_qualifiers (Perturbation ID 9)
===================================================
Validates the 'urgency_qualifiers' perturbation across all 3,500 records.

Contract
--------
  Always applicable. Prepend or append an urgency/priority qualifier.
  Examples: 'URGENT:', 'High priority:', 'ASAP:', 'At your convenience,'
  Core content unchanged.

Checks (12 named checks)
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

PERTURBATION_NAME = "urgency_qualifiers"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"
UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}

# Urgency qualifier words/phrases
URGENCY_WORDS = {
    "urgent", "asap", "priority", "immediately", "emergency",
    "critical", "high", "important", "quick", "now", "please",
    "convenience", "whenever", "rush",
    "when you can", "low", "no rush",
}

def check_record(r, comp, result):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "always_applicable", "Entry missing"); return
    if sp.get("applicable") is not True:
        result.fail(rid, comp, "always_applicable", f"Expected True, got {sp.get('applicable')!r}"); return
    result.ok("always_applicable")

    baseline_nl = baseline(r); base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 3. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); return
    result.ok("no_object_repr")

    # 4. has_urgency_qualifier
    if not any(w in pert_l for w in URGENCY_WORDS):
        result.fail(rid, comp, "has_urgency_qualifier", f"No urgency word: {perturbed[:120]}")
    else:
        result.ok("has_urgency_qualifier")

    # 5. urgency_is_additive — perturbed longer than original
    if len(perturbed.split()) <= len(baseline_nl.split()):
        result.fail(rid, comp, "urgency_is_additive",
                    f"Perturbed not longer than original: {len(perturbed.split())} vs {len(baseline_nl.split())}")
    else:
        result.ok("urgency_is_additive")

    # 6. core_content_unchanged — baseline text appears somewhere in perturbed
    # For UNION: urgency may be prepended only to first clause; check first clause presence
    bl_stripped = baseline_nl.strip().rstrip(".")
    if comp == "union":
        # Check that the urgency prefix + at least the first clause are present
        # (the second clause follows after "combined with")
        combined_idx = bl_stripped.lower().find("combined with")
        first_clause = bl_stripped[:combined_idx].strip() if combined_idx > 0 else bl_stripped[:80]
        if first_clause not in perturbed:
            result.fail(rid, comp, "core_content_unchanged",
                        f"First union clause not in perturbed: {perturbed[:120]}")
        else:
            result.ok("core_content_unchanged")
    elif bl_stripped not in perturbed:
        result.fail(rid, comp, "core_content_unchanged",
                    f"Baseline not found in perturbed: {perturbed[:120]}")
    else:
        result.ok("core_content_unchanged")

    # 7. columns_preserved
    for col in known_columns():
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved", f"Column '{col}' missing"); break
    else:
        result.ok("columns_preserved")

    # 8. table_still_present
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base and not any(table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present", f"No table found: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 9. condition_values_preserved
    lits = sql_literals(baseline_nl); ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved", f"Literal '{lit}' lost"); ok=False; break
    if ok:
        for num in numbers(baseline_nl):
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved", f"Number '{num}' lost"); break
        else:
            result.ok("condition_values_preserved")

    # 10. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost: {perturbed[:100]}")
        else: result.ok("ordering_cue_preserved")

    # 11. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent: {perturbed[:120]}")
        else: result.ok("join_relationship_preserved")

    # 12. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved", f"Connector absent: {perturbed[:120]}")
        else: result.ok("union_connector_preserved")


def main():
    import argparse
    p = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}'.")
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
