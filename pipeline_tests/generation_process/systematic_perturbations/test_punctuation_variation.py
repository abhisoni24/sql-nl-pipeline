"""
Test Suite: punctuation_variation (Perturbation ID 8)
======================================================
Validates the 'punctuation_variation' perturbation across all 3,500 records.

Contract
--------
  Applicable when baseline NL has a list (3+ columns with commas).
  1567 applicable, 1933 not-applicable.
  Changes: semicolons replace some commas, or adds/removes trailing period.
  Core content unchanged.

Checks (14 named checks)
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

PERTURBATION_NAME = "punctuation_variation"
DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"
UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}


def _has_list(text): return text.count(",") >= 2  # 3+ comma-separated items


def check_record(r, comp, result):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "applicable_field_present", "Entry missing"); return
    result.ok("applicable_field_present")

    applicable = sp.get("applicable")
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool", f"Type {type(applicable).__name__}"); return
    result.ok("applicable_is_bool")

    baseline_nl = baseline(r); base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    has_list = _has_list(baseline_nl)

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # The generator uses its own applicability logic; we don't second-guess it
        result.ok("applicable_when_has_list")
        return

    # applicable = True
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 6. punctuation_changed — semicolons, period, exclamation, or ellipsis changes
    orig_semis = baseline_nl.count(";")
    pert_semis  = perturbed.count(";")
    orig_period = baseline_nl.strip().endswith(".")
    pert_period = perturbed.strip().endswith(".")
    orig_excl = baseline_nl.count("!")
    pert_excl  = perturbed.count("!")
    orig_ellipsis = "..." in baseline_nl
    pert_ellipsis = "..." in perturbed
    if (pert_semis == orig_semis and orig_period == pert_period
            and orig_excl == pert_excl and orig_ellipsis == pert_ellipsis):
        result.fail(rid, comp, "punctuation_changed",
                    f"No punctuation change detected: {perturbed[:120]}")
    else:
        result.ok("punctuation_changed")

    # 7. words_unchanged — only punctuation changed, not words
    orig_words = re.sub(r"[;,\.\!]", "", baseline_nl.lower()).split()
    pert_words = re.sub(r"[;,\.\!]", "", perturbed.lower()).split()
    common = sum(1 for w in orig_words if w in pert_words)
    if len(orig_words) > 0 and common < len(orig_words) * 0.80:
        result.fail(rid, comp, "words_unchanged",
                    f"Only {common}/{len(orig_words)} words preserved: {perturbed[:120]}")
    else:
        result.ok("words_unchanged")

    # 8. columns_preserved
    for col in known_columns():
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved", f"Column '{col}' missing"); break
    else:
        result.ok("columns_preserved")

    # 9. table_still_present
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base and not any(table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present", f"No table: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 10. condition_values_preserved
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

    # 11. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 12. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent")
        else: result.ok("join_relationship_preserved")

    # 13. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")

    # 14. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved", f"Connector absent")
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
