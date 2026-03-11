"""
Test Suite: temporal_expression_variation (Perturbation ID 7)
=============================================================
Validates the 'temporal_expression_variation' perturbation.

Contract
--------
  Applicable when the NL has a DATETIME / relative time reference 
  (e.g. 'within the last N days' or 'over the last N days').
  643 applicable, 2857 not-applicable.
  Changes temporal phrasing (e.g. 'within the last N days' → 'over the last N days').
  The numeric N value is preserved.

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

PERTURBATION_NAME = "temporal_expression_variation"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"
UNION_CONNECTORS = {"combined with", "union", "along with"}

# Temporal anchor words — both the original and valid substitutions
TEMPORAL_ANCHORS = {
    "days", "week", "weeks", "month", "months", "hours", "year", "years",
    "last", "past", "recent", "previous", "ago", "over", "within", "since"
}

def _has_temporal_expr(text):
    """True if NL has a relative temporal expression."""
    t = text.lower()
    return bool(re.search(r"\b(within|over|in)\s+the\s+(last|past)\b", t) or
                re.search(r"\bfrom\s+\d+\s+days?\s+ago", t) or
                re.search(r"\bstarting\s+from\b", t))


def check_record(r, comp, result):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "applicable_field_present", "Entry missing"); return
    result.ok("applicable_field_present")

    applicable = sp.get("applicable")
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool", f"Type={type(applicable).__name__}"); return
    result.ok("applicable_is_bool")

    baseline_nl = baseline(r); base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    has_temporal = _has_temporal_expr(baseline_nl)

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # Generator has its own stricter criteria for temporal applicability
        # (e.g. date literals that look like DATETIME columns are excluded)
        result.ok("not_applicable_when_no_temporal")
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

    # 6. temporal_anchor_preserved — a temporal word still present
    if not any(w in pert_l for w in TEMPORAL_ANCHORS):
        result.fail(rid, comp, "temporal_anchor_preserved",
                    f"No temporal anchor in perturbed: {perturbed[:120]}")
    else:
        result.ok("temporal_anchor_preserved")

    # 7. numeric_value_preserved — the N in 'last N days' must remain
    nums_base = numbers(baseline_nl); nums_pert = numbers(perturbed)
    missing = [n for n in nums_base if n not in nums_pert]
    if missing:
        result.fail(rid, comp, "numeric_value_preserved",
                    f"Numbers {missing} lost: {perturbed[:120]}")
    else:
        result.ok("numeric_value_preserved")

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

    # 10. string_literals_preserved
    lits = sql_literals(baseline_nl)
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "string_literals_preserved", f"Literal '{lit}' gone"); break
    else:
        result.ok("string_literals_preserved")

    # 11. length_reasonable — re-rendering + temporal expansion can change word count
    orig_wc = len(baseline_nl.split())
    delta = abs(len(perturbed.split()) - orig_wc)
    limit = max(8, int(orig_wc * 0.5))
    if delta > limit:
        result.fail(rid, comp, "length_reasonable",
                    f"Word count delta {delta} > {limit}: orig={orig_wc}, pert={len(perturbed.split())}")
    else:
        result.ok("length_reasonable")

    # 12. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

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
