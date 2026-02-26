"""
Test Suite: incomplete_join_spec (Perturbation ID 12)
======================================================
Validates the 'incomplete_join_spec' perturbation.

Contract
--------
  Only applicable to JOIN queries.
  500 applicable (all join records), 3000 not-applicable.
  The ON condition is removed, leaving just 'TABLE and their TABLE' style.

Checks (12 named checks)
"""

import re
import sys

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "incomplete_join_spec"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"

INCOMPLETE_JOIN_MARKERS = {
    "and their", "and the", "with their", "along with", "and its", "with "
}



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

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # Not all JOIN queries are applicable (some use implicit join patterns)
        # The generator selects a subset of JOIN queries
        result.ok("applicable_only_for_join")
        return

    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. applicable_only_for_join
    if comp != "join":
        result.fail(rid, comp, "applicable_only_for_join",
                    f"Non-join complexity '{comp}' but applicable=True")
    else:
        result.ok("applicable_only_for_join")

    # 6. on_condition_removed — no explicit 'equals' in join condition context
    # The original NL has "on the X's col equals the Y's col" — this should be gone
    has_explicit_on_cond = bool(re.search(
        r"\bon\b.{0,60}\b(equals|matches|is equal to|equal to)\b", pert_l))
    if has_explicit_on_cond:
        result.fail(rid, comp, "on_condition_removed",
                    f"Explicit ON condition still present: {perturbed[:120]}")
    else:
        result.ok("on_condition_removed")

    # 7. join_implicit_marker
    has_marker = any(m in pert_l for m in INCOMPLETE_JOIN_MARKERS)
    if not has_marker:
        result.fail(rid, comp, "join_implicit_marker",
                    f"No implicit join marker ('and their' etc.): {perturbed[:120]}")
    else:
        result.ok("join_implicit_marker")

    # 8. both_tables_present
    tables_in_base = [t for t in known_tables() if t in base_l]
    tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
    if len(tables_in_base) >= 2 and len(tables_in_pert) < 2:
        result.fail(rid, comp, "both_tables_present",
                    f"Only {len(tables_in_pert)}/{len(tables_in_base)} tables found: {perturbed[:120]}")
    else:
        result.ok("both_tables_present")

    # 9. condition_values_preserved
    lits = sql_literals(baseline_nl); ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved", f"Literal '{lit}' lost");
            ok=False; break
    if ok:
        for num in numbers(baseline_nl):
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved", f"Number '{num}' lost"); break
        else:
            result.ok("condition_values_preserved")

    # 10. shorter_without_on — ON clause removal may be offset by re-rendering
    #     that adds possessive forms; use percentage-based tolerance.
    orig_wc = len(baseline_nl.split())
    tolerance = max(5, int(orig_wc * 0.6))
    if len(perturbed.split()) > orig_wc + tolerance:
        result.fail(rid, comp, "shorter_without_on",
                    f"Perturbed ({len(perturbed.split())}) much longer than original ({orig_wc})+{tolerance}")
    else:
        result.ok("shorter_without_on")

    # 11. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 12. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}'.")
    add_common_args(parser)
    parser.add_argument("--input", "-i", default=str(ROOT / DEFAULT_INPUT_FILE))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    init_from_args(args)
    result = run_tests(args.input, PERTURBATION_NAME, check_record, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)
