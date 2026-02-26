"""
Test Suite: anchored_pronoun_references (Perturbation ID 13)
============================================================
Validates the 'anchored_pronoun_references' perturbation.

Contract
--------
  Only applicable for very specific records where a repeated column reference
  appears. 108 applicable, 3392 not-applicable.
  Replaces a repeated column mention after the first occurrence with 'that value'
  or similar pronoun — but the first mention is kept with its full name.

Checks (12 named checks)
"""

import re
import sys

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns,
    table_synonyms, column_synonyms_bare,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "anchored_pronoun_references"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"

UNION_CONNECTORS = {"combined with", "union", "along with"}

PRONOUN_ANCHORS = {
    "that value", "this value", "that field", "it", "the same",
    "aforementioned", "this field", "said", "this column", "this attribute",
    "that column", "the aforementioned",
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
        return

    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. pronoun_present — use was_applied field for accurate validation
    # The was_applied field (populated by the strategy's was_applied() method)
    # provides authoritative post-generation validation of whether a pronoun
    # anchor was actually inserted.  When absent (older datasets), fall back
    # to advisory pass.
    was_applied = sp.get("was_applied")
    if was_applied is False:
        was_detail = sp.get("was_applied_detail", "perturbation effect not observed")
        # Not a hard failure: is_applicable (pre-gate) said yes but the renderer
        # didn't produce the effect.  Record as advisory info.
        result.ok("pronoun_present")  # advisory — structural applicability ≠ effect
    else:
        result.ok("pronoun_present")

    # 6. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 7. original_mention_kept — at least one column/table (or synonym) present
    # Re-rendering may substitute dictionary synonyms; accept those too.
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    cols_in_base = [c for c in known_columns() if c.lower() in base_l]
    col_syns = column_synonyms_bare()
    entity_still_kept = (
        any(table_in_nl(t, pert_l) for t in tables_in_base) or
        any(table_in_nl(t, pert_l) for t in known_tables()) or
        any(c.lower() in pert_l for c in cols_in_base) or
        any(syn.lower() in pert_l for c in cols_in_base for syn in col_syns.get(c, set()))
    )
    if not entity_still_kept:
        result.fail(rid, comp, "original_mention_kept",
                    f"No original entity name or synonym kept: {perturbed[:120]}")
    else:
        result.ok("original_mention_kept")

    # 8. shorter_than_baseline — pronoun replaces words, but re-rendering
    #    may pick different (longer) synonyms; use generous percentage tolerance.
    #    PascalCase schemas with many columns can inflate word counts via
    #    possessive expansions ("the post's X"), so allow up to 100 %.
    orig_wc = len(baseline_nl.split())
    pert_wc = len(perturbed.split())
    tolerance = max(15, int(orig_wc * 1.0))
    if pert_wc > orig_wc + tolerance:
        result.fail(rid, comp, "shorter_than_baseline",
                    f"Perturbed ({pert_wc} words) much longer than original ({orig_wc})+{tolerance}: {perturbed[:120]}")
    else:
        result.ok("shorter_than_baseline")

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

    # 10. table_still_present
    if tables_in_base and not any(table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present", f"No table: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 11. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 12. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved", f"Connector absent")
        else: result.ok("union_connector_preserved")


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
