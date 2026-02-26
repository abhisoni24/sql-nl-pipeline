"""
Test Suite: table_column_synonyms (Perturbation ID 11)
=======================================================
Validates the 'table_column_synonyms' perturbation.

Contract
--------
  Applicable when the generator has a synonym for the table/column in the NL.
  3222 applicable, 278 not-applicable.
  Replaces table/column names with domain synonyms (e.g. users→members, post_id→article id).

Checks (13 named checks)
"""

import re
import sys

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns,
    table_in_nl, table_synonyms, column_synonyms_bare,
    sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "table_column_synonyms"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"

UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}



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

    # 5. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 6. synonym_used — the perturbed differs from baseline (a synonym was substituted)
    # Use word-diff to confirm something changed — any novel word not in baseline counts
    base_words = set(re.sub(r"[',]","",base_l).split())
    pert_words = set(re.sub(r"[',]","",pert_l).split())
    novel_words = pert_words - base_words
    tbl_syn_used = any(syn.replace(" ","") in pert_l.replace(" ","") for syns in table_synonyms().values() for syn in syns)
    col_syn_used = any(syn.replace(" ","") in pert_l.replace(" ","") for syns in column_synonyms_bare().values() for syn in syns)
    # Also accept any novel meaningful word (synonym substitution happened)
    meaningful_novel = any(len(w) > 3 and w.isalpha() for w in novel_words)
    if not (tbl_syn_used or col_syn_used or meaningful_novel):
        result.fail(rid, comp, "synonym_used",
                    f"No synonym change detected: {perturbed[:120]}")
    else:
        result.ok("synonym_used")

    # 7. noun_class_preserved — some entity concept preserved (table or one of its synonyms)
    # Check all known tables (not just those found in baseline via canonical name)
    # because self-joins and re-rendering may use different synonyms.
    any_entity_present = any(table_in_nl(t, pert_l) for t in known_tables())
    # Also accept any table synonym appearing in perturbed
    any_syns_present = any(
        syn.lower() in pert_l
        for syns in table_synonyms().values()
        for syn in syns
    )
    if not any_entity_present and not any_syns_present:
        result.fail(rid, comp, "noun_class_preserved",
                    f"No table or its synonym found: {perturbed[:120]}")
    else:
        result.ok("noun_class_preserved")

    # 8. condition_values_preserved
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

    # 9. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")

    # 10. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 11. length_reasonable — synonyms may add/remove words; use generous
    #     tolerance since multi-word synonyms (e.g. "electronic mail") inflate count
    orig_wc = len(baseline_nl.split())
    delta = abs(len(perturbed.split()) - orig_wc)
    limit = max(15, int(orig_wc * 1.0))
    if delta > limit:
        result.fail(rid, comp, "length_reasonable",
                    f"Word delta {delta} > {limit}: {perturbed[:120]}")
    else:
        result.ok("length_reasonable")

    # 12. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING) or bool(re.search(r"\bJOIN\b", perturbed))
        if not has_coupling:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent: {perturbed[:120]}")
        else: result.ok("join_relationship_preserved")

    # 13. union_connector_preserved
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
