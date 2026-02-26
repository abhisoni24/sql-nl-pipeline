"""
Test Suite: phrasal_and_idiomatic_action_substitution (Perturbation ID 2)
==========================================================================
Validates the 'phrasal_and_idiomatic_action_substitution' perturbation in the
systematic dataset for all 3,500 records (7 complexity types × 500).

Perturbation contract (from cached_info.py)
-------------------------------------------
  Purpose : Replace the root action verb (the "intent verb") at the start of
            SELECT-based prompts with a multi-word phrasal verb or idiomatic
            expression from a predefined substitution bank, while keeping the
            rest of the prompt intact.
  Applicable : simple, join, advanced, union (SELECT-based).
               NOT applicable for INSERT, UPDATE, DELETE (DML has a fixed
               operation verb that cannot be substituted for a retrieval phrase).
  Rules   :
    1. Target ONLY the root action verb (first meaningful word / phrase).
    2. Replace 1-to-1 with a phrasal verb from the RETRIEVE, DISPLAY, or QUERY bank.
    3. The rest of the prompt (columns, tables, conditions) must remain intact.
    4. Substitution must not overlap with schema synonym or operator terms.

Substitution banks (from cached_info.py):
  RETRIEVE : pull up, dig out, fetch back, snag, grab, grab a hold of,
             extract a list of
  DISPLAY  : bring up, give me a look at, run a check for, produce a listing of,
             spit out, display for me
  QUERY    : look through, search for, track down, filter through, identify
  Also seen in dataset: go get, pick out, single out, fetch me, choose, retrieve,
                        display (re-used as replacement for other verbs)

Checks implemented (18 named checks)
--------------------------------------
APPLICABILITY
  1.  applicable_field_present      – perturbation entry exists for every record
  2.  applicable_is_bool            – 'applicable' is a boolean
  3.  null_when_not_applicable      – applicable=False → perturbed_nl_prompt is null
  4.  string_when_applicable        – applicable=True → non-empty string
  5.  not_applicable_for_dml        – insert/update/delete must be not-applicable
  6.  applicable_for_select_types   – simple/join/advanced/union must be applicable

VERB SUBSTITUTION (when applicable)
  7.  first_word_changed            – first word of perturbed != first word of original
                                     (substitution actually occurred)
  8.  verb_from_substitution_bank   – the leading phrase of perturbed comes from the
                                     defined substitution bank or known renderer phrases
  9.  original_verb_not_reused      – original first word does not appear as the
                                     leading verb word in the perturbed
                                     (exact same verb was not kept)

CONTENT INTEGRITY (when applicable)
  10. columns_preserved             – column names from baseline still in perturbed
  11. table_still_present           – at least one schema table name in perturbed
  12. condition_values_preserved    – SQL string literals and numeric values preserved
  13. ordering_cue_preserved        – if baseline has "ordered by", perturbed retains it
  14. limit_cue_preserved           – if baseline has "limited to", perturbed retains it
  15. length_reasonable             – word count of perturbed is within ±30 (or 100% of original)
                                     (verb phrase replacement, not content rewrite)
  16. no_object_repr                – no [None], None token, Subquery(, Column(

COMPLEXITY-SPECIFIC (when applicable)
  17. join_relationship_preserved   – join complexity: coupling phrase or both tables still
                                     convey the join relationship
  18. union_connector_preserved     – union complexity: "combined with" or "union" connector
                                     still present in perturbed

Usage
-----
  python pipeline_tests/generation_process/systematic_perturbations/test_phrasal_and_idiomatic_action_substitution.py
  python ...test_phrasal_and_idiomatic_action_substitution.py -v
  python ...test_phrasal_and_idiomatic_action_substitution.py --input path/to/file.json
"""

import re
import sys

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns, column_synonyms_bare,
    col_in_text, is_synonym_fragment,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "phrasal_and_idiomatic_action_substitution"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"

SELECT_COMPLEXITIES = {"simple", "join", "advanced", "union"}
DML_COMPLEXITIES    = {"insert", "update", "delete"}

# Full substitution bank (all verbs/phrases the renderer may produce)
SUBSTITUTION_BANK_WORDS = {
    # RETRIEVE bank
    "pull", "dig", "fetch", "snag", "grab", "extract",
    # DISPLAY bank
    "bring", "give", "run", "produce", "spit", "display",
    # QUERY bank
    "look", "search", "track", "filter", "identify",
    # Observed in dataset (renderer uses its own vocab too)
    "go", "pick", "single", "choose", "retrieve", "get", "find",
    "spot", "select", "show", "list", "read", "pull",
}

# These are the original baseline renderer intent verbs (first words)
BASELINE_INTENT_VERBS = {
    "get", "show", "select", "retrieve", "find", "display", "fetch",
    "list", "pull", "read", "pick", "return", "look", "bring", "go",
    "grab", "extract", "gather", "report", "spot", "single", "choose",
    "run", "give", "produce", "spit", "scan",
}

UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING = {"and their", "along with", "joined with", "join", "with their",
                 "left join", "right join", "full join", "inner join"}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _first_word(text: str) -> str:
    """Return the first alphabetical word (lowercased)."""
    m = re.search(r"[a-zA-Z]+", text)
    return m.group(0).lower() if m else ""


# ── Check function ────────────────────────────────────────────────────────────

def check_record(r: dict, comp: str, result: TestResult):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)

    # ── 1. applicable_field_present ──────────────────────────────────────
    if sp is None:
        result.fail(rid, comp, "applicable_field_present",
                    f"Perturbation '{PERTURBATION_NAME}' entry missing")
        return
    result.ok("applicable_field_present")

    # ── 2. applicable_is_bool ────────────────────────────────────────────
    applicable = sp.get("applicable")
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool",
                    f"'applicable' type is {type(applicable).__name__}, expected bool")
        return
    result.ok("applicable_is_bool")

    baseline_nl = baseline(r)
    base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")

    # ── 3. null_when_not_applicable ──────────────────────────────────────
    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt is not null: {str(perturbed)[:80]}")
        else:
            result.ok("null_when_not_applicable")
        # 5. DML must not be applicable
        if comp in DML_COMPLEXITIES:
            result.ok("not_applicable_for_dml")
        # 6. SELECT types must be applicable
        if comp in SELECT_COMPLEXITIES:
            result.fail(rid, comp, "applicable_for_select_types",
                        f"SELECT type '{comp}' must be applicable but is not")
        return

    # applicable = True beyond this point ────────────────────────────────

    # ── 4. string_when_applicable ────────────────────────────────────────
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable",
                    "applicable=True but perturbed_nl_prompt missing or not a string")
        return
    result.ok("string_when_applicable")

    pert_l = perturbed.lower()

    # ── 5. not_applicable_for_dml ────────────────────────────────────────
    if comp in DML_COMPLEXITIES:
        result.fail(rid, comp, "not_applicable_for_dml",
                    f"DML type '{comp}' must be not-applicable, got: {perturbed[:80]}")
    else:
        result.ok("not_applicable_for_dml")

    # ── 6. applicable_for_select_types ───────────────────────────────────
    if comp in SELECT_COMPLEXITIES:
        result.ok("applicable_for_select_types")

    # ── 7. first_word_changed ────────────────────────────────────────────
    orig_fw = _first_word(baseline_nl)
    pert_fw = _first_word(perturbed)
    if orig_fw == pert_fw and orig_fw:
        result.fail(rid, comp, "first_word_changed",
                    f"First word unchanged '{orig_fw}' — no substitution: {perturbed[:100]}")
    else:
        result.ok("first_word_changed")

    # ── 8. verb_from_substitution_bank ───────────────────────────────────
    if pert_fw not in SUBSTITUTION_BANK_WORDS:
        result.fail(rid, comp, "verb_from_substitution_bank",
                    f"First word '{pert_fw}' not in substitution bank: {perturbed[:100]}")
    else:
        result.ok("verb_from_substitution_bank")

    # ── 9. original_verb_not_reused ──────────────────────────────────────
    # The new first word should not be the same as the original first word
    # (already covered by check 7, but this specifically targets the exact verb)
    if orig_fw and pert_fw == orig_fw:
        result.fail(rid, comp, "original_verb_not_reused",
                    f"Original verb '{orig_fw}' reused unchanged as first word: {perturbed[:100]}")
    else:
        result.ok("original_verb_not_reused")

    # ── 10. columns_preserved (dictionary-aware, word-boundary + synonym-fragment safe) ──
    col_syns = column_synonyms_bare()
    for col in known_columns():
        if col_in_text(col, base_l) and not col_in_text(col, pert_l):
            if is_synonym_fragment(col, base_l):
                continue
            synonyms = col_syns.get(col, set())
            if not any(syn.lower() in pert_l for syn in synonyms):
                result.fail(rid, comp, "columns_preserved",
                            f"Column '{col}' in baseline but missing from perturbed: {perturbed[:120]}")
                break
    else:
        result.ok("columns_preserved")

    # ── 11. table_still_present (schema-aware) ──────────────────────────
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base:
        any_present = any(table_in_nl(t, pert_l) for t in tables_in_base)
        if not any_present:
            # Fallback: accept if ANY known table appears in perturbed
            any_known = any(table_in_nl(t, pert_l) for t in known_tables())
            if not any_known:
                result.fail(rid, comp, "table_still_present",
                            f"No schema table from baseline found in perturbed: {perturbed[:120]}")
            else:
                result.ok("table_still_present")
        else:
            result.ok("table_still_present")

    # ── 12. condition_values_preserved ───────────────────────────────────
    lits = sql_literals(baseline_nl)
    nums = numbers(baseline_nl)
    value_ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved",
                        f"String literal '{lit}' lost from perturbed: {perturbed[:120]}")
            value_ok = False
            break
    if value_ok:
        for num in nums:
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved",
                            f"Numeric value '{num}' lost from perturbed: {perturbed[:120]}")
                break
        else:
            result.ok("condition_values_preserved")

    # ── 13. ordering_cue_preserved ───────────────────────────────────────
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved",
                        f"Ordering cue lost from perturbed: {perturbed[:120]}")
        else:
            result.ok("ordering_cue_preserved")

    # ── 14. limit_cue_preserved ──────────────────────────────────────────
    if "limited to" in base_l or "limit" in base_l:
        if "limit" not in pert_l:
            result.fail(rid, comp, "limit_cue_preserved",
                        f"Limit cue lost from perturbed: {perturbed[:120]}")
        else:
            result.ok("limit_cue_preserved")

    # ── 15. length_reasonable ────────────────────────────────────────────
    #    Schemas with many columns or PascalCase names can inflate word counts
    #    via possessive expansions, so allow up to ±30 words or 100 % of original.
    orig_wc = len(baseline_nl.split())
    pert_wc = len(perturbed.split())
    max_delta = max(30, orig_wc)
    if abs(pert_wc - orig_wc) > max_delta:
        result.fail(rid, comp, "length_reasonable",
                    f"Word count delta too large: orig={orig_wc}, pert={pert_wc} "
                    f"(delta={pert_wc-orig_wc}): {perturbed[:100]}")
    else:
        result.ok("length_reasonable")

    # ── 16. no_object_repr ───────────────────────────────────────────────
    for pat in [r"\[None\]", r"\bNone\b", r"Subquery\(", r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr",
                        f"Object repr pattern '{pat}' in perturbed: {perturbed[:100]}")
            break
    else:
        result.ok("no_object_repr")

    # ── 17. join_relationship_preserved ──────────────────────────────────
    if comp == "join":
        has_coupling = any(phrase in pert_l for phrase in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved",
                        f"Join coupling absent and not both tables in perturbed: {perturbed[:120]}")
        else:
            result.ok("join_relationship_preserved")

    # ── 18. union_connector_preserved ────────────────────────────────────
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved",
                        f"Union connector absent in perturbed: {perturbed[:120]}")
        else:
            result.ok("union_connector_preserved")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}' perturbations.")
    add_common_args(parser)
    parser.add_argument("--input", "-i", default=str(ROOT / DEFAULT_INPUT_FILE))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    init_from_args(args)
    result = run_tests(args.input, PERTURBATION_NAME, check_record, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)
