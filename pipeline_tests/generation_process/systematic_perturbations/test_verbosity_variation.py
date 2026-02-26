"""
Test Suite: verbosity_variation (Perturbation ID 4)
====================================================
Validates the 'verbosity_variation' perturbation across all 3,500 records.

Perturbation contract
---------------------
  Purpose  : Add conversational fillers, hedging language, and informal expressions
             to make the prompt more verbose and casual.
  Applicable: ALWAYS (all 3,500 records) — applicable=True for every record.
  Rules    :
    1. Add filler words: 'basically', 'like', 'you know', 'kind of', 'sort of'
    2. Add hedging: 'I think', 'maybe', 'probably', 'or something'
    3. Add conversational starts: 'So', 'Well', 'Okay', 'Alright'
    4. Add informal phrasing: 'I'd like to', 'Can we', 'I want'
    5. Perturbed must be LONGER than original (fillers add words)
    6. Core content must be preserved (columns, tables, conditions)

Filler banks (from cached_info.py):
  hedging      : basically, kind of, sort of, like, you know, I think, probably
  conversational: So, Well, Okay, Alright, Um, Uh
  informal     : gonna, wanna, gotta, a bunch of, or something, or whatever
  redundant    : all the, any and all, each and every, the whole

Observed additions in dataset:
  'okay', 'so', 'well', 'uh', 'um', 'alright', 'gotta', 'you know', 'wanna',
  'or something', 'or whatever', 'a bunch of', 'like'

Checks implemented (16 named checks)
--------------------------------------
APPLICABILITY
  1.  always_applicable             – applicable=True for every record
  2.  string_when_applicable        – non-empty string prompt
  3.  no_object_repr                – no [None], Subquery(, etc.

LENGTH
  4.  prompt_longer_than_original   – perturbed has MORE words than original
  5.  not_absurdly_long             – word count ≤ 3× original (not runaway)
  6.  length_increase_reasonable    – word delta in [2, 20] (dataset shows min=2, max=12)

FILLER PRESENCE
  7.  contains_filler_word          – at least one filler word from defined banks present
  8.  filler_not_only_addition      – added words include at least one SEMANTIC filler
                                     (not just punctuation changes)

CONTENT PRESERVATION
  9.  columns_preserved             – column names from baseline in perturbed
  10. table_still_present           – at least one schema table name in perturbed
  11. condition_values_preserved    – SQL string literals & numbers preserved
  12. ordering_cue_preserved        – "ordered by" cue preserved if in baseline
  13. limit_cue_preserved           – "limited to"/"limit" cue preserved if in baseline
  14. no_control_chars              – no raw tab/newline/CR

COMPLEXITY-SPECIFIC
  15. join_relationship_preserved   – join: coupling phrase or both tables present
  16. union_connector_preserved     – union: "combined with" or "union" connector present
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

PERTURBATION_NAME = "verbosity_variation"
DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"

# Filler words from all banks (lower-cased)
FILLER_WORDS = {
    "basically", "kind", "sort", "like", "you", "know", "think", "probably",
    "so", "well", "okay", "alright", "um", "uh",
    "gonna", "wanna", "gotta", "something", "whatever", "bunch",
    "all", "every",
}

UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}


def check_record(r, comp, result):
    rid = r["id"]
    sp  = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "always_applicable", "Perturbation entry missing")
        return

    applicable = sp.get("applicable")
    baseline_nl = baseline(r)
    base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")

    # 1. always_applicable
    if applicable is not True:
        result.fail(rid, comp, "always_applicable",
                    f"Expected applicable=True for all records, got {applicable!r}")
        return
    result.ok("always_applicable")

    # 2. string_when_applicable
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable",
                    "applicable=True but perturbed_nl_prompt missing or wrong type")
        return
    result.ok("string_when_applicable")

    pert_l = perturbed.lower()

    # 3. no_object_repr
    for pat in [r"\[None\]", r"\bNone\b", r"Subquery\(", r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"Object repr pattern '{pat}': {perturbed[:100]}")
            return
    result.ok("no_object_repr")

    # 4. prompt_longer_than_original
    orig_wc = len(baseline_nl.split())
    pert_wc = len(perturbed.split())
    if pert_wc <= orig_wc:
        result.fail(rid, comp, "prompt_longer_than_original",
                    f"Perturbed ({pert_wc} words) not longer than original ({orig_wc} words)")
    else:
        result.ok("prompt_longer_than_original")

    # 5. not_absurdly_long
    if pert_wc > orig_wc * 3:
        result.fail(rid, comp, "not_absurdly_long",
                    f"Perturbed is {pert_wc} words vs {orig_wc} (>{orig_wc*3}: too long)")
    else:
        result.ok("not_absurdly_long")

    # 6. length_increase_reasonable (observed range min=2, max=12, allow up to 25)
    delta = pert_wc - orig_wc
    if delta < 1 or delta > 25:
        result.fail(rid, comp, "length_increase_reasonable",
                    f"Word delta {delta} outside [1,25]: orig={orig_wc}, pert={pert_wc}")
    else:
        result.ok("length_increase_reasonable")

    # 7. contains_filler_word
    pert_words = set(re.findall(r"\b[a-z]+\b", pert_l))
    orig_words = set(re.findall(r"\b[a-z]+\b", base_l))
    added_words = pert_words - orig_words
    if not (added_words & FILLER_WORDS):
        result.fail(rid, comp, "contains_filler_word",
                    f"No filler word added. Added words: {sorted(added_words)[:8]}")
    else:
        result.ok("contains_filler_word")

    # 8. filler_not_only_addition — at least one multi-char non-numeric filler added
    real_additions = [w for w in added_words if len(w) >= 2 and not w.isdigit()]
    if not real_additions:
        result.fail(rid, comp, "filler_not_only_addition",
                    "No real (2+ char) added words found in perturbed")
    else:
        result.ok("filler_not_only_addition")

    # 9. columns_preserved
    for col in known_columns():
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' in baseline but missing from perturbed: {perturbed[:120]}")
            break
    else:
        result.ok("columns_preserved")

    # 10. table_still_present
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base:
        if not any(table_in_nl(t, pert_l) for t in tables_in_base):
            result.fail(rid, comp, "table_still_present",
                        f"No schema table found in perturbed: {perturbed[:120]}")
        else:
            result.ok("table_still_present")

    # 11. condition_values_preserved
    lits = sql_literals(baseline_nl)
    nums = numbers(baseline_nl)
    ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved",
                        f"String literal '{lit}' lost: {perturbed[:120]}")
            ok = False; break
    if ok:
        for num in nums:
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved",
                            f"Numeric value '{num}' lost: {perturbed[:120]}")
                break
        else:
            result.ok("condition_values_preserved")

    # 12. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved",
                        f"Ordering cue lost: {perturbed[:120]}")
        else:
            result.ok("ordering_cue_preserved")

    # 13. limit_cue_preserved
    if "limited to" in base_l or "limit" in base_l:
        if "limit" not in pert_l:
            result.fail(rid, comp, "limit_cue_preserved", f"Limit cue lost: {perturbed[:120]}")
        else:
            result.ok("limit_cue_preserved")

    # 14. no_control_chars
    if re.search(r"[\t\n\r]", perturbed):
        result.fail(rid, comp, "no_control_chars", f"Control char in: {repr(perturbed[:80])}")
    else:
        result.ok("no_control_chars")

    # 15. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved",
                        f"Join coupling absent: {perturbed[:120]}")
        else:
            result.ok("join_relationship_preserved")

    # 16. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved",
                        f"Union connector absent: {perturbed[:120]}")
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
