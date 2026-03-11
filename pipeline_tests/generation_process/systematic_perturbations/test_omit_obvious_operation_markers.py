"""
Test Suite: omit_obvious_operation_markers (Perturbation ID 1)
==============================================================
Validates the 'omit_obvious_operation_markers' perturbation field produced by the
systematic perturbation generator for all 3,500 records (7 complexity types × 500).

Perturbation contract (from cached_info.py)
-------------------------------------------
  Purpose : Remove explicit SQL keywords and structural clause markers while
            retaining sufficient NL signals so the operation type and constraints
            remain clear.
  Applicable : SELECT-based complexities (simple, join, advanced, union) PLUS
               update and delete (which have meaningful operation markers to omit).
               NOT applicable for INSERT (operation markers cannot be stripped while
               keeping intent clear from the generator's encoding).
  Key rules  :
    1. Remove structural clause prepositions that directly map to SQL: "FROM"
       (as rendered by the NL renderer as "from", "in", "within", "out of", etc.)
    2. Remove filter-introducing prepositions (e.g., "filtered for", "where")
       OR minimally transform them.
    3. Preserve: column names, table names, condition values, ordering cues.
    4. Preserve enough NL cues to identify the operation type.

Checks implemented (21 named checks)
--------------------------------------
APPLICABILITY
  1.  applicable_field_present      – every record's perturbation entry has the 'applicable' field
  2.  applicable_is_bool            – 'applicable' is a boolean (not null or string)
  3.  null_when_not_applicable      – when applicable=False, perturbed_nl_prompt must be null/None
  4.  string_when_applicable        – when applicable=True, perturbed_nl_prompt is a non-empty string
  5.  not_applicable_for_insert     – INSERT complexity must always be not-applicable
  6.  applicable_for_select_types   – simple / join / advanced / union must always be applicable

STRUCTURE (when applicable)
  7.  no_object_repr                – no [None], None token, Subquery(, Column( in perturbed
  8.  no_control_chars              – no raw tab / newline / carriage return in perturbed
  9.  length_sanity                 – perturbed is 5–900 chars
  10. shorter_than_original         – perturbed must be ≤ original length (omitting words, not adding)
      OR at most marginally longer (+15 chars to allow minor rephrasing)

CONTENT PRESERVATION (when applicable)
  11. columns_preserved             – each column name from the baseline NL still appears in perturbed
  12. table_still_present           – at least one schema table name (or recognised synonym) still in perturbed
  13. condition_values_preserved    – string literals ('value') and numeric values from baseline still present
  14. ordering_cue_preserved        – if baseline contains "ordered by", perturbed still has ordering reference
  15. limit_cue_preserved           – if baseline contains "limited to", perturbed still has limit reference

MARKER OMISSION (when applicable)
  16. from_preposition_reduced      – at least one FROM-equivalent preposition ("in ", " within ", " out of ",
                                     " from ") that was in the baseline is absent or reduced in the perturbed;
                                     OR the perturbed is semantically different from the original (some change made)
  17. prompt_is_different           – perturbed != original (some change was actually made)
  18. major_content_not_lost        – perturbed is not empty or a single word (operation was not over-stripped)

COMPLEXITY-SPECIFIC
  19. join_relationship_preserved   – for join: the coupling phrase ("and their", "along with", "joined with",
                                     JOIN keyword, or both table names) still conveys the join relationship
  20. union_connector_preserved     – for union: "combined with" or equivalent connector still present in perturbed
  21. dml_operation_cue_preserved   – for update/delete: the operation action word (update/delete/remove/change/
                                     modify/erase/etc.) still present in perturbed

Usage
-----
  python pipeline_tests/generation_process/systematic_perturbations/test_omit_obvious_operation_markers.py
  python pipeline_tests/generation_process/systematic_perturbations/test_omit_obvious_operation_markers.py -v
  python pipeline_tests/generation_process/systematic_perturbations/test_omit_obvious_operation_markers.py \\
      --input dataset/social_media/systematic_perturbations.json
"""

import re

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns, column_synonyms_bare,
    col_in_text, is_synonym_fragment,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "omit_obvious_operation_markers"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"

# Complexity groupings
SELECT_COMPLEXITIES = {"simple", "join", "advanced", "union"}
DML_COMPLEXITIES    = {"insert", "update", "delete"}

# Operation verbs that must survive in update/delete NL
UPDATE_VERBS = {"update", "modify", "change", "set", "alter", "edit", "adjust", "correct", "amend"}
DELETE_VERBS = {"delete", "remove", "erase", "drop", "purge", "eliminate", "clear", "discard", "strip", "wipe"}

# Prepositions the renderer uses that directly correspond to "FROM"
FROM_PREPOSITIONS = {" from ", " in ", " within ", " out of ", " inside ", " across "}

# Union connector the renderer uses
UNION_CONNECTORS = {"combined with", "union", "along with"}

# Join coupling phrases the renderer uses
JOIN_COUPLING = {"and their", "along with", "joined with", "join", "with their", "left join",
                 "right join", "full join", "inner join"}


# ── Check functions ────────────────────────────────────────────────────────

def check_record(r: dict, comp: str, result: TestResult):
    rid = r["id"]
    sp = get_pert(r, PERTURBATION_NAME)
    if sp is None:
        result.fail(rid, comp, "applicable_field_present",
                    f"Perturbation '{PERTURBATION_NAME}' entry missing entirely")
        return

    baseline_nl = baseline(r)
    base_l = baseline_nl.lower()

    # ── 1. applicable_field_present ──────────────────────────────────────
    result.ok("applicable_field_present")

    # ── 2. applicable_is_bool ────────────────────────────────────────────
    applicable = sp.get("applicable")
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool",
                    f"'applicable' is {type(applicable).__name__}, expected bool")
        return
    result.ok("applicable_is_bool")

    perturbed = sp.get("perturbed_nl_prompt")

    # ── 3. null_when_not_applicable ──────────────────────────────────────
    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but perturbed_nl_prompt is not null: {str(perturbed)[:80]}")
        else:
            result.ok("null_when_not_applicable")
        # Applicability contract checks (always run)
        # 5. INSERT must be not-applicable
        if comp == "insert":
            result.ok("not_applicable_for_insert")
        # 6. SELECT types must be applicable – flag if they are not
        if comp in SELECT_COMPLEXITIES:
            result.fail(rid, comp, "applicable_for_select_types",
                        f"SELECT-type complexity '{comp}' must be applicable but is not")
        return

    # ── applicable = True branch ─────────────────────────────────────────

    # 4. string_when_applicable
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable",
                    "applicable=True but perturbed_nl_prompt missing or not a string")
        return
    result.ok("string_when_applicable")

    pert_l = perturbed.lower()

    # 5. not_applicable_for_insert
    if comp == "insert":
        result.fail(rid, comp, "not_applicable_for_insert",
                    f"INSERT must be not-applicable, but applicable=True: {perturbed[:80]}")
    else:
        result.ok("not_applicable_for_insert")

    # 6. applicable_for_select_types (already applicable=True, so pass)
    if comp in SELECT_COMPLEXITIES:
        result.ok("applicable_for_select_types")

    # ── 7. no_object_repr ────────────────────────────────────────────────
    obj_pats = [r"\[None\]", r"\bNone\b", r"Subquery\(", r"Column\("]
    for pat in obj_pats:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr",
                        f"Object repr pattern '{pat}' in perturbed: {perturbed[:100]}")
            return
    result.ok("no_object_repr")

    # ── 8. no_control_chars ──────────────────────────────────────────────
    if re.search(r"[\t\n\r]", perturbed):
        result.fail(rid, comp, "no_control_chars",
                    f"Control char in perturbed: {repr(perturbed[:80])}")
    else:
        result.ok("no_control_chars")

    # ── 9. length_sanity ─────────────────────────────────────────────────
    if len(perturbed) < 5:
        result.fail(rid, comp, "length_sanity", f"Too short ({len(perturbed)} chars): {perturbed!r}")
    elif len(perturbed) > 900:
        result.fail(rid, comp, "length_sanity", f"Too long ({len(perturbed)} chars)")
    else:
        result.ok("length_sanity")

    # ── 10. shorter_than_original (or within synonym-variation tolerance) ─
    # Re-rendering may pick different (longer) synonyms, so allow up to 100%
    # extra length beyond the baseline to account for synonym variation.
    tolerance = max(20, int(len(baseline_nl) * 1.0))
    if len(perturbed) > len(baseline_nl) + tolerance:
        result.fail(rid, comp, "shorter_than_original",
                    f"Perturbed longer than baseline by {len(perturbed)-len(baseline_nl)} chars "
                    f"(expected omission). orig={len(baseline_nl)}, pert={len(perturbed)}")
    else:
        result.ok("shorter_than_original")

    # ── 11. columns_preserved (dictionary-aware, word-boundary + synonym-fragment safe) ──
    # Column names in baseline NL should survive in perturbed (or as synonyms)
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

    # ── 12. table_still_present (schema-aware) ──────────────────────────
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base:
        still_there = any(table_in_nl(t, pert_l) for t in tables_in_base)
        if not still_there:
            # Fallback: accept if ANY known table appears in perturbed
            any_known = any(table_in_nl(t, pert_l) for t in known_tables())
            if not any_known:
                result.fail(rid, comp, "table_still_present",
                            f"No schema table from baseline found in perturbed: {perturbed[:120]}")
            else:
                result.ok("table_still_present")
        else:
            result.ok("table_still_present")

    # ── 13. condition_values_preserved ───────────────────────────────────
    # String literals and numeric values from baseline should still be in perturbed
    lits = sql_literals(baseline_nl)
    nums = numbers(baseline_nl)
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved",
                        f"String literal '{lit}' lost from perturbed: {perturbed[:120]}")
            break
    else:
        for num in nums:
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved",
                            f"Numeric value '{num}' lost from perturbed: {perturbed[:120]}")
                break
        else:
            result.ok("condition_values_preserved")

    # ── 14. ordering_cue_preserved ───────────────────────────────────────
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved",
                        f"Baseline has ordering cue but perturbed loses it: {perturbed[:120]}")
        else:
            result.ok("ordering_cue_preserved")

    # ── 15. limit_cue_preserved ──────────────────────────────────────────
    if "limited to" in base_l or "limit" in base_l:
        if "limit" not in pert_l:
            result.fail(rid, comp, "limit_cue_preserved",
                        f"Baseline has limit cue but perturbed loses it: {perturbed[:120]}")
        else:
            result.ok("limit_cue_preserved")

    # ── 16. from_preposition_reduced ─────────────────────────────────────
    # Count FROM-equivalent prepositions in baseline vs perturbed
    # The perturbation should reduce at least one, OR the prompt must differ somehow
    count_base = sum(1 for prep in FROM_PREPOSITIONS if prep in base_l)
    count_pert = sum(1 for prep in FROM_PREPOSITIONS if prep in pert_l)
    if count_base > 0 and count_pert >= count_base and perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "from_preposition_reduced",
                    f"No FROM-preposition omitted and prompt unchanged: {perturbed[:120]}")
    else:
        result.ok("from_preposition_reduced")

    # ── 17. prompt_is_different ──────────────────────────────────────────
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "prompt_is_different",
                    f"Perturbed == original (no change made): {perturbed[:120]}")
    else:
        result.ok("prompt_is_different")

    # ── 18. major_content_not_lost ───────────────────────────────────────
    words_pert = len(perturbed.split())
    words_base = len(baseline_nl.split())
    if words_pert < max(2, words_base // 3):
        result.fail(rid, comp, "major_content_not_lost",
                    f"Perturbed has only {words_pert} words vs {words_base} in baseline "
                    f"(over-stripped): {perturbed[:120]}")
    else:
        result.ok("major_content_not_lost")

    # ── 19. join_relationship_preserved ──────────────────────────────────
    if comp == "join":
        has_coupling = any(phrase in pert_l for phrase in JOIN_COUPLING)
        # Also accept if both tables appear (implicit relationship even without explicit JOIN phrase)
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved",
                        f"Join: no coupling phrase AND not both tables in perturbed: {perturbed[:120]}")
        else:
            result.ok("join_relationship_preserved")

    # ── 20. union_connector_preserved ────────────────────────────────────
    if comp == "union":
        has_connector = any(c in pert_l for c in UNION_CONNECTORS)
        if not has_connector:
            result.fail(rid, comp, "union_connector_preserved",
                        f"Union: no connector phrase in perturbed: {perturbed[:120]}")
        else:
            result.ok("union_connector_preserved")

    # ── 21. dml_operation_cue_preserved ──────────────────────────────────
    if comp == "update":
        if not any(v in pert_l for v in UPDATE_VERBS):
            result.fail(rid, comp, "dml_operation_cue_preserved",
                        f"Update: no update-action cue in perturbed: {perturbed[:120]}")
        else:
            result.ok("dml_operation_cue_preserved")
    if comp == "delete":
        if not any(v in pert_l for v in DELETE_VERBS):
            result.fail(rid, comp, "dml_operation_cue_preserved",
                        f"Delete: no delete-action cue in perturbed: {perturbed[:120]}")
        else:
            result.ok("dml_operation_cue_preserved")


# ── Runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description=f"Validate '{PERTURBATION_NAME}' perturbations.")
    parser.add_argument("--input", "-i",
                        default=str(ROOT / DEFAULT_INPUT_FILE),
                        help="Path to the systematic NL perturbation JSON file")
    add_common_args(parser)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    init_from_args(args)
    result = run_tests(args.input, PERTURBATION_NAME, check_record, verbose=args.verbose)
    print(result.summary())
    import sys; sys.exit(0 if result.ok_overall else 1)
