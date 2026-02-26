"""
Test Suite: mixed_sql_nl (Perturbation ID 10)
==============================================
Validates the 'mixed_sql_nl' perturbation.

Contract
--------
  Not applicable to INSERT queries (which have no SELECT clause to embed).
  3000 applicable (insert=0), 500 not-applicable.
  Mixes SQL keywords (SELECT, FROM, WHERE) back into the NL prompt.

Checks (14 named checks)
"""

import re
import sys
from pathlib import Path

from common import (
    add_common_args, init_from_args,
    known_tables, known_columns, column_synonyms_bare,
    col_in_text, is_synonym_fragment,
    table_in_nl, sql_literals, numbers,
    get_pert, baseline,
    TestResult, run_tests, ROOT,
)

PERTURBATION_NAME = "mixed_sql_nl"
DEFAULT_INPUT_FILE = "dataset/social_media/systematic_perturbations.json"
UNION_CONNECTORS = {"combined with", "union", "along with"}
JOIN_COUPLING    = {"and their", "along with", "joined with", "join", "with their",
                    "left join", "right join", "full join", "inner join"}

SQL_KEYWORDS = {"SELECT","FROM","WHERE","JOIN","HAVING","GROUP BY","UPDATE","DELETE","SET"}


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
        # Not applicable only for INSERT
        if comp != "insert":
            result.fail(rid, comp, "not_applicable_only_for_insert",
                        f"Not-applicable but complexity={comp}")
        else:
            result.ok("not_applicable_only_for_insert")
        return

    # applicable = True
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. sql_keyword_present — at least one SQL keyword injected
    has_keyword = any(kw in perturbed.upper().split() for kw in SQL_KEYWORDS)
    # Also check for patterns like 'SELECT ' or 'FROM ' with space
    has_keyword = has_keyword or bool(re.search(r"\b(SELECT|FROM|WHERE)\b", perturbed, re.I))
    if not has_keyword:
        result.fail(rid, comp, "sql_keyword_present",
                    f"No SQL keyword found in perturbed: {perturbed[:120]}")
    else:
        result.ok("sql_keyword_present")

    # 6. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 7. columns_preserved (dictionary-aware, word-boundary + synonym-fragment safe)
    col_syns = column_synonyms_bare()
    for col in known_columns():
        if col_in_text(col, base_l) and not col_in_text(col, pert_l):
            # Skip if col is part of a multi-word synonym for another column
            if is_synonym_fragment(col, base_l):
                continue
            # Accept if any dictionary synonym appears in perturbed
            synonyms = col_syns.get(col, set())
            if not any(syn in pert_l for syn in synonyms):
                result.fail(rid, comp, "columns_preserved", f"Column '{col}' missing"); break
    else:
        result.ok("columns_preserved")

    # 8. table_still_present (schema-aware)
    tables_in_base = [t for t in known_tables() if table_in_nl(t, base_l)]
    if tables_in_base:
        any_from_base = any(table_in_nl(t, pert_l) for t in tables_in_base)
        if not any_from_base:
            # Fallback: accept if ANY known table appears in perturbed
            any_known = any(table_in_nl(t, pert_l) for t in known_tables())
            if not any_known:
                result.fail(rid, comp, "table_still_present", f"No table: {perturbed[:120]}")
            else:
                result.ok("table_still_present")
        else:
            result.ok("table_still_present")

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

    # 10. length_reasonable — mixed SQL/NL can be shorter (SQL keywords replace NL phrases)
    # UNION prompts can have larger deltas since both clauses are transformed.
    delta = abs(len(perturbed.split()) - len(baseline_nl.split()))
    limit = 20 if comp == "union" else 12
    if delta > limit:
        result.fail(rid, comp, "length_reasonable",
                    f"Word delta {delta} > {limit}: pert={len(perturbed.split())} orig={len(baseline_nl.split())}")
    else:
        result.ok("length_reasonable")

    # 11. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")

    # 12. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 13. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        join_in_pert = bool(re.search(r"\bJOIN\b", perturbed))
        tables_in_pert = [t for t in tables_in_base if table_in_nl(t, pert_l)]
        if not has_coupling and not join_in_pert and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved", f"Join absent: {perturbed[:120]}")
        else: result.ok("join_relationship_preserved")

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
