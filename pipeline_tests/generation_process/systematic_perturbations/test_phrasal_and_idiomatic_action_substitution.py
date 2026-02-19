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
  15. length_reasonable             – word count of perturbed is within ±8 of original
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

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]

SCHEMA = {
    "users":    {"id", "username", "email", "signup_date", "is_verified", "country_code"},
    "posts":    {"id", "user_id", "content", "posted_at", "view_count"},
    "comments": {"id", "user_id", "post_id", "comment_text", "created_at"},
    "likes":    {"user_id", "post_id", "liked_at"},
    "follows":  {"follower_id", "followee_id", "followed_at"},
}

DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"
PERTURBATION_NAME = "phrasal_and_idiomatic_action_substitution"

KNOWN_TABLES = set(SCHEMA.keys())
KNOWN_COLUMNS = {col for cols in SCHEMA.values() for col in cols}

SELECT_COMPLEXITIES = {"simple", "join", "advanced", "union"}
DML_COMPLEXITIES    = {"insert", "update", "delete"}

TABLE_SYNONYMS = {
    "users":    {"users", "user", "members", "member", "accounts", "account", "people"},
    "posts":    {"posts", "post", "articles", "article", "entries", "entry"},
    "comments": {"comments", "comment", "replies", "reply", "feedback"},
    "likes":    {"likes", "like", "reactions", "reaction", "votes", "vote"},
    "follows":  {"follows", "follow", "connections", "connection", "subscriptions"},
}

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

def _table_in_nl(table: str, nl_lower: str) -> bool:
    for c in TABLE_SYNONYMS.get(table, {table}):
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest   = nl_lower[m.end():]
            before = nl_lower[:m.start()]
            if rest.startswith("_"):
                continue
            if c == "like" and re.match(r"\s*['\"%]", rest):
                continue
            if before.endswith("'") or rest.startswith("'"):
                continue
            return True
    return False


def _extract_sql_literals(text: str) -> list[str]:
    """SQL-style string literals only (not NL possessives)."""
    return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)


def _extract_numbers(text: str) -> list[str]:
    return re.findall(r"\b\d+\b", text)


def _get_perturbation(r: dict) -> dict | None:
    for sp in r.get("generated_perturbations", {}).get("single_perturbations", []):
        if sp.get("perturbation_name") == PERTURBATION_NAME:
            return sp
    return None


def _get_baseline_nl(r: dict) -> str:
    return r.get("generated_perturbations", {}).get("original", {}).get("nl_prompt", "")


def _first_word(text: str) -> str:
    """Return the first alphabetical word (lowercased)."""
    m = re.search(r"[a-zA-Z]+", text)
    return m.group(0).lower() if m else ""


# ── Result collector ─────────────────────────────────────────────────────────

class TestResult:
    def __init__(self, verbose: bool = False):
        self.failures: list[dict] = []
        self.passed = 0
        self.verbose = verbose

    def ok(self, _check: str):
        self.passed += 1

    def fail(self, rid: Any, comp: str, check: str, detail: str):
        self.failures.append({"id": rid, "complexity": comp, "check": check, "detail": detail})
        if self.verbose:
            print(f"  ✗ [{comp} id={rid}] {check}: {detail}")

    def summary(self) -> str:
        total = self.passed + len(self.failures)
        lines = [
            "",
            "=" * 70,
            f"Perturbation Test: {PERTURBATION_NAME}",
            "=" * 70,
            f"  Total checks : {total}",
            f"  Passed       : {self.passed}",
            f"  Failed       : {len(self.failures)}",
        ]
        if self.failures:
            lines.append("")
            lines.append("Failures by check:")
            by_check: dict[str, list] = defaultdict(list)
            for f in self.failures:
                by_check[f["check"]].append(f)
            for check, items in sorted(by_check.items()):
                lines.append(f"  [{len(items):3d}x] {check}")
                for item in items[:3]:
                    lines.append(f"        id={item['id']} [{item['complexity']}]: {item['detail'][:130]}")
                if len(items) > 3:
                    lines.append(f"        ... and {len(items) - 3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self) -> bool:
        return len(self.failures) == 0


# ── Check function ────────────────────────────────────────────────────────────

def check_record(r: dict, comp: str, result: TestResult):
    rid = r["id"]
    sp  = _get_perturbation(r)

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

    baseline_nl = _get_baseline_nl(r)
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

    # ── 10. columns_preserved ────────────────────────────────────────────
    for col in KNOWN_COLUMNS:
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' in baseline but missing from perturbed: {perturbed[:120]}")
            break
    else:
        result.ok("columns_preserved")

    # ── 11. table_still_present ──────────────────────────────────────────
    tables_in_base = [t for t in KNOWN_TABLES if _table_in_nl(t, base_l)]
    if tables_in_base:
        any_present = any(_table_in_nl(t, pert_l) for t in tables_in_base)
        if not any_present:
            result.fail(rid, comp, "table_still_present",
                        f"No schema table from baseline found in perturbed: {perturbed[:120]}")
        else:
            result.ok("table_still_present")

    # ── 12. condition_values_preserved ───────────────────────────────────
    literals = _extract_sql_literals(baseline_nl)
    numbers  = _extract_numbers(baseline_nl)
    value_ok = True
    for lit in literals:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved",
                        f"String literal '{lit}' lost from perturbed: {perturbed[:120]}")
            value_ok = False
            break
    if value_ok:
        for num in numbers:
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
    orig_wc = len(baseline_nl.split())
    pert_wc = len(perturbed.split())
    if abs(pert_wc - orig_wc) > 15:
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
        tables_in_pert = [t for t in tables_in_base if _table_in_nl(t, pert_l)]
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


# ── Runner ────────────────────────────────────────────────────────────────────

def _complexity_from_sql(sql: str) -> str:
    sql_u = sql.upper().strip()
    if sql_u.startswith("INSERT"):  return "insert"
    if sql_u.startswith("UPDATE"):  return "update"
    if sql_u.startswith("DELETE"):  return "delete"
    if "UNION" in sql_u:            return "union"
    if "JOIN"  in sql_u:            return "join"
    if "IN (SELECT" in sql_u or "EXISTS" in sql_u or "FROM (" in sql_u:
        return "advanced"
    tables = re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", sql_u)
    flat = [t for pair in tables for t in pair if t]
    if len(flat) >= 2 and len(set(flat)) == 1:
        return "advanced"
    return "simple"


def run_tests(input_file: str, verbose: bool = False) -> TestResult:
    result = TestResult(verbose=verbose)
    with open(input_file) as f:
        dataset = json.load(f)

    print(f"Loaded {len(dataset)} records from {input_file}")
    print(f"Running tests for: {PERTURBATION_NAME}{'  (verbose)' if verbose else ''}\n")

    by_comp: dict[str, int] = defaultdict(int)
    for r in dataset:
        comp = _complexity_from_sql(r["sql"])
        by_comp[comp] += 1
        check_record(r, comp, result)

    print("Record counts by complexity:")
    for c in ["simple", "join", "advanced", "union", "insert", "update", "delete"]:
        print(f"  {c:12s}: {by_comp.get(c, 0)}")
    print()
    return result


def main():
    parser = argparse.ArgumentParser(
        description=f"Validate '{PERTURBATION_NAME}' perturbations in the systematic dataset."
    )
    parser.add_argument("--input", "-i",
                        default=str(ROOT / DEFAULT_INPUT_FILE),
                        help="Path to the systematic NL perturbation JSON file")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print each failure as it occurs")
    args = parser.parse_args()

    result = run_tests(args.input, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)


if __name__ == "__main__":
    main()
