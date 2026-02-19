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
      --input dataset/current/nl_social_media_queries_systematic_20.json
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]

# Inline schema (mirrors src/core/schema.py) to avoid path issues when run directly
SCHEMA = {
    "users":    {"id": "int", "username": "varchar", "email": "varchar",
                 "signup_date": "datetime", "is_verified": "boolean", "country_code": "varchar"},
    "posts":    {"id": "int", "user_id": "int", "content": "text",
                 "posted_at": "datetime", "view_count": "int"},
    "comments": {"id": "int", "user_id": "int", "post_id": "int",
                 "comment_text": "text", "created_at": "datetime"},
    "likes":    {"user_id": "int", "post_id": "int", "liked_at": "datetime"},
    "follows":  {"follower_id": "int", "followee_id": "int", "followed_at": "datetime"},
}

DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"
PERTURBATION_NAME = "omit_obvious_operation_markers"

KNOWN_TABLES = set(SCHEMA.keys())
KNOWN_COLUMNS = {col for cols in SCHEMA.values() for col in cols}

# Complexity groupings
SELECT_COMPLEXITIES = {"simple", "join", "advanced", "union"}
DML_COMPLEXITIES    = {"insert", "update", "delete"}

# Operation verbs that must survive in update/delete NL
UPDATE_VERBS = {"update", "modify", "change", "set", "alter", "edit", "adjust", "correct", "amend"}
DELETE_VERBS = {"delete", "remove", "erase", "drop", "purge", "eliminate", "clear", "discard"}

# Prepositions the renderer uses that directly correspond to "FROM"
FROM_PREPOSITIONS = {" from ", " in ", " within ", " out of ", " inside ", " across "}

# Table synonyms (same as in test_nl_prompt.py)
TABLE_SYNONYMS = {
    "users":    {"users", "user", "members", "member", "accounts", "account", "people"},
    "posts":    {"posts", "post", "articles", "article", "entries", "entry"},
    "comments": {"comments", "comment", "replies", "reply", "feedback"},
    "likes":    {"likes", "like", "reactions", "reaction", "votes", "vote"},
    "follows":  {"follows", "follow", "connections", "connection", "subscriptions"},
}

# Union connector the renderer uses
UNION_CONNECTORS = {"combined with", "union", "along with"}

# Join coupling phrases the renderer uses
JOIN_COUPLING = {"and their", "along with", "joined with", "join", "with their", "left join",
                 "right join", "full join", "inner join"}


# ── Helpers ────────────────────────────────────────────────────────────────

def _table_in_nl(table: str, nl_lower: str) -> bool:
    """Word-boundary match with context exclusions (same logic as baseline test)."""
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


def _extract_string_literals(text: str) -> list[str]:
    """Return SQL-style single-quoted string literal values only.
    Avoids matching NL possessives (e.g. "user's", "post's") by requiring
    that the opening quote is NOT preceded by an alphanumeric character.
    """
    return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)


def _extract_numbers(text: str) -> list[str]:
    """Return all numeric tokens."""
    return re.findall(r"\b\d+\b", text)


def _get_perturbation(r: dict) -> dict | None:
    """Return the target perturbation dict for this record, or None."""
    for sp in r.get("generated_perturbations", {}).get("single_perturbations", []):
        if sp.get("perturbation_name") == PERTURBATION_NAME:
            return sp
    return None


def _get_baseline_nl(r: dict) -> str:
    return r.get("generated_perturbations", {}).get("original", {}).get("nl_prompt", "")


# ── Result collector ───────────────────────────────────────────────────────

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


# ── Check functions ────────────────────────────────────────────────────────

def check_record(r: dict, comp: str, result: TestResult):
    rid = r["id"]
    sp = _get_perturbation(r)
    if sp is None:
        result.fail(rid, comp, "applicable_field_present",
                    f"Perturbation '{PERTURBATION_NAME}' entry missing entirely")
        return

    baseline_nl = _get_baseline_nl(r)
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

    # ── 10. shorter_than_original (or barely longer) ─────────────────────
    if len(perturbed) > len(baseline_nl) + 15:
        result.fail(rid, comp, "shorter_than_original",
                    f"Perturbed longer than baseline by {len(perturbed)-len(baseline_nl)} chars "
                    f"(expected omission). orig={len(baseline_nl)}, pert={len(perturbed)}")
    else:
        result.ok("shorter_than_original")

    # ── 11. columns_preserved ────────────────────────────────────────────
    # Column names in baseline NL should survive in perturbed
    for col in KNOWN_COLUMNS:
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' in baseline but missing from perturbed: {perturbed[:120]}")
            break
    else:
        result.ok("columns_preserved")

    # ── 12. table_still_present ──────────────────────────────────────────
    tables_in_base = [t for t in KNOWN_TABLES if _table_in_nl(t, base_l)]
    if tables_in_base:
        still_there = any(_table_in_nl(t, pert_l) for t in tables_in_base)
        if not still_there:
            result.fail(rid, comp, "table_still_present",
                        f"No schema table from baseline found in perturbed: {perturbed[:120]}")
        else:
            result.ok("table_still_present")

    # ── 13. condition_values_preserved ───────────────────────────────────
    # String literals and numeric values from baseline should still be in perturbed
    literals = _extract_string_literals(baseline_nl)
    numbers  = _extract_numbers(baseline_nl)
    for lit in literals:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved",
                        f"String literal '{lit}' lost from perturbed: {perturbed[:120]}")
            break
    else:
        for num in numbers:
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
        tables_in_pert = [t for t in tables_in_base if _table_in_nl(t, pert_l)]
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

def load_complexity_map(sys_dataset: list[dict]) -> dict[int, str]:
    """Derive complexity from the SQL query type since the systematic dataset
    doesn't carry a 'complexity' field directly."""
    comp_map = {}
    for r in sys_dataset:
        sql_u = r["sql"].upper().strip()
        if sql_u.startswith("INSERT"):
            comp_map[r["id"]] = "insert"
        elif sql_u.startswith("UPDATE"):
            comp_map[r["id"]] = "update"
        elif sql_u.startswith("DELETE"):
            comp_map[r["id"]] = "delete"
        elif "UNION" in sql_u:
            comp_map[r["id"]] = "union"
        elif "JOIN" in sql_u:
            comp_map[r["id"]] = "join"
        elif ("IN (SELECT" in sql_u or "EXISTS" in sql_u or
              "FROM (" in sql_u):
            comp_map[r["id"]] = "advanced"
        else:
            # Could be simple or advanced (self-join) — use simple as default;
            # self-joins are flagged separately by SQL tests
            comp_map[r["id"]] = "simple"
        # Refine: if it's a self-join advanced query
        if comp_map[r["id"]] == "simple":
            import re as _re
            tables = _re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", sql_u)
            flat = [t for pair in tables for t in pair if t]
            if len(flat) >= 2 and len(set(flat)) == 1:
                comp_map[r["id"]] = "advanced"
    return comp_map


def run_tests(input_file: str, verbose: bool = False) -> TestResult:
    result = TestResult(verbose=verbose)

    with open(input_file) as f:
        dataset = json.load(f)

    print(f"Loaded {len(dataset)} records from {input_file}")
    print(f"Running tests for: {PERTURBATION_NAME}{'  (verbose)' if verbose else ''}\n")

    comp_map = load_complexity_map(dataset)
    by_comp: dict[str, int] = defaultdict(int)

    for r in dataset:
        comp = comp_map.get(r["id"], "unknown")
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
