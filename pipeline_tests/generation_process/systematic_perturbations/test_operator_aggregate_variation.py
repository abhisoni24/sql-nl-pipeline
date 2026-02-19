"""
Test Suite: operator_aggregate_variation (Perturbation ID 5)
=============================================================
Validates the 'operator_aggregate_variation' perturbation across all 3,500 records.

Perturbation contract
---------------------
  Purpose  : Express comparison operators and aggregate functions using varied
             natural language descriptions and symbolic formats.
  Applicable: When SQL or baseline NL contains comparison operators (>, <, >=, <=, !=, =)
              in the WHERE/HAVING clause, OR aggregate functions (COUNT, SUM, AVG, MAX, MIN).
              ~1567 applicable records out of 3500.
  Rules    :
    1. Replace operators with verbal equivalents: > → 'greater than'/'exceeds'/'above',
       >= → 'at least'/'on or after', < → 'less than'/'below', etc.
    2. Replace aggregate names: COUNT → 'total number'/'how many',
       AVG → 'average'/'mean', etc.
    3. Or go the other direction: verbal → symbolic variant.
    4. Preserve the comparison direction (must not flip > to <).

Operator banks (from cached_info.py):
  >  : greater than, exceeds, more than, above, higher than
  <  : less than, below, under, fewer than, lower than
  >= : at least, greater than or equal to, minimum of, no less than, on or after
  <= : at most, less than or equal to, maximum of, no more than
  =  : equals, is, matches, is equal to
  != : not equal to, is not, doesn't match, different from

Aggregate banks:
  COUNT : total number of, how many, count of, number of, quantity of
  SUM   : total, sum of, add up, combined total
  AVG   : average, mean, average value of, typical
  MAX   : maximum, highest, largest, biggest
  MIN   : minimum, lowest, smallest, least

Observed in dataset:
  'within the last N days' (for DATETIME relative) → 'since N days ago', 'after N days ago'
  'equals' → 'matches', 'is equal to'
  'greater than' → 'above', 'exceeds'
  'less than' → 'under', 'lower than', 'below'
  'within the last N days' (DATETIME) → 'on or after N days ago'

Checks implemented (16 named checks)
--------------------------------------
APPLICABILITY
  1.  applicable_field_present      – perturbation entry exists
  2.  applicable_is_bool            – 'applicable' is a boolean
  3.  null_when_not_applicable      – applicable=False → perturbed=null
  4.  string_when_applicable        – applicable=True → non-empty string
  5.  applicable_when_has_operator  – if SQL WHERE/HAVING has comparison op OR agg → applicable
  6.  not_applicable_when_no_op     – if SQL has no comparison op AND no agg → not applicable

OPERATOR/AGGREGATE TRANSFORMATION (when applicable)
  7.  some_change_made              – perturbed differs from baseline (change occurred)
  8.  comparison_direction_preserved – for each > operator in SQL: 'greater', 'above',
                                       'exceeds', 'more', 'higher', 'least', 'at least',
                                       'since', 'after', 'on or after' in perturbed;
                                       similarly for < operators
  9.  operator_synonym_used         – at least one known operator synonym or aggregate synonym
                                       appears in the perturbed
  10. no_raw_iso_date_added         – perturbed does not introduce a new ISO date literal
                                       (e.g. '2024-01-01') that was not in the baseline
                                       (temporal expression should be naturalised)

CONTENT PRESERVATION (when applicable)
  11. columns_preserved             – column names from baseline in perturbed
  12. table_still_present           – at least one schema table in perturbed
  13. condition_values_preserved    – non-date string literals and numbers preserved
  14. ordering_cue_preserved        – ordering cue preserved if in baseline
  15. no_object_repr                – no [None], Subquery(, etc.

COMPLEXITY-SPECIFIC
  16. union_connector_preserved     – union: "combined with" or "union" connector present
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
PERTURBATION_NAME = "operator_aggregate_variation"

KNOWN_TABLES  = set(SCHEMA.keys())
KNOWN_COLUMNS = {col for cols in SCHEMA.values() for col in cols}

TABLE_SYNONYMS = {
    "users":    {"users", "user", "members", "member", "accounts", "account", "people"},
    "posts":    {"posts", "post", "articles", "article", "entries", "entry"},
    "comments": {"comments", "comment", "replies", "reply", "feedback"},
    "likes":    {"likes", "like", "reactions", "reaction", "votes", "vote"},
    "follows":  {"follows", "follow", "connections", "connection", "subscriptions"},
}

UNION_CONNECTORS = {"combined with", "union", "along with"}

# Operator synonym sets (words that indicate a direction)
# Also include temporal expressions used for DATETIME comparisons
GT_WORDS = {"greater", "above", "exceeds", "exceed", "more", "higher", "least", "minimum",
            "since", "after", "on or after", "no less", "following",
            "onwards", "starting from", "onward", "from"}  # DATETIME >= patterns
LT_WORDS = {"less", "below", "under", "fewer", "lower", "most", "maximum",
            "before", "no more", "at most", "prior to", "up to", "on or before",
            "within", "preceding", "until", "no later", "through", "earlier than", "older than"}
EQ_WORDS = {"equals", "equal", "matches", "match", "is the same"}

# All known operator/aggregate synonyms (including temporal expressions)
ALL_OP_SYNONYMS = GT_WORDS | LT_WORDS | EQ_WORDS | {
    "not equal", "doesn't match", "different",
    "total number", "how many", "count of", "number of", "quantity of",
    "total", "sum of", "add up", "combined total",
    "average", "mean", "average value", "typical",
    "maximum", "highest", "largest", "biggest",
    "minimum", "lowest", "smallest", "least",
}

# Comparison operators in SQL — only the inequality / range operators trigger this perturbation.
# Plain equality (=) and inequality (<>) are NOT varied by the generator.
INEQ_OP = re.compile(r"(?<![<>!])([<>]=?)(?!=)(?!>)")  # < <= > >= but not = or <> or !=
AGG_FUNCS = re.compile(r"\b(COUNT|SUM|AVG|MAX|MIN)\s*\(", re.I)
# ISO date literal
ISO_DATE = re.compile(r"'\d{4}-\d{2}-\d{2}'")


def _has_comparison_or_agg(sql: str) -> bool:
    """True if SQL WHERE/HAVING/ON has an inequality operator (< <= > >=) or an aggregate."""
    if AGG_FUNCS.search(sql):
        return True
    # Check WHERE/HAVING clause
    where_parts = re.split(r"\bWHERE\b|\bHAVING\b", sql, flags=re.I)
    if len(where_parts) > 1:
        for clause in where_parts[1:]:
            clause_clean = re.sub(r"\(SELECT.+?\)", "", clause, flags=re.I | re.S)
            if INEQ_OP.search(clause_clean):
                return True
    # Also check ON clause (self-join comparisons)
    on_parts = re.split(r"\bON\b", sql, flags=re.I)
    if len(on_parts) > 1:
        for clause in on_parts[1:]:
            # Strip sub-selects and only look at ON part before next JOIN/WHERE
            on_clause = re.split(r"\bWHERE\b|\bJOIN\b|\bORDER\b", clause, flags=re.I)[0]
            if INEQ_OP.search(on_clause):
                return True
    return False


def _table_in_nl(table, nl_lower):
    for c in TABLE_SYNONYMS.get(table, {table}):
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest   = nl_lower[m.end():]
            before = nl_lower[:m.start()]
            if rest.startswith("_"): continue
            if c == "like" and re.match(r"\s*['\"%]", rest): continue
            if before.endswith("'") or rest.startswith("'"): continue
            return True
    return False


def _sql_literals(text):
    return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)


def _numbers(text):
    return re.findall(r"\b\d+\b", text)


def _get_pert(r):
    for sp in r.get("generated_perturbations", {}).get("single_perturbations", []):
        if sp.get("perturbation_name") == PERTURBATION_NAME:
            return sp
    return None


def _baseline(r):
    return r.get("generated_perturbations", {}).get("original", {}).get("nl_prompt", "")


def _complexity(sql):
    u = sql.upper().strip()
    if u.startswith("INSERT"): return "insert"
    if u.startswith("UPDATE"): return "update"
    if u.startswith("DELETE"): return "delete"
    if "UNION" in u: return "union"
    if "JOIN" in u: return "join"
    if "IN (SELECT" in u or "EXISTS" in u or "FROM (" in u: return "advanced"
    tables = re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", u)
    flat = [t for pair in tables for t in pair if t]
    if len(flat) >= 2 and len(set(flat)) == 1: return "advanced"
    return "simple"


class TestResult:
    def __init__(self, verbose=False):
        self.failures = []; self.passed = 0; self.verbose = verbose

    def ok(self, _): self.passed += 1

    def fail(self, rid, comp, check, detail):
        self.failures.append({"id": rid, "complexity": comp, "check": check, "detail": detail})
        if self.verbose: print(f"  ✗ [{comp} id={rid}] {check}: {detail}")

    def summary(self):
        total = self.passed + len(self.failures)
        lines = ["", "=" * 70, f"Perturbation Test: {PERTURBATION_NAME}", "=" * 70,
                 f"  Total checks : {total}", f"  Passed       : {self.passed}",
                 f"  Failed       : {len(self.failures)}"]
        if self.failures:
            lines.append("\nFailures by check:")
            by_check = defaultdict(list)
            for f in self.failures: by_check[f["check"]].append(f)
            for check, items in sorted(by_check.items()):
                lines.append(f"  [{len(items):3d}x] {check}")
                for item in items[:3]:
                    lines.append(f"        id={item['id']} [{item['complexity']}]: {item['detail'][:130]}")
                if len(items) > 3: lines.append(f"        ... and {len(items)-3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self): return len(self.failures) == 0


def check_record(r, comp, result):
    rid = r["id"]
    sql = r.get("sql", "")
    sp  = _get_pert(r)

    # 1. applicable_field_present
    if sp is None:
        result.fail(rid, comp, "applicable_field_present", "Perturbation entry missing")
        return
    result.ok("applicable_field_present")

    applicable = sp.get("applicable")
    # 2. applicable_is_bool
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool", f"Type is {type(applicable).__name__}")
        return
    result.ok("applicable_is_bool")

    baseline_nl = _baseline(r)
    base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    has_op_agg = _has_comparison_or_agg(sql)

    if not applicable:
        # 3. null_when_not_applicable
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt is {str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # 6. not_applicable_when_no_op
        if has_op_agg:
            result.fail(rid, comp, "not_applicable_when_no_op",
                        f"SQL has operator/agg but applicable=False: {sql[:80]}")
        else:
            result.ok("not_applicable_when_no_op")
        return

    # applicable = True
    # 4. string_when_applicable
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable",
                    "applicable=True but prompt missing or not a string")
        return
    result.ok("string_when_applicable")

    pert_l = perturbed.lower()

    # 5. applicable_when_has_operator
    if not has_op_agg:
        result.fail(rid, comp, "applicable_when_has_operator",
                    f"applicable=True but SQL lacks operator/agg: {sql[:80]}")
    else:
        result.ok("applicable_when_has_operator")

    # 7. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made",
                    "Perturbed == original (no operator/agg variation made)")
    else:
        result.ok("some_change_made")

    # 8. comparison_direction_preserved
    # Check for inequality operators (< <= > >=) in WHERE clause
    after_where_parts = re.split(r"\bWHERE\b|\bHAVING\b", sql, flags=re.I)
    if len(after_where_parts) > 1:
        where_clause = after_where_parts[-1]
        # Strip sub-selects
        where_clean = re.sub(r"\(SELECT.+?\)", "", where_clause, flags=re.I | re.S)
        has_gt  = bool(re.search(r"(?<![<>=!])>(?!=)", where_clean))
        has_gte = bool(re.search(r">=", where_clean))
        has_lt  = bool(re.search(r"(?<![<>=!])<(?!=)(?!>)", where_clean) and not re.search(r"<=", where_clean))
        has_lte = bool(re.search(r"<=", where_clean))
        if (has_gt or has_gte) and not any(w in pert_l for w in GT_WORDS):
            result.fail(rid, comp, "comparison_direction_preserved",
                        f"SQL has '>'/>=' but no GT/temporal synonym in perturbed: {perturbed[:120]}")
        elif (has_lt or has_lte) and not any(w in pert_l for w in LT_WORDS):
            result.fail(rid, comp, "comparison_direction_preserved",
                        f"SQL has '<'/'<=' but no LT/temporal synonym in perturbed: {perturbed[:120]}")
        else:
            result.ok("comparison_direction_preserved")

    # 9. operator_synonym_used
    if not any(syn in pert_l for syn in ALL_OP_SYNONYMS):
        result.fail(rid, comp, "operator_synonym_used",
                    f"No operator/aggregate synonym found in perturbed: {perturbed[:120]}")
    else:
        result.ok("operator_synonym_used")

    # 10. no_raw_iso_date_added
    orig_iso_dates = set(ISO_DATE.findall(baseline_nl))
    pert_iso_dates = set(ISO_DATE.findall(perturbed))
    new_dates = pert_iso_dates - orig_iso_dates
    if new_dates:
        result.fail(rid, comp, "no_raw_iso_date_added",
                    f"New ISO date(s) {new_dates} introduced in perturbed: {perturbed[:120]}")
    else:
        result.ok("no_raw_iso_date_added")

    # 11. columns_preserved
    for col in KNOWN_COLUMNS:
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' missing from perturbed: {perturbed[:120]}")
            break
    else:
        result.ok("columns_preserved")

    # 12. table_still_present
    tables_in_base = [t for t in KNOWN_TABLES if _table_in_nl(t, base_l)]
    if tables_in_base and not any(_table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present",
                    f"No schema table in perturbed: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 13. condition_values_preserved (exclude temporal values as those can be transformed)
    lits = [l for l in _sql_literals(baseline_nl) if not re.match(r"\d{4}-\d{2}-\d{2}", l)]
    nums = [n for n in _numbers(baseline_nl) if int(n) < 9999]  # exclude years
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

    # 14. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved",
                        f"Ordering cue lost: {perturbed[:120]}")
        else:
            result.ok("ordering_cue_preserved")

    # 15. no_object_repr
    for pat in [r"\[None\]", r"\bNone\b", r"Subquery\(", r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"Object repr '{pat}': {perturbed[:100]}")
            break
    else:
        result.ok("no_object_repr")

    # 16. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved",
                        f"Union connector absent: {perturbed[:120]}")
        else:
            result.ok("union_connector_preserved")


def run_tests(input_file, verbose=False):
    result = TestResult(verbose=verbose)
    with open(input_file) as f:
        dataset = json.load(f)
    print(f"Loaded {len(dataset)} records from {input_file}")
    print(f"Running tests for: {PERTURBATION_NAME}{'  (verbose)' if verbose else ''}\n")
    by_comp = defaultdict(int)
    for r in dataset:
        comp = _complexity(r["sql"])
        by_comp[comp] += 1
        check_record(r, comp, result)
    print("Record counts by complexity:")
    for c in ["simple","join","advanced","union","insert","update","delete"]:
        print(f"  {c:12s}: {by_comp.get(c,0)}")
    print()
    return result


def main():
    parser = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}' perturbations.")
    parser.add_argument("--input", "-i", default=str(ROOT / DEFAULT_INPUT_FILE))
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()
    result = run_tests(args.input, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)


if __name__ == "__main__":
    main()
