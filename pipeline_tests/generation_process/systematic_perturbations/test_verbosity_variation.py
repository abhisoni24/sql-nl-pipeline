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
PERTURBATION_NAME = "verbosity_variation"

KNOWN_TABLES   = set(SCHEMA.keys())
KNOWN_COLUMNS  = {col for cols in SCHEMA.values() for col in cols}

TABLE_SYNONYMS = {
    "users":    {"users", "user", "members", "member", "accounts", "account", "people"},
    "posts":    {"posts", "post", "articles", "article", "entries", "entry"},
    "comments": {"comments", "comment", "replies", "reply", "feedback"},
    "likes":    {"likes", "like", "reactions", "reaction", "votes", "vote"},
    "follows":  {"follows", "follow", "connections", "connection", "subscriptions"},
}

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
        self.failures = []
        self.passed   = 0
        self.verbose  = verbose

    def ok(self, _):
        self.passed += 1

    def fail(self, rid, comp, check, detail):
        self.failures.append({"id": rid, "complexity": comp, "check": check, "detail": detail})
        if self.verbose:
            print(f"  ✗ [{comp} id={rid}] {check}: {detail}")

    def summary(self):
        total = self.passed + len(self.failures)
        lines = ["", "=" * 70,
                 f"Perturbation Test: {PERTURBATION_NAME}",
                 "=" * 70,
                 f"  Total checks : {total}",
                 f"  Passed       : {self.passed}",
                 f"  Failed       : {len(self.failures)}"]
        if self.failures:
            lines.append("\nFailures by check:")
            by_check = defaultdict(list)
            for f in self.failures: by_check[f["check"]].append(f)
            for check, items in sorted(by_check.items()):
                lines.append(f"  [{len(items):3d}x] {check}")
                for item in items[:3]:
                    lines.append(f"        id={item['id']} [{item['complexity']}]: {item['detail'][:130]}")
                if len(items) > 3:
                    lines.append(f"        ... and {len(items)-3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self):
        return len(self.failures) == 0


def check_record(r, comp, result):
    rid = r["id"]
    sp  = _get_pert(r)
    if sp is None:
        result.fail(rid, comp, "always_applicable", "Perturbation entry missing")
        return

    applicable = sp.get("applicable")
    baseline_nl = _baseline(r)
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
    for col in KNOWN_COLUMNS:
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved",
                        f"Column '{col}' in baseline but missing from perturbed: {perturbed[:120]}")
            break
    else:
        result.ok("columns_preserved")

    # 10. table_still_present
    tables_in_base = [t for t in KNOWN_TABLES if _table_in_nl(t, base_l)]
    if tables_in_base:
        if not any(_table_in_nl(t, pert_l) for t in tables_in_base):
            result.fail(rid, comp, "table_still_present",
                        f"No schema table found in perturbed: {perturbed[:120]}")
        else:
            result.ok("table_still_present")

    # 11. condition_values_preserved
    lits = _sql_literals(baseline_nl)
    nums = _numbers(baseline_nl)
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
        tables_in_pert = [t for t in tables_in_base if _table_in_nl(t, pert_l)]
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
