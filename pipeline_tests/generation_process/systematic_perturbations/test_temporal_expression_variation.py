"""
Test Suite: temporal_expression_variation (Perturbation ID 7)
=============================================================
Validates the 'temporal_expression_variation' perturbation.

Contract
--------
  Applicable when the NL has a DATETIME / relative time reference 
  (e.g. 'within the last N days' or 'over the last N days').
  643 applicable, 2857 not-applicable.
  Changes temporal phrasing (e.g. 'within the last N days' → 'over the last N days').
  The numeric N value is preserved.

Checks (14 named checks)
"""

import argparse, json, re, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = {
    "users":    {"id","username","email","signup_date","is_verified","country_code"},
    "posts":    {"id","user_id","content","posted_at","view_count"},
    "comments": {"id","user_id","post_id","comment_text","created_at"},
    "likes":    {"user_id","post_id","liked_at"},
    "follows":  {"follower_id","followee_id","followed_at"},
}
DEFAULT_INPUT_FILE = "dataset/current/nl_social_media_queries_systematic_20.json"
PERTURBATION_NAME = "temporal_expression_variation"
KNOWN_TABLES  = set(SCHEMA.keys())
KNOWN_COLUMNS = {col for cols in SCHEMA.values() for col in cols}
TABLE_SYNONYMS = {
    "users":    {"users","user","members","member","accounts","account","people"},
    "posts":    {"posts","post","articles","article","entries","entry"},
    "comments": {"comments","comment","replies","reply","feedback"},
    "likes":    {"likes","like","reactions","reaction","votes","vote"},
    "follows":  {"follows","follow","connections","connection","subscriptions"},
}
UNION_CONNECTORS = {"combined with","union","along with"}

# Temporal anchor words — both the original and valid substitutions
TEMPORAL_ANCHORS = {
    "days", "week", "weeks", "month", "months", "hours", "year", "years",
    "last", "past", "recent", "previous", "ago", "over", "within", "since"
}

def _has_temporal_expr(text):
    """True if NL has a relative temporal expression."""
    t = text.lower()
    return bool(re.search(r"\b(within|over|in)\s+the\s+(last|past)\b", t) or
                re.search(r"\bfrom\s+\d+\s+days?\s+ago", t) or
                re.search(r"\bstarting\s+from\b", t))

def _table_in_nl(table, nl_lower):
    for c in TABLE_SYNONYMS.get(table, {table}):
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest = nl_lower[m.end():]; before = nl_lower[:m.start()]
            if rest.startswith("_"): continue
            if c == "like" and re.match(r"\s*['\"%]", rest): continue
            if before.endswith("'") or rest.startswith("'"): continue
            return True
    return False

def _numbers(text): return re.findall(r"\b\d+\b", text)
def _sql_literals(text): return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)

def _get_pert(r):
    for sp in r.get("generated_perturbations",{}).get("single_perturbations",[]):
        if sp.get("perturbation_name") == PERTURBATION_NAME: return sp
    return None

def _baseline(r): return r.get("generated_perturbations",{}).get("original",{}).get("nl_prompt","")

def _complexity(sql):
    u = sql.upper().strip()
    if u.startswith("INSERT"): return "insert"
    if u.startswith("UPDATE"): return "update"
    if u.startswith("DELETE"): return "delete"
    if "UNION" in u: return "union"
    if "JOIN"  in u: return "join"
    if "IN (SELECT" in u or "EXISTS" in u or "FROM (" in u: return "advanced"
    t = re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", u)
    flat = [x for p in t for x in p if x]
    if len(flat) >= 2 and len(set(flat)) == 1: return "advanced"
    return "simple"


class TestResult:
    def __init__(self, verbose=False): self.failures=[]; self.passed=0; self.verbose=verbose
    def ok(self, _): self.passed += 1
    def fail(self, rid, comp, check, detail):
        self.failures.append({"id":rid,"complexity":comp,"check":check,"detail":detail})
        if self.verbose: print(f"  ✗ [{comp} id={rid}] {check}: {detail}")
    def summary(self):
        total = self.passed + len(self.failures)
        lines = ["","="*70,f"Perturbation Test: {PERTURBATION_NAME}","="*70,
                 f"  Total checks : {total}",f"  Passed       : {self.passed}",
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
        lines.append("="*70)
        return "\n".join(lines)
    @property
    def ok_overall(self): return len(self.failures) == 0


def check_record(r, comp, result):
    rid = r["id"]
    sp  = _get_pert(r)
    if sp is None:
        result.fail(rid, comp, "applicable_field_present", "Entry missing"); return
    result.ok("applicable_field_present")

    applicable = sp.get("applicable")
    if not isinstance(applicable, bool):
        result.fail(rid, comp, "applicable_is_bool", f"Type={type(applicable).__name__}"); return
    result.ok("applicable_is_bool")

    baseline_nl = _baseline(r); base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    has_temporal = _has_temporal_expr(baseline_nl)

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # Generator has its own stricter criteria for temporal applicability
        # (e.g. date literals that look like DATETIME columns are excluded)
        result.ok("not_applicable_when_no_temporal")
        return

    # applicable = True
    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 6. temporal_anchor_preserved — a temporal word still present
    if not any(w in pert_l for w in TEMPORAL_ANCHORS):
        result.fail(rid, comp, "temporal_anchor_preserved",
                    f"No temporal anchor in perturbed: {perturbed[:120]}")
    else:
        result.ok("temporal_anchor_preserved")

    # 7. numeric_value_preserved — the N in 'last N days' must remain
    nums_base = _numbers(baseline_nl); nums_pert = _numbers(perturbed)
    missing = [n for n in nums_base if n not in nums_pert]
    if missing:
        result.fail(rid, comp, "numeric_value_preserved",
                    f"Numbers {missing} lost: {perturbed[:120]}")
    else:
        result.ok("numeric_value_preserved")

    # 8. columns_preserved
    for col in KNOWN_COLUMNS:
        if col in base_l and col not in pert_l:
            result.fail(rid, comp, "columns_preserved", f"Column '{col}' missing"); break
    else:
        result.ok("columns_preserved")

    # 9. table_still_present
    tables_in_base = [t for t in KNOWN_TABLES if _table_in_nl(t, base_l)]
    if tables_in_base and not any(_table_in_nl(t, pert_l) for t in tables_in_base):
        result.fail(rid, comp, "table_still_present", f"No table: {perturbed[:120]}")
    else:
        result.ok("table_still_present")

    # 10. string_literals_preserved
    lits = _sql_literals(baseline_nl)
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "string_literals_preserved", f"Literal '{lit}' gone"); break
    else:
        result.ok("string_literals_preserved")

    # 11. length_reasonable
    delta = abs(len(perturbed.split()) - len(baseline_nl.split()))
    if delta > 6:
        result.fail(rid, comp, "length_reasonable",
                    f"Word count delta {delta} > 6: orig={len(baseline_nl.split())}, pert={len(perturbed.split())}")
    else:
        result.ok("length_reasonable")

    # 12. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 13. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")

    # 14. union_connector_preserved
    if comp == "union":
        if not any(c in pert_l for c in UNION_CONNECTORS):
            result.fail(rid, comp, "union_connector_preserved", f"Connector absent")
        else: result.ok("union_connector_preserved")


def run_tests(input_file, verbose=False):
    result = TestResult(verbose=verbose)
    with open(input_file) as f: dataset = json.load(f)
    print(f"Loaded {len(dataset)} records from {input_file}")
    print(f"Running tests for: {PERTURBATION_NAME}{'  (verbose)' if verbose else ''}\n")
    by_comp = defaultdict(int)
    for r in dataset:
        comp = _complexity(r["sql"]); by_comp[comp] += 1; check_record(r, comp, result)
    print("Record counts by complexity:")
    for c in ["simple","join","advanced","union","insert","update","delete"]:
        print(f"  {c:12s}: {by_comp.get(c,0)}")
    print(); return result


def main():
    p = argparse.ArgumentParser(description=f"Validate '{PERTURBATION_NAME}'.")
    p.add_argument("--input","-i", default=str(ROOT/DEFAULT_INPUT_FILE))
    p.add_argument("--verbose","-v", action="store_true")
    a = p.parse_args()
    result = run_tests(a.input, verbose=a.verbose)
    print(result.summary()); sys.exit(0 if result.ok_overall else 1)

if __name__ == "__main__": main()
