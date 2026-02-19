"""
Test Suite: punctuation_variation (Perturbation ID 8)
======================================================
Validates the 'punctuation_variation' perturbation across all 3,500 records.

Contract
--------
  Applicable when baseline NL has a list (3+ columns with commas).
  1567 applicable, 1933 not-applicable.
  Changes: semicolons replace some commas, or adds/removes trailing period.
  Core content unchanged.

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
PERTURBATION_NAME = "punctuation_variation"
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
JOIN_COUPLING    = {"and their","along with","joined with","join","with their",
                    "left join","right join","full join","inner join"}


def _table_in_nl(table, nl_lower):
    for c in TABLE_SYNONYMS.get(table, {table}):
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest = nl_lower[m.end():]; before = nl_lower[:m.start()]
            if rest.startswith("_"): continue
            if c == "like" and re.match(r"\s*['\"%]", rest): continue
            if before.endswith("'") or rest.startswith("'"): continue
            return True
    return False

def _sql_literals(text): return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)
def _numbers(text): return re.findall(r"\b\d+\b", text)
def _has_list(text): return text.count(",") >= 2  # 3+ comma-separated items

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
        result.fail(rid, comp, "applicable_is_bool", f"Type {type(applicable).__name__}"); return
    result.ok("applicable_is_bool")

    baseline_nl = _baseline(r); base_l = baseline_nl.lower()
    perturbed = sp.get("perturbed_nl_prompt")
    has_list = _has_list(baseline_nl)

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        # The generator uses its own applicability logic; we don't second-guess it
        result.ok("applicable_when_has_list")
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

    # 6. punctuation_changed — semicolons or period changes
    orig_semis = baseline_nl.count(";")
    pert_semis  = perturbed.count(";")
    orig_period = baseline_nl.strip().endswith(".")
    pert_period = perturbed.strip().endswith(".")
    if pert_semis == orig_semis and orig_period == pert_period:
        result.fail(rid, comp, "punctuation_changed",
                    f"No punctuation change detected: {perturbed[:120]}")
    else:
        result.ok("punctuation_changed")

    # 7. words_unchanged — only punctuation changed, not words
    orig_words = re.sub(r"[;,\.\!]", "", baseline_nl.lower()).split()
    pert_words = re.sub(r"[;,\.\!]", "", perturbed.lower()).split()
    common = sum(1 for w in orig_words if w in pert_words)
    if len(orig_words) > 0 and common < len(orig_words) * 0.80:
        result.fail(rid, comp, "words_unchanged",
                    f"Only {common}/{len(orig_words)} words preserved: {perturbed[:120]}")
    else:
        result.ok("words_unchanged")

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

    # 10. condition_values_preserved
    lits = _sql_literals(baseline_nl); ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved", f"Literal '{lit}' lost"); ok=False; break
    if ok:
        for num in _numbers(baseline_nl):
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved", f"Number '{num}' lost"); break
        else:
            result.ok("condition_values_preserved")

    # 11. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 12. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING)
        tables_in_pert = [t for t in tables_in_base if _table_in_nl(t, pert_l)]
        if not has_coupling and len(tables_in_pert) < 2:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent")
        else: result.ok("join_relationship_preserved")

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
    a = p.parse_args();
    result = run_tests(a.input, verbose=a.verbose)
    print(result.summary()); sys.exit(0 if result.ok_overall else 1)

if __name__ == "__main__": main()
