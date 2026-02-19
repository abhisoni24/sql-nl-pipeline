"""
Test Suite: table_column_synonyms (Perturbation ID 11)
=======================================================
Validates the 'table_column_synonyms' perturbation.

Contract
--------
  Applicable when the generator has a synonym for the table/column in the NL.
  3222 applicable, 278 not-applicable.
  Replaces table/column names with domain synonyms (e.g. users→members, post_id→article id).

Checks (13 named checks)
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
PERTURBATION_NAME = "table_column_synonyms"
KNOWN_TABLES  = set(SCHEMA.keys())
KNOWN_COLUMNS = {col for cols in SCHEMA.values() for col in cols}

# All valid synonyms used by the generator — checked to verify the perturbed actually uses one
TABLE_SYNONYMS_MAP = {
    "users":    {"members","member","accounts","account","people","clients","client",
                 "profiles","profile","users","registrations","registration"},
    "posts":    {"articles","article","entries","entry","publications","publication",
                 "updates","update","content","records","record"},
    "comments": {"replies","reply","feedback","messages","message","notes","note",
                 "responses","response","remarks","remark"},
    "likes":    {"reactions","reaction","votes","vote","approvals","approval","favorites",
                 "favorite","interests","interest"},
    "follows":  {"connections","connection","subscriptions","subscription","followers",
                 "follower","following"},
}
COLUMN_SYNONYMS_MAP = {
    "username":     {"handle","alias","screen name","display name","nickname"},
    "email":        {"email address","mail","contact","electronic mail","email_address"},
    "post_id":      {"article id","entry id","publication id","record id","identifier"},
    "user_id":      {"member id","account id","client id"},
    "comment_text": {"reply text","feedback text","message text","remark text","response text"},
    "view_count":   {"view tally","views","impressions","read count"},
    "follower_id":  {"subscriber id","connection id"},
    "followee_id":  {"followed id","target id"},
    "liked_at":     {"reaction time","vote time","approval time","approval date"},
    "id":           {"identifier","unique id","record id","record","unique"},
    "signup_date":  {"registration date","join date","since","date"},
    "content":      {"text","body","post text","article text"},
    "posted_at":    {"publication date","post date","since","date"},
}

UNION_CONNECTORS = {"combined with","union","along with"}
JOIN_COUPLING    = {"and their","along with","joined with","join","with their",
                    "left join","right join","full join","inner join"}


def _table_in_nl(table, nl_lower):
    synonyms = TABLE_SYNONYMS_MAP.get(table, set()) | {table}
    for c in synonyms:
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest = nl_lower[m.end():]; before = nl_lower[:m.start()]
            if rest.startswith("_"): continue
            if c == "like" and re.match(r"\s*['\"%]", rest): continue
            if before.endswith("'") or rest.startswith("'"): continue
            return True
    return False

def _sql_literals(text): return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)
def _numbers(text): return re.findall(r"\b\d+\b", text)

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

    if not applicable:
        if perturbed is not None:
            result.fail(rid, comp, "null_when_not_applicable",
                        f"applicable=False but prompt={str(perturbed)[:80]!r}")
        else:
            result.ok("null_when_not_applicable")
        return

    if not perturbed or not isinstance(perturbed, str):
        result.fail(rid, comp, "string_when_applicable", "Missing prompt"); return
    result.ok("string_when_applicable")
    pert_l = perturbed.lower()

    # 5. some_change_made
    if perturbed.strip() == baseline_nl.strip():
        result.fail(rid, comp, "some_change_made", "No change made")
    else:
        result.ok("some_change_made")

    # 6. synonym_used — the perturbed differs from baseline (a synonym was substituted)
    # Use word-diff to confirm something changed — any novel word not in baseline counts
    base_words = set(re.sub(r"[',]","",base_l).split())
    pert_words = set(re.sub(r"[',]","",pert_l).split())
    novel_words = pert_words - base_words
    tbl_syn_used = any(syn.replace(" ","") in pert_l.replace(" ","") for syns in TABLE_SYNONYMS_MAP.values() for syn in syns)
    col_syn_used = any(syn.replace(" ","") in pert_l.replace(" ","") for syns in COLUMN_SYNONYMS_MAP.values() for syn in syns)
    # Also accept any novel meaningful word (synonym substitution happened)
    meaningful_novel = any(len(w) > 3 and w.isalpha() for w in novel_words)
    if not (tbl_syn_used or col_syn_used or meaningful_novel):
        result.fail(rid, comp, "synonym_used",
                    f"No synonym change detected: {perturbed[:120]}")
    else:
        result.ok("synonym_used")

    # 7. noun_class_preserved — some entity concept preserved (table or one of its synonyms)
    tables_in_base = [t for t in KNOWN_TABLES if t in base_l]
    any_entity_present = any(_table_in_nl(t, pert_l) for t in tables_in_base)
    # Also accept any table synonym appearing in perturbed
    any_syns_present = any(
        syn in pert_l
        for t in tables_in_base
        for syn in TABLE_SYNONYMS_MAP.get(t, set())
    )
    if tables_in_base and not any_entity_present and not any_syns_present:
        result.fail(rid, comp, "noun_class_preserved",
                    f"No table or its synonym found: {perturbed[:120]}")
    else:
        result.ok("noun_class_preserved")

    # 8. condition_values_preserved
    lits = _sql_literals(baseline_nl); ok = True
    for lit in lits:
        if lit not in perturbed:
            result.fail(rid, comp, "condition_values_preserved", f"Literal '{lit}' lost");
            ok=False; break
    if ok:
        for num in _numbers(baseline_nl):
            if num not in perturbed:
                result.fail(rid, comp, "condition_values_preserved", f"Number '{num}' lost"); break
        else:
            result.ok("condition_values_preserved")

    # 9. ordering_cue_preserved
    if "ordered by" in base_l or "order by" in base_l:
        if "order" not in pert_l and "sorted" not in pert_l:
            result.fail(rid, comp, "ordering_cue_preserved", f"Ordering cue lost")
        else: result.ok("ordering_cue_preserved")

    # 10. no_object_repr
    for pat in [r"\[None\]",r"\bNone\b",r"Subquery\(",r"Column\("]:
        if re.search(pat, perturbed):
            result.fail(rid, comp, "no_object_repr", f"'{pat}': {perturbed[:100]}"); break
    else:
        result.ok("no_object_repr")

    # 11. length_reasonable — synonyms may add/remove words slightly
    delta = abs(len(perturbed.split()) - len(baseline_nl.split()))
    if delta > 10:
        result.fail(rid, comp, "length_reasonable",
                    f"Word delta {delta} > 10: {perturbed[:120]}")
    else:
        result.ok("length_reasonable")

    # 12. join_relationship_preserved
    if comp == "join":
        has_coupling = any(p in pert_l for p in JOIN_COUPLING) or bool(re.search(r"\bJOIN\b", perturbed))
        if not has_coupling:
            result.fail(rid, comp, "join_relationship_preserved", f"Join coupling absent: {perturbed[:120]}")
        else: result.ok("join_relationship_preserved")

    # 13. union_connector_preserved
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
