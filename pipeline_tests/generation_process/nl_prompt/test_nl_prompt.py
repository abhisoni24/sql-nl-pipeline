"""
NL Prompt Test Suite (Baseline / Vanilla Rendering)
=====================================================
Validates the nl_prompt field produced by src/core/nl_renderer.py (baseline
config — no perturbations active) against the paired SQL query in the dataset.

Usage
-----
  # Against the default file defined below:
  python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py

  # Against any specific file:
  python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py \\
      --input dataset/social_media/nl_prompts.json

  # Verbose — print every failure as it occurs:
  python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py -v

What is tested (55 named checks)
---------------------------------
STRUCTURAL (all complexities)
  1.  nl_prompt key present and non-empty string
  2.  No raw Python object repr ([None], <Class>, Subquery(, Column(, etc.)
  3.  No stray newline / tab characters inside the prompt
  4.  Intent verb present at start (get/select/show/display/find/… or INSERT-family for DML)
  5.  Prompt does not start with a SQL keyword (SELECT/FROM/WHERE/INSERT/UPDATE/DELETE)
      — unless MIXED_SQL_NL is intended (baseline should not contain them)
  6.  No bare 'None' token in the NL prompt
  7.  Prompt references at least one schema table name (or synonym recognised by the renderer)
  8.  Length sanity: not shorter than 10 chars, not longer than 1000 chars

FIDELITY — COLUMN COVERAGE (all SELECT-based complexities)
  9.  If SQL selects *, the NL says "all columns" (or equivalent synonym)
  10. If SQL selects specific columns, each column name or schema-synonym appears
      somewhere in the NL (checks that no selected column is silently dropped)
      — note: alias-qualified names like "the user's email" expand col name only

WHERE CLAUSE COVERAGE (all complexities)
  11. If the SQL has a WHERE/condition, the NL contains at least one filtering
      indicator word (where/filtered/looking only/for which/that have/matches/etc.)
  12. If the SQL has no WHERE, the NL must NOT contain a false filter claim

ORDERING / LIMIT COVERAGE (SELECT-based)
  13. If SQL has ORDER BY, NL contains "ordered by" or "order"
  14. If SQL has LIMIT, NL contains "limited to" or "limit"

── SIMPLE ──
  15. NL references the single from-table name (exact or synonym)
  16. NL does NOT contain JOIN/UNION/FROM (unexpected SQL keyword leakage)
  17. NL does not mention a second table name from schema (single-table query only)

── JOIN ──
  18. NL contains both joined table names (or recognised synonyms)
  19. For INNER JOIN on standard FK: NL uses "and their" (expected template) OR
      contains the target table name in a natural coupling phrase
  20. For LEFT JOIN: NL mentions "left" or "if any" or "along with" (signals optionality)
      OR the target table is present (weaker check acceptable for complex cases)
  21. For RIGHT JOIN / FULL JOIN: NL either preserves "RIGHT"/"FULL" keyword literal
      OR explicitly describes both sides (verifies SQL meaning is preserved)
  22. JOIN NL must NOT omit the right-hand table entirely
  23. JOIN NL must reference at least one FK column name (confirms ON clause was read,
      except for standard FK "and their" template which implicitly encodes the FK)
  24. JOIN NL must NOT appear to be a simple single-table statement (no join mention)

── ADVANCED ──
  Subtype auto-detected (same heuristic as SQL test suite).

  subquery_where (25–31):
  25. NL contains "matches any" or "is in" — signals IN semantics
  26. NL mentions the outer table
  27. NL mentions the inner (subquery) table
  28. NL retains the inner WHERE condition (filter word present)
  29. NL does NOT contain raw "[None]" or "(unknown)"
  30. NL does NOT expose internal alias prefix "sub_" as a bare table or column
      identifier without context (e.g. "sub_u.id" alone with no table around it
      is acceptable, but purely "sub_u" as a table name is an artifact)

  subquery_from (31–36):
  31. NL mentions the source table of the inner query
  32. NL does NOT say "derived_table" literally (internal alias must be suppressed)
  33. NL does NOT say "inner_" prefix literally (e.g. "inner_users")
  34. NL does NOT say "a derived query" alone without the actual table name
  35. Outer WHERE condition (if any) is reflected in the NL

  self_join (36–40):
  36. NL mentions the joined table (must appear at least once)
  37. NL does NOT produce a plain "all columns … from X" with no join indication
      (must contain either "JOIN" keyword literal, or the table mentioned twice,
       or a structural comparison phrase like "same", "each other", "pair")
  38. NL does NOT claim to filter by a condition that isn't in the SQL

  exists_subquery (39–44):
  39. NL contains existence language: "there is", "corresponding", "exists", "no"
  40. NOT EXISTS should produce negation: "no corresponding", "not", "without"
  41. NL mentions the table used in the EXISTS subquery
  42. NL retains the correlated condition (inner WHERE present in NL)

── UNION ──
  43. NL contains "combined with" (expected connector template)
  44. For UNION (distinct): NL contains "removing duplicates"
  45. For UNION ALL: NL contains "including duplicates"
  46. Both halves of the UNION appear in the NL (the connector splits them)
  47. Both legs' table names appear in the NL
  48. If either leg has ORDER BY, "ordered by" appears in NL
  49. NL does not claim the two legs are separate independent queries
      (must be joined by the union connector)

── INSERT ──
  50. NL contains an insert action verb (insert/add/create/put/store)
  51. NL mentions the target table or a recognised singular/plural synonym
  52. NL mentions each inserted column name
  53. NL does NOT contain SQL VALUES keyword (should be natural language)

── UPDATE ──
  54. NL contains an update action verb (update/modify/change/set/alter)
  55. NL mentions the SET column name
  56. NL mentions the updated value (or "current time" for DATETIME('now'))
  57. NL reflects the WHERE condition (filter word present — UPDATE always has WHERE)
  58. NL does NOT contain raw SQL SET keyword

── DELETE ──
  59. NL contains a delete action verb (delete/remove/erase/drop)
  60. NL mentions the target table
  61. NL reflects the WHERE condition (filter word present — DELETE always has WHERE)
  62. NL does NOT contain raw SQL WHERE keyword literally
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from sqlglot import exp, parse_one, errors as sqlglot_errors

# ── Schema loading (configurable via --schema CLI arg) ────────────────────
_SCHEMA_STATE = {
    "SCHEMA": {},
    "FOREIGN_KEYS": {},
    "KNOWN_TABLES": set(),
    "DIALECT": "sqlite",
}


def _load_schema(schema_path=None):
    """Load schema from YAML or fall back to legacy schema.py."""
    if schema_path:
        from src.core.schema_loader import load_from_yaml
        cfg = load_from_yaml(schema_path)
        _SCHEMA_STATE["SCHEMA"] = cfg.get_legacy_schema()
        _SCHEMA_STATE["FOREIGN_KEYS"] = cfg.get_fk_pairs()
        _SCHEMA_STATE["DIALECT"] = cfg.dialect
    else:
        from src.core.schema import SCHEMA, FOREIGN_KEYS, USED_SQL_DIALECT
        _SCHEMA_STATE["SCHEMA"] = SCHEMA
        _SCHEMA_STATE["FOREIGN_KEYS"] = FOREIGN_KEYS
        _SCHEMA_STATE["DIALECT"] = USED_SQL_DIALECT
    _SCHEMA_STATE["KNOWN_TABLES"] = set(_SCHEMA_STATE["SCHEMA"].keys())

DEFAULT_INPUT_FILE = "dataset/social_media/nl_prompts.json"
# ── NL vocabulary maps ─────────────────────────────────────────────────────

# Intent verbs used by the renderer at the start of SELECT prompts
# (exhaustive list from renderer's _choose_word / random.choice vocabulary)
SELECT_INTENT_VERBS = {
    "get", "show", "select", "retrieve", "find", "display", "fetch",
    "list", "pull", "read", "pick", "return", "look", "bring", "go",
    "produce", "dig", "spot", "choose", "identify", "give",
    "grab", "extract", "gather", "report",
    # additional renderer variants observed in dataset
    "single",   # "Single out …"
    "run",      # "Run a check for …"
    "query",
    "output",
    "show",
    "seek",
}

INSERT_INTENT_VERBS = {
    "insert", "add", "create", "put", "store", "record", "log", "save",
    "include", "enter",
}

UPDATE_INTENT_VERBS = {
    "update", "modify", "change", "set", "alter", "edit", "revise",
    "adjust", "correct", "amend",
}

DELETE_INTENT_VERBS = {
    "delete", "remove", "erase", "drop", "purge", "eliminate",
    "clear", "discard", "strip", "wipe",
}

# Words that confirm a WHERE/filter is present in the NL
FILTER_INDICATORS = {
    "where", "for which", "filtered", "looking only", "that have",
    "that is", "matches", "equals", "greater", "less", "older",
    "within the last", "in the past", "over the last", "like '",
    "is in [", "is not", "not equal",
}

# Table synonyms used by the renderer.
# Base set (social_media) — extended at runtime when a dictionary YAML is loaded.
TABLE_SYNONYMS = {
    "users": {"users", "user", "members", "member", "accounts", "account", "people"},
    "posts": {"posts", "post", "articles", "article", "entries", "entry"},
    "comments": {"comments", "comment", "replies", "reply", "feedback"},
    "likes": {"likes", "like", "reactions", "reaction", "votes", "vote"},
    "follows": {"follows", "follow", "connections", "connection", "subscriptions"},
}

# Column synonyms — populated by _load_dictionary()
# Key: qualified "table.column", Value: set of synonym strings
COLUMN_SYNONYMS: dict[str, set[str]] = {}

# Reverse: any synonym → canonical table name
SYNONYM_TO_TABLE = {}


def _rebuild_synonym_to_table():
    """Rebuild the reverse index after TABLE_SYNONYMS is modified."""
    SYNONYM_TO_TABLE.clear()
    for canonical, syns in TABLE_SYNONYMS.items():
        for s in syns:
            SYNONYM_TO_TABLE[s] = canonical


_rebuild_synonym_to_table()


def _load_dictionary(dict_path: str):
    """Merge table & column synonyms from a dictionary YAML into the test lookups."""
    import yaml
    with open(dict_path) as f:
        data = yaml.safe_load(f)

    # Merge table synonyms
    for tname, syns in data.get("table_synonyms", {}).items():
        existing = TABLE_SYNONYMS.get(tname, {tname})
        TABLE_SYNONYMS[tname] = existing | set(syns)
    _rebuild_synonym_to_table()

    # Load column synonyms
    for qualified_col, syns in data.get("column_synonyms", {}).items():
        COLUMN_SYNONYMS[qualified_col] = set(syns)

# Words that indicate successful rendering of union connector
UNION_DISTINCT_INDICATOR = "removing duplicates"
UNION_ALL_INDICATOR = "including duplicates"
UNION_CONNECTOR = "combined with"

# SQL keywords that must NOT appear raw in a baseline NL prompt
FORBIDDEN_SQL_KEYWORDS_IN_NL = {
    # DQL
    "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING",
    # DML (structural keywords)
    "VALUES", "SET",
}

# ── Helpers ────────────────────────────────────────────────────────────────

def _nl_lower(r: dict) -> str:
    return r.get("nl_prompt", "").lower()


def _has_any(text: str, words: set | list) -> bool:
    tl = text.lower()
    return any(w.lower() in tl for w in words)


def _has_filter(nl: str) -> bool:
    nl_lower = nl.lower()
    return any(ind in nl_lower for ind in FILTER_INDICATORS)


def _col_in_nl(col: str, nl_lower: str, table: str = None) -> bool:
    """Return True if a column name (or any of its dictionary synonyms) appears in the NL.

    Checks:
      1. The raw column name (with underscores) as it may appear verbatim
      2. The column name with underscores replaced by spaces (e.g. 'user_id' → 'user id')
      3. All synonyms from the COLUMN_SYNONYMS dictionary
    """
    col_raw = col.lower()
    col_spaced = col_raw.replace("_", " ")
    if col_raw in nl_lower or col_spaced in nl_lower:
        return True

    # Check dictionary synonyms for this column
    qualified_keys = []
    if table:
        qualified_keys.append(f"{table}.{col}")
    # Also try all tables that have this column
    for qk in COLUMN_SYNONYMS:
        if qk.endswith(f".{col}"):
            qualified_keys.append(qk)
    for qk in qualified_keys:
        for syn in COLUMN_SYNONYMS.get(qk, set()):
            if syn.lower() in nl_lower:
                return True
    return False


def _table_in_nl(table: str, nl_lower: str) -> bool:
    """Return True if the table name or any of its synonyms appear in the NL as a
    genuine table reference (not as part of a column name compound or string literal).

    Avoids false positives such as:
      - 'user' inside 'user_id'         (column name, not table reference)
      - 'user' inside "'user'"           (string literal value, not table reference)
      - 'like' followed by a quote/percent (SQL LIKE operator pattern)
      - 'follow' inside 'followed_at'   (column fragment)
      - 'account' inside 'account followed id' (column synonym containing a table synonym)
    """
    candidates = TABLE_SYNONYMS.get(table, {table})
    # Auto-expand: add underscore variants and simple singulars.
    # The NL renderer may emit "research_project" (singular + underscore) while
    # the schema table is "research_projects" and the dictionary only has
    # "research projects" (space-separated, plural).
    expanded = set()
    for c in candidates:
        expanded.add(c)
        # Underscore ↔ space variants
        if "_" in c:
            expanded.add(c.replace("_", " "))
        elif " " in c:
            expanded.add(c.replace(" ", "_"))
        # Naive singular: strip trailing 's' (catches tables/projects/enrollments...)
        if c.endswith("s") and len(c) > 2:
            singular = c[:-1]
            expanded.add(singular)
            if "_" in singular:
                expanded.add(singular.replace("_", " "))
            elif " " in singular:
                expanded.add(singular.replace(" ", "_"))
    for c in expanded:
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            start, end = m.start(), m.end()
            rest  = nl_lower[end:]
            before = nl_lower[:start]

            # Reject: synonym is a column name prefix (followed by underscore: user_id)
            if rest.startswith("_"):
                continue

            # Reject: LIKE operator (synonym 'like' followed by quote/percent pattern)
            if c == "like" and re.match(r"\s*['\"%]", rest):
                continue

            # Reject: string literal value (synonym surrounded by quotes)
            if before.endswith("'") or rest.startswith("'"):
                continue

            # Reject: match is embedded in a column synonym phrase
            if _match_inside_column_synonym(c, nl_lower, start, end):
                continue

            # Reject: match is inside a longer table synonym for a *different* table
            if _match_inside_other_table_synonym(table, c, nl_lower, start, end):
                continue

            # This match is a genuine table reference
            return True
    return False


def _match_inside_other_table_synonym(table: str, word: str, nl_lower: str,
                                       start: int, end: int) -> bool:
    """Check if a word match is part of a longer multi-word table synonym for a different table."""
    for other_table, syns in TABLE_SYNONYMS.items():
        if other_table == table:
            continue
        for syn in syns:
            syn_l = syn.lower()
            # Only check multi-word synonyms that contain the matched word
            if " " not in syn_l or word not in syn_l:
                continue
            idx = nl_lower.find(syn_l)
            while idx != -1:
                if idx <= start and idx + len(syn_l) >= end:
                    return True
                idx = nl_lower.find(syn_l, idx + 1)
    return False


def _match_inside_column_synonym(word: str, nl_lower: str, start: int, end: int) -> bool:
    """Check if a word match at [start:end] is part of a longer column synonym phrase."""
    if not COLUMN_SYNONYMS:
        return False
    for syns in COLUMN_SYNONYMS.values():
        for syn in syns:
            syn_l = syn.lower()
            if word not in syn_l or syn_l == word:
                continue
            # See if this column synonym phrase is present in the NL around the match
            idx = nl_lower.find(syn_l)
            while idx != -1:
                if idx <= start and idx + len(syn_l) >= end:
                    return True
                idx = nl_lower.find(syn_l, idx + 1)
    return False


def _detect_advanced_subtype(sql: str) -> str:
    sql_u = sql.upper()
    if "IN (SELECT" in sql_u:
        return "subquery_where"
    if "EXISTS" in sql_u and "IN (SELECT" not in sql_u:
        return "exists_subquery"
    if "FROM (" in sql_u:
        return "subquery_from"
    # self_join: same table appears more than once in table list
    try:
        ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
        tables = [t.name for t in ast.find_all(exp.Table)]
        if len(tables) >= 2 and len(set(tables)) == 1:
            return "self_join"
    except Exception:
        pass
    return "unknown"


def _is_union_all(sql: str) -> bool:
    return "UNION ALL" in sql.upper()


def _sql_has_order(sql: str) -> bool:
    return "ORDER BY" in sql.upper()


def _sql_has_limit(sql: str) -> bool:
    return " LIMIT " in sql.upper()


def _sql_has_star(sql: str) -> bool:
    # SELECT *, not inside a subquery (simplified: just check top-level)
    return bool(re.search(r"SELECT\s+\*", sql, re.IGNORECASE))


def _selected_columns(sql: str) -> list[str]:
    """Extract bare column names referenced in SELECT list (not *)."""
    try:
        ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
        sel = ast if isinstance(ast, exp.Select) else None
        if sel is None:
            return []
        cols = []
        for e in sel.expressions:
            if isinstance(e, exp.Column):
                cols.append(e.name)
            elif isinstance(e, exp.Alias) and isinstance(e.this, exp.Column):
                cols.append(e.this.name)
        return cols
    except Exception:
        return []


def _join_info(sql: str):
    """Return (left_table, right_table, join_kind, join_side) or None."""
    try:
        ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
        if not isinstance(ast, exp.Select):
            return None
        joins = ast.args.get("joins", [])
        if not joins:
            return None
        jn = joins[0]
        from_node = ast.args.get("from_")
        left_table = from_node.this.this.name if from_node else ""
        right_table = jn.this.this.name if hasattr(jn.this, "this") else ""
        join_side = (jn.side or "").upper()
        join_kind = (jn.kind or "").upper()
        return left_table, right_table, join_kind, join_side
    except Exception:
        return None


# ── Result collector ───────────────────────────────────────────────────────

class TestResult:
    def __init__(self, verbose: bool = False):
        self.failures: list[dict] = []
        self.passed = 0
        self.verbose = verbose

    def ok(self, check: str):
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
            "NL Prompt Test Results (Baseline / Vanilla Rendering)",
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
                    detail = item["detail"][:130]
                    lines.append(f"        id={item['id']}: {detail}")
                if len(items) > 3:
                    lines.append(f"        ... and {len(items)-3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self) -> bool:
        return len(self.failures) == 0


# ── Check functions ────────────────────────────────────────────────────────

def check_structural(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    nl = r.get("nl_prompt", "")

    # 1. nl_prompt present and non-empty
    if not nl or not isinstance(nl, str):
        result.fail(rid, comp, "nl_present", "nl_prompt missing or not a string")
        return False
    result.ok("nl_present")
    nl_l = nl.lower()

    # 2. No Python object repr
    obj_patterns = [r"\[None\]", r"\bNone\b", r"Subquery\(", r"Column\(", r"<[A-Z][a-z]"]
    for pat in obj_patterns:
        if re.search(pat, nl):
            result.fail(rid, comp, "no_object_repr", f"Pattern '{pat}' found: {nl[:100]}")
            return False
    result.ok("no_object_repr")

    # 3. No stray whitespace control chars
    if re.search(r"[\t\n\r]", nl):
        result.fail(rid, comp, "no_control_chars", f"Newline/tab found in: {repr(nl[:80])}")
    else:
        result.ok("no_control_chars")

    # 4. Intent verb or DML verb at start
    first_word = nl_l.split()[0] if nl_l.split() else ""
    all_intent_verbs = (SELECT_INTENT_VERBS | INSERT_INTENT_VERBS |
                        UPDATE_INTENT_VERBS | DELETE_INTENT_VERBS)
    if first_word not in all_intent_verbs:
        result.fail(rid, comp, "intent_verb_at_start",
                    f"First word '{first_word}' is not a known intent verb: {nl[:80]}")
    else:
        result.ok("intent_verb_at_start")

    # 5. No raw uppercase SQL keyword at the very start.
    # Exception: DML complexities correctly begin with the action word (delete, update, insert),
    # and SELECT-type prompts may begin with "select" as a natural language intent word.
    # Flag only if the first word is a structural-only SQL keyword that has no NL meaning.
    first_word_upper = nl.split()[0].upper() if nl.split() else ""
    structural_only_sql_kws = {"FROM", "WHERE", "JOIN", "GROUP", "HAVING"}
    if first_word_upper in structural_only_sql_kws:
        result.fail(rid, comp, "no_raw_sql_start",
                    f"NL starts with structural SQL keyword '{first_word_upper}'")
    else:
        result.ok("no_raw_sql_start")

    # 6. No bare 'None' token
    if re.search(r"\bNone\b", nl):
        result.fail(rid, comp, "no_none_token", f"Bare 'None' in NL: {nl[:100]}")
    else:
        result.ok("no_none_token")

    # 7. Prompt references at least one schema table (name or synonym)
    mentions_table = any(_table_in_nl(t, nl_l) for t in _SCHEMA_STATE["KNOWN_TABLES"])
    if not mentions_table:
        result.fail(rid, comp, "mentions_a_table",
                    f"No schema table mentioned in: {nl[:100]}")
    else:
        result.ok("mentions_a_table")

    # 8. Length sanity
    if len(nl) < 10:
        result.fail(rid, comp, "length_sanity", f"NL too short ({len(nl)} chars): {nl!r}")
    elif len(nl) > 1000:
        result.fail(rid, comp, "length_sanity", f"NL too long ({len(nl)} chars)")
    else:
        result.ok("length_sanity")

    return True  # structural checks passed; caller may proceed to type-specific


def check_select_fidelity(r: dict, result: TestResult):
    """Checks 9–14 apply to all SELECT-based complexities."""
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()

    # 9. SELECT * → "all columns"
    if _sql_has_star(sql):
        if "all columns" not in nl_l and "all of the columns" not in nl_l:
            result.fail(rid, comp, "star_maps_to_all_columns",
                        f"SQL has SELECT * but NL lacks 'all columns': {nl[:120]}")
        else:
            result.ok("star_maps_to_all_columns")

    # 10. Specific columns present in NL
    else:
        cols = _selected_columns(sql)
        # Try to determine the source table for better synonym lookup
        table_match = re.search(r"FROM\s+(\w+)", sql, re.IGNORECASE)
        source_table = table_match.group(1) if table_match else None
        for col in cols:
            # Column names can appear as "the user's email", "email", "the like's liked_at"
            if not _col_in_nl(col, nl_l, source_table):
                result.fail(rid, comp, "column_name_in_nl",
                            f"Column '{col}' not found in NL: {nl[:120]}")
            else:
                result.ok("column_name_in_nl")

    # 11. WHERE → filter indicator in NL
    sql_has_where = "WHERE" in sql.upper()
    if sql_has_where:
        if not _has_filter(nl):
            result.fail(rid, comp, "where_reflected_in_nl",
                        f"SQL has WHERE but NL has no filter indicator: {nl[:120]}")
        else:
            result.ok("where_reflected_in_nl")

    # 12. No WHERE → no false filter in NL
    else:
        # Only flag if NL contains "where" appearing as a filter phrase
        # Acceptable false positives to skip: "somewhere", "wherever", self-join ON phrases
        #   (self-join ON conditions use "equals"/"greater"/"less" but that is not a WHERE)
        if re.search(r"\bwhere\b", nl_l) and not re.search(r"some\s*where|where\s*ever", nl_l):
            result.fail(rid, comp, "no_false_where",
                        f"SQL has no WHERE but NL contains 'where': {nl[:120]}")
        else:
            result.ok("no_false_where")

    # 13. ORDER BY → "ordered" in NL
    if _sql_has_order(sql):
        if "ordered" not in nl_l and "order" not in nl_l:
            result.fail(rid, comp, "order_reflected_in_nl",
                        f"SQL has ORDER BY but NL lacks ordering mention: {nl[:120]}")
        else:
            result.ok("order_reflected_in_nl")

    # 14. LIMIT → "limited to" in NL
    if _sql_has_limit(sql):
        if "limited to" not in nl_l and "limit" not in nl_l:
            result.fail(rid, comp, "limit_reflected_in_nl",
                        f"SQL has LIMIT but NL lacks limit mention: {nl[:120]}")
        else:
            result.ok("limit_reflected_in_nl")


def check_simple(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()

    # Determine single table
    try:
        ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
        tables = list({t.name for t in ast.find_all(exp.Table)})
    except Exception:
        return

    # 15. table mentioned in NL
    if tables:
        if not _table_in_nl(tables[0], nl_l):
            result.fail(rid, comp, "simple_table_in_nl",
                        f"Table '{tables[0]}' not mentioned in: {nl[:120]}")
        else:
            result.ok("simple_table_in_nl")

    # 16. No SQL structural keyword leakage
    # 'from' is a valid English preposition used by the renderer (e.g. "from likes");
    # only flag JOIN and UNION which have no natural NL equivalents here.
    for kw in ("JOIN", "UNION"):
        if re.search(rf"\b{kw}\b", nl, re.IGNORECASE):
            result.fail(rid, comp, "simple_no_sql_kw_leakage",
                        f"SQL keyword '{kw}' leaked into simple NL: {nl[:120]}")
        else:
            result.ok("simple_no_sql_kw_leakage")

    # 17. No second schema table mentioned (simple should only reference one table)
    other_tables = _SCHEMA_STATE["KNOWN_TABLES"] - {tables[0]} if tables else _SCHEMA_STATE["KNOWN_TABLES"]
    for other in other_tables:
        if _table_in_nl(other, nl_l):
            result.fail(rid, comp, "simple_only_one_table",
                        f"Second table '{other}' (or synonym) appears in simple NL: {nl[:120]}")
            return  # one failure is enough
    result.ok("simple_only_one_table")


def check_join(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()
    info = _join_info(sql)
    if not info:
        result.fail(rid, comp, "join_parse_ok", "Could not parse join info from SQL")
        return
    left_table, right_table, join_kind, join_side = info

    # 18. Both tables mentioned
    for tbl in [left_table, right_table]:
        if not _table_in_nl(tbl, nl_l):
            result.fail(rid, comp, "join_both_tables_in_nl",
                        f"Table '{tbl}' missing from join NL: {nl[:120]}")
        else:
            result.ok("join_both_tables_in_nl")

    # 19. INNER JOIN on FK → "and their" or coupling phrase
    is_inner = (join_kind == "INNER" and not join_side) or (not join_kind and not join_side)
    fk_pair = (left_table, right_table)
    rev_pair = (right_table, left_table)
    is_standard_fk = fk_pair in _SCHEMA_STATE["FOREIGN_KEYS"] or rev_pair in _SCHEMA_STATE["FOREIGN_KEYS"]

    if is_inner and is_standard_fk:
        coupling_words = ["and their", "with their", "along with", "joined with", "join"]
        if not any(w in nl_l for w in coupling_words):
            result.fail(rid, comp, "inner_join_fk_coupling",
                        f"INNER JOIN FK pair but no coupling phrase in NL: {nl[:120]}")
        else:
            result.ok("inner_join_fk_coupling")

    # 20. LEFT JOIN → signals optionality
    if join_side == "LEFT":
        left_indicators = ["left", "if any", "along with", "left join", "along with their"]
        if not any(w in nl_l for w in left_indicators):
            result.fail(rid, comp, "left_join_optionality_signal",
                        f"LEFT JOIN but no optionality signal in NL: {nl[:120]}")
        else:
            result.ok("left_join_optionality_signal")

    # 21. RIGHT/FULL JOIN → keyword preserved OR both sides explicitly described
    if join_side in ("RIGHT", "FULL"):
        has_kw = join_side.lower() in nl_l or "full" in nl_l
        has_both = _table_in_nl(left_table, nl_l) and _table_in_nl(right_table, nl_l)
        if not (has_kw or has_both):
            result.fail(rid, comp, "right_full_join_preserved",
                        f"{join_side} JOIN: neither keyword nor both tables in NL: {nl[:120]}")
        else:
            result.ok("right_full_join_preserved")

    # 22. Right-hand table NOT omitted entirely
    right_syns = TABLE_SYNONYMS.get(right_table, {right_table})
    if not any(s in nl_l for s in right_syns):
        result.fail(rid, comp, "join_right_table_present",
                    f"Right-hand table '{right_table}' entirely absent from NL: {nl[:120]}")
    else:
        result.ok("join_right_table_present")

    # 23. FK column referenced in NL (applies when ON clause is NOT suppressed by standard FK template)
    # Standard FK template "and their X" encodes FK implicitly — skip column check for those
    if not (is_inner and is_standard_fk and "and their" in nl_l):
        left_key, right_key = "", ""
        if fk_pair in _SCHEMA_STATE["FOREIGN_KEYS"]:
            left_key, right_key = _SCHEMA_STATE["FOREIGN_KEYS"][fk_pair]
        elif rev_pair in _SCHEMA_STATE["FOREIGN_KEYS"]:
            left_key, right_key = _SCHEMA_STATE["FOREIGN_KEYS"][rev_pair]
        if left_key and right_key:
            if left_key not in nl_l and right_key not in nl_l:
                result.fail(rid, comp, "join_fk_col_in_nl",
                            f"Neither FK col ('{left_key}','{right_key}') in NL: {nl[:120]}")
            else:
                result.ok("join_fk_col_in_nl")

    # 24. NL doesn't look like a plain single-table statement
    join_signal_words = ["join", "and their", "along with", "right", "full", "left join"]
    if not any(w in nl_l for w in join_signal_words):
        result.fail(rid, comp, "join_has_join_signal",
                    f"NL has no join signal word — looks like single table: {nl[:120]}")
    else:
        result.ok("join_has_join_signal")


def check_advanced(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()
    subtype = _detect_advanced_subtype(sql)

    if subtype == "unknown":
        result.fail(rid, comp, "advanced_subtype_detected",
                    f"Cannot detect subtype: {sql[:80]}")
        return
    result.ok("advanced_subtype_detected")

    # ── subquery_where ──────────────────────────────────────────────────────
    if subtype == "subquery_where":
        # 25. IN semantics expressed
        in_words = ["matches any", "is in", "match any", "belongs to", "included in"]
        if not any(w in nl_l for w in in_words):
            result.fail(rid, comp, "subq_where_in_semantics",
                        f"No IN semantics phrase in NL: {nl[:120]}")
        else:
            result.ok("subq_where_in_semantics")

        # 26. Outer table mentioned
        try:
            ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
            from_node = ast.args.get("from_")
            outer_table = from_node.this.this.name if from_node else ""
        except Exception:
            outer_table = ""
        if outer_table and not _table_in_nl(outer_table, nl_l):
            result.fail(rid, comp, "subq_where_outer_table",
                        f"Outer table '{outer_table}' missing from NL: {nl[:120]}")
        else:
            result.ok("subq_where_outer_table")

        # 27. Inner (subquery) table mentioned
        inner_table_match = re.search(
            r"IN\s*\(SELECT.*?FROM\s+(\w+)", sql, re.IGNORECASE
        )
        if inner_table_match:
            inner_table = inner_table_match.group(1).rstrip(")")
            # strip alias suffix (e.g. "likes AS sub_l" → "likes")
            inner_table = inner_table.split()[0]
            if inner_table in _SCHEMA_STATE["KNOWN_TABLES"] and not _table_in_nl(inner_table, nl_l):
                result.fail(rid, comp, "subq_where_inner_table",
                            f"Inner table '{inner_table}' missing from NL: {nl[:120]}")
            else:
                result.ok("subq_where_inner_table")

        # 28. Inner WHERE condition retained
        # If inner SELECT has a WHERE, the NL should mention "where" somewhere
        inner_where_match = re.search(
            r"IN\s*\(SELECT.*?WHERE\s+.+?\)", sql, re.IGNORECASE | re.DOTALL
        )
        if inner_where_match:
            if not _has_filter(nl):
                result.fail(rid, comp, "subq_where_inner_where_retained",
                            f"Inner WHERE not reflected in NL: {nl[:120]}")
            else:
                result.ok("subq_where_inner_where_retained")

        # 29. No [None] or (unknown)
        if "[None]" in nl or "(unknown)" in nl:
            result.fail(rid, comp, "subq_where_no_none",
                        f"[None] or (unknown) in NL: {nl[:120]}")
        else:
            result.ok("subq_where_no_none")

        # 30. "sub_" prefix not appearing as a lone table identifier
        # Acceptable: "sub_u.id" (column context) — NOT acceptable: table name "sub_u" alone
        lone_sub_alias = re.search(r"\bsub_\w+\b(?!\.\w)", nl_l)
        if lone_sub_alias:
            result.fail(rid, comp, "subq_where_no_lone_sub_alias",
                        f"Lone sub_ alias '{lone_sub_alias.group()}' in NL: {nl[:120]}")
        else:
            result.ok("subq_where_no_lone_sub_alias")

    # ── subquery_from ────────────────────────────────────────────────────────
    elif subtype == "subquery_from":
        # 31. Source table mentioned
        inner_table_match = re.search(
            r"FROM\s*\(\s*SELECT.*?FROM\s+(\w+)", sql, re.IGNORECASE
        )
        if inner_table_match:
            inner_table = inner_table_match.group(1)
            if inner_table in _SCHEMA_STATE["KNOWN_TABLES"] and not _table_in_nl(inner_table, nl_l):
                result.fail(rid, comp, "subq_from_source_table",
                            f"Source table '{inner_table}' missing from NL: {nl[:120]}")
            else:
                result.ok("subq_from_source_table")

        # 32. "derived_table" not in NL
        if "derived_table" in nl_l:
            result.fail(rid, comp, "subq_from_no_derived_table_literal",
                        f"Internal alias 'derived_table' leaked into NL: {nl[:120]}")
        else:
            result.ok("subq_from_no_derived_table_literal")

        # 33. "inner_" prefix not in NL
        if re.search(r"\binner_", nl_l):
            result.fail(rid, comp, "subq_from_no_inner_prefix",
                        f"Internal 'inner_' prefix leaked into NL: {nl[:120]}")
        else:
            result.ok("subq_from_no_inner_prefix")

        # 34. NL does not say ONLY "a derived query" without naming the table
        if "a derived query" in nl_l and not any(_table_in_nl(t, nl_l) for t in _SCHEMA_STATE["KNOWN_TABLES"]):
            result.fail(rid, comp, "subq_from_no_bare_derived_query",
                        f"NL says 'a derived query' with no table name: {nl[:120]}")
        else:
            result.ok("subq_from_no_bare_derived_query")

        # 35. Outer WHERE condition reflected (if present)
        # Outer WHERE is after the derived table closing paren
        outer_where = re.search(r"\)\s+AS\s+\w+\s+WHERE\s+.+", sql, re.IGNORECASE)
        if outer_where and not _has_filter(nl):
            result.fail(rid, comp, "subq_from_outer_where_reflected",
                        f"Outer WHERE not reflected in NL: {nl[:120]}")
        elif outer_where:
            result.ok("subq_from_outer_where_reflected")

    # ── self_join ────────────────────────────────────────────────────────────
    elif subtype == "self_join":
        # 36. Joined table mentioned
        try:
            ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
            tables = [t.name for t in ast.find_all(exp.Table)]
            base_table = tables[0] if tables else ""
        except Exception:
            base_table = ""

        if base_table and not _table_in_nl(base_table, nl_l):
            result.fail(rid, comp, "self_join_table_in_nl",
                        f"Self-join table '{base_table}' not in NL: {nl[:120]}")
        else:
            result.ok("self_join_table_in_nl")

        # 37. NL signals a join (not a plain single-table SELECT)
        join_signals = ["join", "and their", "same", "both", "pair", "each other", "twice"]
        if not any(w in nl_l for w in join_signals):
            result.fail(rid, comp, "self_join_has_structural_signal",
                        f"No self-join structural signal in NL: {nl[:120]}")
        else:
            result.ok("self_join_has_structural_signal")

        # 38. No false WHERE condition — if SQL has no WHERE, the NL must not claim a filter.
        # Self-join ON clauses use "equals"/"less"/"greater" in the NL for the JOIN condition;
        # those are expected and do NOT constitute a false WHERE. Only flag the literal word
        # "where" (meaning a WHERE clause was falsely introduced).
        sql_has_where = "WHERE" in sql.upper()
        if not sql_has_where and re.search(r"\bwhere\b", nl_l):
            result.fail(rid, comp, "self_join_no_false_filter",
                        f"SQL has no WHERE but NL contains 'where': {nl[:120]}")
        else:
            result.ok("self_join_no_false_filter")

    # ── exists_subquery ──────────────────────────────────────────────────────
    elif subtype == "exists_subquery":
        # 39. Existence language present
        exist_words = ["there is", "corresponding", "exists", "exist", "no corresponding",
                       "no match", "without a"]
        if not any(w in nl_l for w in exist_words):
            result.fail(rid, comp, "exists_existence_language",
                        f"No existence language in NL: {nl[:120]}")
        else:
            result.ok("exists_existence_language")

        # 40. NOT EXISTS → negation in NL
        if "NOT EXISTS" in sql.upper():
            negation_words = ["no corresponding", "not", "without", "no match", "exclud"]
            if not any(w in nl_l for w in negation_words):
                result.fail(rid, comp, "exists_not_negated",
                            f"NOT EXISTS but no negation in NL: {nl[:120]}")
            else:
                result.ok("exists_not_negated")

        # 41. EXISTS subquery table mentioned
        exists_table_match = re.search(
            r"EXISTS\s*\(\s*SELECT.*?FROM\s+(\w+)", sql, re.IGNORECASE
        )
        if exists_table_match:
            exists_table = exists_table_match.group(1)
            if exists_table in _SCHEMA_STATE["KNOWN_TABLES"] and not _table_in_nl(exists_table, nl_l):
                result.fail(rid, comp, "exists_subquery_table_in_nl",
                            f"EXISTS table '{exists_table}' not in NL: {nl[:120]}")
            else:
                result.ok("exists_subquery_table_in_nl")

        # 42. Correlated condition reflected (EXISTS inner WHERE)
        inner_where_match = re.search(
            r"EXISTS\s*\(SELECT.*?WHERE\s+.+?\)", sql, re.IGNORECASE | re.DOTALL
        )
        if inner_where_match and not _has_filter(nl):
            result.fail(rid, comp, "exists_inner_condition_reflected",
                        f"EXISTS inner WHERE not reflected in NL: {nl[:120]}")
        elif inner_where_match:
            result.ok("exists_inner_condition_reflected")


def check_union(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()
    is_all = _is_union_all(sql)

    # 43. "combined with" connector present
    if UNION_CONNECTOR not in nl_l:
        result.fail(rid, comp, "union_connector_present",
                    f"'combined with' not in union NL: {nl[:150]}")
    else:
        result.ok("union_connector_present")

    # 44. UNION (distinct) → "removing duplicates"
    if not is_all:
        if UNION_DISTINCT_INDICATOR not in nl_l:
            result.fail(rid, comp, "union_distinct_indicator",
                        f"UNION (distinct) but no 'removing duplicates' in NL: {nl[:150]}")
        else:
            result.ok("union_distinct_indicator")

    # 45. UNION ALL → "including duplicates"
    if is_all:
        if UNION_ALL_INDICATOR not in nl_l:
            result.fail(rid, comp, "union_all_indicator",
                        f"UNION ALL but no 'including duplicates' in NL: {nl[:150]}")
        else:
            result.ok("union_all_indicator")

    # 46. Both halves present (connector splits the NL into two meaningful parts)
    if UNION_CONNECTOR in nl_l:
        parts = nl_l.split(UNION_CONNECTOR, 1)
        if len(parts) == 2:
            left_part, right_part = parts
            if len(left_part.strip()) < 5 or len(right_part.strip()) < 5:
                result.fail(rid, comp, "union_both_halves_present",
                            f"One side of union NL is near-empty: {nl[:150]}")
            else:
                result.ok("union_both_halves_present")

    # 47. Both legs' table names in NL
    try:
        ast = parse_one(sql, dialect=_SCHEMA_STATE["DIALECT"])
        if isinstance(ast, exp.Union):
            left_tables = {t.name for t in ast.left.find_all(exp.Table)} if ast.left else set()
            right_tables = {t.name for t in ast.right.find_all(exp.Table)} if ast.right else set()
            for tbl in (left_tables | right_tables):
                if tbl and not _table_in_nl(tbl, nl_l):
                    result.fail(rid, comp, "union_tables_in_nl",
                                f"Table '{tbl}' not in union NL: {nl[:150]}")
                elif tbl:
                    result.ok("union_tables_in_nl")
    except Exception:
        pass

    # 48. ORDER BY reflected
    if _sql_has_order(sql):
        if "ordered" not in nl_l and "order" not in nl_l:
            result.fail(rid, comp, "union_order_reflected",
                        f"UNION has ORDER BY but NL lacks ordering mention: {nl[:150]}")
        else:
            result.ok("union_order_reflected")

    # 49. NL is joined (not two independent-looking sentences)
    sentence_split = re.split(r"(?<=[.!?])\s+(?=[A-Z])", nl)
    if len(sentence_split) > 1:
        result.fail(rid, comp, "union_is_single_joined_nl",
                    f"Union NL appears to be {len(sentence_split)} separate sentences: {nl[:150]}")
    else:
        result.ok("union_is_single_joined_nl")


def check_insert(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()

    # 50. Insert verb
    first_word = nl_l.split()[0] if nl_l.split() else ""
    if first_word not in INSERT_INTENT_VERBS:
        result.fail(rid, comp, "insert_verb",
                    f"First word '{first_word}' not an insert verb: {nl[:100]}")
    else:
        result.ok("insert_verb")

    # 51. Target table mentioned
    table_match = re.search(r"INSERT\s+INTO\s+(\w+)", sql, re.IGNORECASE)
    if table_match:
        tbl = table_match.group(1)
        if not _table_in_nl(tbl, nl_l):
            result.fail(rid, comp, "insert_table_in_nl",
                        f"Insert target '{tbl}' not in NL: {nl[:100]}")
        else:
            result.ok("insert_table_in_nl")

    # 52. Inserted column names present in NL
    cols_match = re.search(r"INSERT\s+INTO\s+\w+\s*\(([^)]+)\)", sql, re.IGNORECASE)
    if cols_match:
        insert_table_match = re.search(r"INSERT\s+INTO\s+(\w+)", sql, re.IGNORECASE)
        insert_table = insert_table_match.group(1) if insert_table_match else None
        col_names = [c.strip() for c in cols_match.group(1).split(",")]
        for col in col_names:
            if not _col_in_nl(col, nl_l, insert_table):
                result.fail(rid, comp, "insert_column_in_nl",
                            f"Insert column '{col}' not in NL: {nl[:120]}")
            else:
                result.ok("insert_column_in_nl")

    # 53. No raw VALUES keyword
    if re.search(r"\bVALUES\b", nl, re.IGNORECASE):
        result.fail(rid, comp, "insert_no_values_kw",
                    f"Raw VALUES keyword in insert NL: {nl[:100]}")
    else:
        result.ok("insert_no_values_kw")


def check_update(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()

    # 54. Update verb
    first_word = nl_l.split()[0] if nl_l.split() else ""
    if first_word not in UPDATE_INTENT_VERBS:
        result.fail(rid, comp, "update_verb",
                    f"First word '{first_word}' not an update verb: {nl[:100]}")
    else:
        result.ok("update_verb")

    # 55. SET column mentioned
    set_match = re.search(r"SET\s+(\w+)\s*=", sql, re.IGNORECASE)
    if set_match:
        col = set_match.group(1)
        update_table_match = re.search(r"UPDATE\s+(\w+)", sql, re.IGNORECASE)
        update_table = update_table_match.group(1) if update_table_match else None
        if not _col_in_nl(col, nl_l, update_table):
            result.fail(rid, comp, "update_set_col_in_nl",
                        f"SET column '{col}' not in NL: {nl[:100]}")
        else:
            result.ok("update_set_col_in_nl")

    # 56. Updated value reflected
    # For DATETIME('now'), renderer outputs "the current time"
    if "DATETIME('now')" in sql:
        if "current time" not in nl_l:
            result.fail(rid, comp, "update_value_in_nl",
                        f"DATETIME('now') not rendered as 'current time' in NL: {nl[:100]}")
        else:
            result.ok("update_value_in_nl")
    else:
        # At least some value expression in the NL
        val_match = re.search(r"SET\s+\w+\s*=\s*(.+?)\s+WHERE", sql, re.IGNORECASE)
        if val_match:
            val_str = val_match.group(1).strip().strip("'").lower()
            if val_str and val_str not in nl_l:
                result.fail(rid, comp, "update_value_in_nl",
                            f"Updated value '{val_str}' not in NL: {nl[:100]}")
            else:
                result.ok("update_value_in_nl")

    # 57. WHERE condition reflected (UPDATE always has WHERE)
    if not _has_filter(nl):
        result.fail(rid, comp, "update_where_reflected",
                    f"UPDATE WHERE not reflected in NL: {nl[:100]}")
    else:
        result.ok("update_where_reflected")

    # 58. No raw SET keyword
    if re.search(r"\bSET\b", nl):  # case-sensitive: "SET" as uppercase raw kw
        result.fail(rid, comp, "update_no_raw_set_kw",
                    f"Raw 'SET' keyword in update NL: {nl[:100]}")
    else:
        result.ok("update_no_raw_set_kw")


def check_delete(r: dict, result: TestResult):
    rid, comp = r["id"], r["complexity"]
    sql, nl = r["sql"], r["nl_prompt"]
    nl_l = nl.lower()

    # 59. Delete verb
    first_word = nl_l.split()[0] if nl_l.split() else ""
    if first_word not in DELETE_INTENT_VERBS:
        result.fail(rid, comp, "delete_verb",
                    f"First word '{first_word}' not a delete verb: {nl[:100]}")
    else:
        result.ok("delete_verb")

    # 60. Target table mentioned
    table_match = re.search(r"DELETE\s+FROM\s+(\w+)", sql, re.IGNORECASE)
    if table_match:
        tbl = table_match.group(1)
        if not _table_in_nl(tbl, nl_l):
            result.fail(rid, comp, "delete_table_in_nl",
                        f"Delete target '{tbl}' not in NL: {nl[:100]}")
        else:
            result.ok("delete_table_in_nl")

    # 61. WHERE condition reflected (DELETE always has WHERE)
    if not _has_filter(nl):
        result.fail(rid, comp, "delete_where_reflected",
                    f"DELETE WHERE not reflected in NL: {nl[:100]}")
    else:
        result.ok("delete_where_reflected")

    # 62. No raw WHERE keyword in NL
    if re.search(r"\bWHERE\b", nl):
        result.fail(rid, comp, "delete_no_raw_where_kw",
                    f"Raw 'WHERE' keyword in delete NL: {nl[:100]}")
    else:
        result.ok("delete_no_raw_where_kw")


# ── Dispatch ───────────────────────────────────────────────────────────────

SELECT_COMPLEXITIES = {"simple", "join", "advanced", "union"}
TYPE_CHECKERS = {
    "simple":   check_simple,
    "join":     check_join,
    "advanced": check_advanced,
    "union":    check_union,
    "insert":   check_insert,
    "update":   check_update,
    "delete":   check_delete,
}


def run_tests(input_file: str, verbose: bool = False) -> TestResult:
    result = TestResult(verbose=verbose)

    with open(input_file) as f:
        dataset = json.load(f)

    # Support both bare-list and metadata-wrapped formats
    if isinstance(dataset, dict) and "records" in dataset:
        records = dataset["records"]
    else:
        records = dataset

    print(f"Loaded {len(records)} records from {input_file}")
    print(f"Running NL prompt tests{'  (verbose)' if verbose else ''}...\n")

    by_complexity: dict[str, int] = defaultdict(int)

    for r in records:
        comp = r.get("complexity", "unknown")
        by_complexity[comp] += 1

        # Structural checks — if they fail skip type-specific
        if not check_structural(r, result):
            continue

        # SELECT-based fidelity checks
        if comp in SELECT_COMPLEXITIES:
            check_select_fidelity(r, result)

        # Type-specific checks
        checker = TYPE_CHECKERS.get(comp)
        if checker:
            checker(r, result)

    print("Record counts by complexity:")
    for c in ["simple", "join", "advanced", "union", "insert", "update", "delete"]:
        print(f"  {c:12s}: {by_complexity.get(c, 0)}")
    print()

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Validate NL prompts generated by src/core/nl_renderer.py"
    )
    parser.add_argument(
        "--input", "-i",
        default=str(ROOT / DEFAULT_INPUT_FILE),
        help=f"Path to the NL prompt JSON dataset file (default: {DEFAULT_INPUT_FILE})"
    )
    parser.add_argument(
        "--schema", "-s",
        default=None,
        help="Path to a YAML schema file (default: use legacy src/core/schema.py)"
    )
    parser.add_argument(
        "--dictionary", "-d",
        default=None,
        help="Path to a dictionary YAML to load table/column synonyms from"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print each failure as it occurs"
    )
    args = parser.parse_args()

    # Load schema before running tests
    _load_schema(args.schema)

    # Load dictionary synonyms if provided
    if args.dictionary:
        _load_dictionary(args.dictionary)
        print(f"Loaded dictionary synonyms from {args.dictionary}")

    result = run_tests(args.input, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)


if __name__ == "__main__":
    main()
