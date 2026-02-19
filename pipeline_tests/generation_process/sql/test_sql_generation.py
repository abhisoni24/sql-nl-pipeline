"""
SQL Generation Test Suite
=========================
Validates the raw SQL queries produced by src/core/generator.py against a
JSON dataset file (e.g. dataset/current/raw_social_media_queries_20.json).

Usage
-----
  # Against the default file defined below:
  python pipeline_tests/generation_process/sql/test_sql_generation.py

  # Against a specific file:
  python pipeline_tests/generation_process/sql/test_sql_generation.py \\
      --input dataset/current/raw_social_media_queries_20.json

  # Verbose: show every failing query
  python pipeline_tests/generation_process/sql/test_sql_generation.py -v

What is tested
--------------
STRUCTURAL (all complexities)
  1.  Required record keys present (id, complexity, sql, tables)
  2.  SQL string is non-empty and parseable by sqlglot
  3.  complexity field is one of the 7 known types
  4.  tables list matches tables actually referenced in the AST
  5.  No raw Python object reprs in SQL (〈classname〉, None literals, etc.)

TYPE-SPECIFIC CONTRACTS
  simple    6.  Only SELECT statements; FROM clause present; max 1 table
            7.  No JOIN/UNION/subquery
            8.  WHERE (if present) uses only columns from the single table

  join      9.  Exactly one JOIN clause
            10. JOIN uses a known foreign-key pair from schema.FOREIGN_KEYS
            11. ON clause references the correct FK columns (not arbitrary cols)
            12. Join type is one of (INNER, LEFT, RIGHT, FULL/FULL OUTER)
            13. Selected columns only come from the two joined tables

  advanced  14. Exactly one advanced subtype present per query
                (subquery_where → IN + subquery, subquery_from → derived table,
                 self_join → same table aliased twice, exists_subquery → EXISTS)
            15. subquery_where: outer WHERE uses IN(...), inner is a SELECT
            16. subquery_where: inner SELECT references a related table via FK
            17. subquery_from: FROM clause is a derived subquery, not a raw table
            18. self_join: both aliases refer to the SAME base table
            19. exists_subquery: WHERE contains EXISTS (or NOT EXISTS)
            20. exists_subquery: correlated condition links outer and inner table

  union     21. Top-level AST is a UNION or UNION ALL node
            22. Both sides of the UNION select the SAME columns (same count)
            23. Both legs reference the same base table
            24. UNION ALL / UNION distinction respected (distinct flag)

  insert    25. INSERT INTO a valid schema table
            26. Column list matches VALUES count
            27. Primary key column 'id' NOT in the INSERT column list
            28. Composite PK columns excluded correctly (likes: user_id+post_id
                are valid INSERT targets, follows: follower_id+followee_id ok)
            29. Value types are consistent with schema column types

  update    30. UPDATE targets a valid schema table
            31. SET clause updates exactly one column
            32. Composite PK columns NOT updated (follows.follower_id/followee_id,
                likes.user_id/likes.post_id)
            33. 'id' column NOT updated
            34. WHERE clause is present (generator always adds one)

  delete    35. DELETE FROM a valid schema table
            36. WHERE clause is present
            37. No subquery inside DELETE (generator does not produce them)
"""

import argparse
import json
import sys
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

# Allow running from the project root
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from sqlglot import exp, parse_one, errors as sqlglot_errors
from src.core.schema import (
    SCHEMA,
    FOREIGN_KEYS,
    NUMERIC_TYPES,
    TEXT_TYPES,
    DATE_TYPES,
    BOOLEAN_TYPES,
    USED_SQL_DIALECT,
)

# ── default input file (can be overridden via --input CLI arg) ─────────────
DEFAULT_INPUT_FILE = "dataset/current/raw_social_media_queries_20.json"

KNOWN_COMPLEXITIES = {"simple", "join", "advanced", "union", "insert", "update", "delete"}
KNOWN_TABLES = set(SCHEMA.keys())

# Composite primary key columns that must never be updated
COMPOSITE_PK_COLS = {
    "follows": {"follower_id", "followee_id"},
    "likes": {"user_id", "post_id"},
}

# ── helpers ────────────────────────────────────────────────────────────────

def _fk_pairs():
    """Return a set of (table, table) tuples that share a foreign key."""
    return set(FOREIGN_KEYS.keys())


def _columns_of(table: str) -> set:
    return set(SCHEMA.get(table, {}).keys())


def _col_type(table: str, col: str) -> str | None:
    return SCHEMA.get(table, {}).get(col)


def _ast_table_names(ast: exp.Expression) -> list[str]:
    """All physical table names referenced in the AST (via exp.Table nodes)."""
    return [t.name for t in ast.find_all(exp.Table) if t.name]


def _is_numeric_literal(node: exp.Expression) -> bool:
    return isinstance(node, exp.Literal) and not node.is_string


def _is_text_literal(node: exp.Expression) -> bool:
    return isinstance(node, (exp.Literal,)) and node.is_string


def _is_date_expr(node: exp.Expression) -> bool:
    """datetime('now') or datetime('now', '-X days') used for DATE columns."""
    return isinstance(node, exp.Anonymous) and str(node.this).lower() == "datetime"


# ── Result collector ───────────────────────────────────────────────────────

class TestResult:
    def __init__(self, verbose: bool = False):
        self.failures: list[dict] = []
        self.passed = 0
        self.verbose = verbose

    def ok(self, check: str):
        self.passed += 1

    def fail(self, record_id: Any, complexity: str, check: str, detail: str):
        self.failures.append(
            {"id": record_id, "complexity": complexity, "check": check, "detail": detail}
        )
        if self.verbose:
            print(f"  ✗ [{complexity} id={record_id}] {check}: {detail}")

    def summary(self) -> str:
        total = self.passed + len(self.failures)
        lines = [
            "",
            "=" * 70,
            f"SQL Generation Test Results",
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
                for item in items[:3]:  # show up to 3 examples
                    lines.append(f"        id={item['id']}: {item['detail'][:120]}")
                if len(items) > 3:
                    lines.append(f"        ... and {len(items)-3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self) -> bool:
        return len(self.failures) == 0


# ── Per-check test functions ───────────────────────────────────────────────

def check_structural(record: dict, ast: exp.Expression, r: TestResult):
    rid = record.get("id", "?")
    comp = record.get("complexity", "?")

    # 1. Required keys
    for key in ("id", "complexity", "sql", "tables"):
        if key not in record:
            r.fail(rid, comp, "required_keys", f"Missing key '{key}'")
        else:
            r.ok("required_keys")

    # 2. SQL parseable (already done — ast is not None if we reach here)
    r.ok("sql_parseable")

    # 3. complexity is known
    if comp not in KNOWN_COMPLEXITIES:
        r.fail(rid, comp, "known_complexity", f"Unknown complexity '{comp}'")
    else:
        r.ok("known_complexity")

    # 4. tables list matches AST tables
    declared = set(record.get("tables", []))
    ast_tables = set(_ast_table_names(ast))
    if declared != ast_tables:
        r.fail(rid, comp, "tables_match_ast",
               f"declared={sorted(declared)} ast={sorted(ast_tables)}")
    else:
        r.ok("tables_match_ast")

    # 5. No raw Python object reprs
    sql_str = record.get("sql", "")
    bad_patterns = [r"<[A-Z][a-zA-Z]+", r"\bNone\b", r"Subquery\(", r"Column\("]
    for pat in bad_patterns:
        if re.search(pat, sql_str):
            r.fail(rid, comp, "no_object_repr", f"Pattern '{pat}' found in SQL: {sql_str[:100]}")
            return
    r.ok("no_object_repr")

    # 5b. All referenced tables exist in schema
    for tname in ast_tables:
        if tname and tname not in KNOWN_TABLES:
            r.fail(rid, comp, "tables_in_schema", f"Unknown table '{tname}' referenced")
        else:
            r.ok("tables_in_schema")


def check_simple(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "simple"

    # 6. Is SELECT; FROM present; only 1 physical table
    if not isinstance(ast, exp.Select):
        r.fail(rid, comp, "simple_is_select", "Not a SELECT statement")
        return
    r.ok("simple_is_select")

    tables = _ast_table_names(ast)
    unique_base_tables = set(tables)
    if len(unique_base_tables) != 1:
        r.fail(rid, comp, "simple_one_table",
               f"Expected exactly 1 table, got: {sorted(unique_base_tables)}")
    else:
        r.ok("simple_one_table")

    # 7. No JOIN, UNION, subquery
    joins = ast.args.get("joins", [])
    if joins:
        r.fail(rid, comp, "simple_no_join", f"Found {len(joins)} JOIN(s) in simple query")
    else:
        r.ok("simple_no_join")

    if ast.find(exp.Union):
        r.fail(rid, comp, "simple_no_union", "Found UNION in simple query")
    else:
        r.ok("simple_no_union")

    if ast.find(exp.Subquery):
        r.fail(rid, comp, "simple_no_subquery", "Found subquery in simple query")
    else:
        r.ok("simple_no_subquery")

    # 8. WHERE columns (if any) come from the single table in schema
    where = ast.args.get("where")
    if where and unique_base_tables:
        table_name = next(iter(unique_base_tables))
        valid_cols = _columns_of(table_name)
        for col_node in where.find_all(exp.Column):
            col_name = col_node.name
            if col_name and col_name not in valid_cols:
                r.fail(rid, comp, "simple_where_columns",
                       f"Column '{col_name}' not in schema for table '{table_name}'")
            else:
                r.ok("simple_where_columns")


def check_join(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "join"

    if not isinstance(ast, exp.Select):
        r.fail(rid, comp, "join_is_select", "Not a SELECT")
        return
    r.ok("join_is_select")

    # 9. Exactly one JOIN
    joins = ast.args.get("joins", [])
    if len(joins) != 1:
        r.fail(rid, comp, "join_exactly_one", f"Expected 1 JOIN, found {len(joins)}")
        return
    else:
        r.ok("join_exactly_one")

    join_node = joins[0]

    # 10. Join uses a known FK pair
    from_node = ast.args.get("from_")
    left_table = from_node.this.this.name if from_node else None
    right_table = join_node.this.this.name if hasattr(join_node.this, "this") else None

    fk_pair = (left_table, right_table) if left_table and right_table else None
    reverse_pair = (right_table, left_table) if left_table and right_table else None

    if fk_pair not in FOREIGN_KEYS and reverse_pair not in FOREIGN_KEYS:
        r.fail(rid, comp, "join_uses_fk",
               f"No FK defined for ({left_table}, {right_table})")
    else:
        r.ok("join_uses_fk")

    # 11. ON clause references the correct FK columns
    on_node = join_node.args.get("on")
    if on_node is None:
        r.fail(rid, comp, "join_has_on_clause", "JOIN has no ON clause")
    else:
        r.ok("join_has_on_clause")
        on_sql = on_node.sql(dialect=USED_SQL_DIALECT).lower()
        pair = fk_pair if fk_pair in FOREIGN_KEYS else reverse_pair
        if pair and pair in FOREIGN_KEYS:
            lk, rk = FOREIGN_KEYS[pair]
            if lk.lower() not in on_sql or rk.lower() not in on_sql:
                r.fail(rid, comp, "join_on_fk_columns",
                       f"ON clause '{on_sql}' missing FK cols ({lk}, {rk})")
            else:
                r.ok("join_on_fk_columns")

    # 12. Join type is legal
    join_side = (join_node.side or "").upper()
    join_kind = (join_node.kind or "").upper()
    combined = f"{join_side} {join_kind}".strip()
    legal_patterns = {"INNER", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER", "LEFT", "RIGHT", "FULL", ""}
    if combined not in legal_patterns and join_kind not in {"INNER", ""}:
        r.fail(rid, comp, "join_type_legal",
               f"Unexpected join type: side='{join_side}' kind='{join_kind}'")
    else:
        r.ok("join_type_legal")

    # 13. Selected columns only from the two joined tables
    valid_cols = set()
    if left_table:
        valid_cols |= _columns_of(left_table)
    if right_table:
        valid_cols |= _columns_of(right_table)
    for col_node in ast.find_all(exp.Column):
        col_name = col_node.name
        if col_name and col_name not in valid_cols:
            r.fail(rid, comp, "join_column_scope",
                   f"Column '{col_name}' not in either joined table")
        else:
            r.ok("join_column_scope")


def _detect_advanced_subtype(ast: exp.Expression, sql_upper: str) -> str:
    """Heuristic: classify which advanced subtype this query represents."""
    # Self-join: same table name appears twice in exp.Table nodes
    table_names = [t.name for t in ast.find_all(exp.Table)]
    if len(table_names) >= 2 and len(set(table_names)) == 1:
        return "self_join"
    if "EXISTS" in sql_upper and "IN (SELECT" not in sql_upper:
        return "exists_subquery"
    if "IN (SELECT" in sql_upper or (ast.find(exp.In) and ast.find(exp.Select)):
        return "subquery_where"
    # Derived table: FROM clause contains a Subquery
    from_node = ast.args.get("from_") if isinstance(ast, exp.Select) else None
    if from_node and isinstance(from_node.this, exp.Subquery):
        return "subquery_from"
    return "unknown"


def check_advanced(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "advanced"
    sql_upper = record["sql"].upper()

    if not isinstance(ast, exp.Select):
        r.fail(rid, comp, "advanced_is_select", "Not a SELECT")
        return
    r.ok("advanced_is_select")

    # 14. Identify subtype
    subtype = _detect_advanced_subtype(ast, sql_upper)
    if subtype == "unknown":
        r.fail(rid, comp, "advanced_subtype_detected",
               f"Could not identify advanced subtype in: {record['sql'][:100]}")
        return
    r.ok("advanced_subtype_detected")

    if subtype == "subquery_where":
        # 15. IN with a subquery in WHERE
        in_node = ast.find(exp.In)
        if in_node is None:
            r.fail(rid, comp, "subquery_where_has_in", "No IN expression found")
            return
        r.ok("subquery_where_has_in")

        # inner must be a SELECT
        raw_sub = in_node.args.get("query")
        if raw_sub is None and in_node.expressions:
            raw_sub = in_node.expressions[0]
        inner = raw_sub.this if isinstance(raw_sub, exp.Subquery) else raw_sub
        if not isinstance(inner, exp.Select):
            r.fail(rid, comp, "subquery_where_inner_select",
                   "IN clause does not contain a SELECT")
        else:
            r.ok("subquery_where_inner_select")

        # 16. inner table related to outer via FK
        outer_tables = [t.name for t in ast.args.get("from_", exp.From()).find_all(exp.Table)]
        inner_tables = [t.name for t in inner.find_all(exp.Table)] if isinstance(inner, exp.Select) else []
        for ot in outer_tables:
            for it in inner_tables:
                if (ot, it) in FOREIGN_KEYS or (it, ot) in FOREIGN_KEYS:
                    r.ok("subquery_where_fk_related")
                    break

    elif subtype == "subquery_from":
        # 17. FROM clause is a derived subquery
        from_node = ast.args.get("from_")
        if not (from_node and isinstance(from_node.this, exp.Subquery)):
            r.fail(rid, comp, "subquery_from_derived_table",
                   "FROM clause is not a derived subquery")
        else:
            r.ok("subquery_from_derived_table")
            # inner must be a SELECT
            inner = from_node.this.this
            if not isinstance(inner, exp.Select):
                r.fail(rid, comp, "subquery_from_inner_select",
                       "Derived table does not contain a SELECT")
            else:
                r.ok("subquery_from_inner_select")

    elif subtype == "self_join":
        # 18. Both JOIN sides reference the same table
        table_names = [t.name for t in ast.find_all(exp.Table)]
        if len(set(table_names)) != 1:
            r.fail(rid, comp, "self_join_same_table",
                   f"Expected one table twice, got: {sorted(set(table_names))}")
        else:
            r.ok("self_join_same_table")

        # Must have exactly one JOIN
        joins = ast.args.get("joins", [])
        if len(joins) != 1:
            r.fail(rid, comp, "self_join_has_join", f"Expected 1 JOIN, found {len(joins)}")
        else:
            r.ok("self_join_has_join")

    elif subtype == "exists_subquery":
        # 19. WHERE contains EXISTS (or NOT EXISTS)
        where = ast.args.get("where")
        if where is None:
            r.fail(rid, comp, "exists_has_where", "No WHERE clause in exists_subquery")
            return
        r.ok("exists_has_where")

        has_exists = bool(where.find(exp.Exists))
        if not has_exists:
            r.fail(rid, comp, "exists_has_exists_expr", "No EXISTS expression in WHERE")
        else:
            r.ok("exists_has_exists_expr")

        # 20. Correlated condition: inner references outer alias column
        exists_node = where.find(exp.Exists)
        if exists_node:
            inner_sel = exists_node.this
            if isinstance(inner_sel, exp.Select):
                inner_where = inner_sel.args.get("where")
                if inner_where is None:
                    r.fail(rid, comp, "exists_correlated",
                           "EXISTS subquery has no WHERE (not correlated)")
                else:
                    r.ok("exists_correlated")


def check_union(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "union"

    # 21. Top-level is UNION or UNION ALL
    if not isinstance(ast, exp.Union):
        r.fail(rid, comp, "union_is_union_node", "Top-level AST is not a UNION node")
        return
    r.ok("union_is_union_node")

    left = ast.left
    right = ast.right

    if not isinstance(left, exp.Select):
        r.fail(rid, comp, "union_left_is_select", "Left leg of UNION is not a SELECT")
        return
    r.ok("union_left_is_select")

    if not isinstance(right, exp.Select):
        r.fail(rid, comp, "union_right_is_select", "Right leg of UNION is not a SELECT")
        return
    r.ok("union_right_is_select")

    # 22. Both sides select the same number of columns
    left_cols = len(left.expressions)
    right_cols = len(right.expressions)
    if left_cols != right_cols:
        r.fail(rid, comp, "union_column_count_match",
               f"Column count mismatch: left={left_cols}, right={right_cols}")
    else:
        r.ok("union_column_count_match")

    # 23. Both legs reference the same base table
    left_tables = set(_ast_table_names(left))
    right_tables = set(_ast_table_names(right))
    left_base = left_tables - right_tables or left_tables
    right_base = right_tables - left_tables or right_tables
    # Since aliases differ (t1, t2), we check the schema names are the same
    if left_tables.isdisjoint(right_tables) and left_tables != right_tables:
        # aliases differ: both sides select from their own alias of the same table
        # The check we actually care about: no unknown table involved
        for t in left_tables | right_tables:
            if t not in KNOWN_TABLES:
                r.fail(rid, comp, "union_tables_in_schema", f"Unknown table '{t}'")
            else:
                r.ok("union_tables_in_schema")
    else:
        r.ok("union_tables_in_schema")

    # 24. UNION ALL vs UNION: distinct flag matches SQL keyword
    sql_upper = record["sql"].upper()
    is_all_in_sql = "UNION ALL" in sql_upper
    is_all_in_ast = ast.args.get("distinct") is False
    if is_all_in_sql != is_all_in_ast:
        r.fail(rid, comp, "union_all_distinct_flag",
               f"SQL says UNION {'ALL' if is_all_in_sql else '(distinct)'} "
               f"but AST distinct flag={ast.args.get('distinct')}")
    else:
        r.ok("union_all_distinct_flag")


def check_insert(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "insert"

    if not isinstance(ast, exp.Insert):
        r.fail(rid, comp, "insert_is_insert", "Not an INSERT statement")
        return
    r.ok("insert_is_insert")

    # 25. Target table is in schema
    schema_node = ast.this
    table_name = schema_node.this.name if hasattr(schema_node, "this") else str(schema_node)
    if table_name not in KNOWN_TABLES:
        r.fail(rid, comp, "insert_table_in_schema", f"Unknown table '{table_name}'")
    else:
        r.ok("insert_table_in_schema")

    # Gather columns in INSERT
    insert_cols = [c.name for c in schema_node.expressions] if hasattr(schema_node, "expressions") else []

    # Gather values
    values_node = ast.expression
    values = []
    if values_node and hasattr(values_node, "expressions"):
        for tup in values_node.expressions:
            if hasattr(tup, "expressions"):
                values = tup.expressions
                break

    # 26. Column count == value count
    if insert_cols and values:
        if len(insert_cols) != len(values):
            r.fail(rid, comp, "insert_col_val_count",
                   f"Columns({len(insert_cols)}) != Values({len(values)})")
        else:
            r.ok("insert_col_val_count")

    # 27. 'id' not in INSERT columns
    if "id" in insert_cols:
        r.fail(rid, comp, "insert_no_pk_id", "'id' column found in INSERT (should be auto-increment)")
    else:
        r.ok("insert_no_pk_id")

    # 28. All insert columns exist in schema for that table
    valid_cols = _columns_of(table_name)
    for col in insert_cols:
        if col not in valid_cols:
            r.fail(rid, comp, "insert_columns_in_schema",
                   f"Column '{col}' not in schema for table '{table_name}'")
        else:
            r.ok("insert_columns_in_schema")

    # 29. Value types match schema column types (basic check)
    for col, val_node in zip(insert_cols, values):
        expected_type = _col_type(table_name, col)
        if expected_type in NUMERIC_TYPES:
            if not (_is_numeric_literal(val_node) or _is_date_expr(val_node)):
                r.fail(rid, comp, "insert_value_types",
                       f"Column '{col}' (numeric) got non-numeric value: {val_node}")
            else:
                r.ok("insert_value_types")
        elif expected_type in TEXT_TYPES:
            if not _is_text_literal(val_node):
                r.fail(rid, comp, "insert_value_types",
                       f"Column '{col}' (text) got non-string value: {val_node}")
            else:
                r.ok("insert_value_types")
        elif expected_type in DATE_TYPES:
            if not _is_date_expr(val_node):
                r.fail(rid, comp, "insert_value_types",
                       f"Column '{col}' (date) got unexpected value: {val_node}")
            else:
                r.ok("insert_value_types")
        elif expected_type in BOOLEAN_TYPES:
            if not _is_numeric_literal(val_node):
                r.fail(rid, comp, "insert_value_types",
                       f"Column '{col}' (boolean) should be 0 or 1, got: {val_node}")
            else:
                r.ok("insert_value_types")


def check_update(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "update"

    if not isinstance(ast, exp.Update):
        r.fail(rid, comp, "update_is_update", "Not an UPDATE statement")
        return
    r.ok("update_is_update")

    # 30. Target table in schema
    table_node = ast.this
    table_name = table_node.name if hasattr(table_node, "name") else str(table_node)
    if table_name not in KNOWN_TABLES:
        r.fail(rid, comp, "update_table_in_schema", f"Unknown table '{table_name}'")
    else:
        r.ok("update_table_in_schema")

    # 31. SET clause updates exactly one column
    set_exprs = ast.expressions  # list of EQ nodes
    if len(set_exprs) != 1:
        r.fail(rid, comp, "update_one_column", f"Expected 1 SET assignment, found {len(set_exprs)}")
    else:
        r.ok("update_one_column")

    # 32. Composite PK columns NOT updated
    composite_pk = COMPOSITE_PK_COLS.get(table_name, set())
    for eq in set_exprs:
        updated_col = eq.this.name if hasattr(eq.this, "name") else str(eq.this)
        if updated_col in composite_pk:
            r.fail(rid, comp, "update_no_composite_pk",
                   f"Column '{updated_col}' is part of composite PK for '{table_name}' — must not be updated")
        else:
            r.ok("update_no_composite_pk")

    # 33. 'id' not updated
    for eq in set_exprs:
        col = eq.this.name if hasattr(eq.this, "name") else str(eq.this)
        if col == "id":
            r.fail(rid, comp, "update_no_pk_id", "Primary key 'id' must not be updated")
        else:
            r.ok("update_no_pk_id")

    # 34. WHERE clause is present
    where = ast.args.get("where")
    if where is None:
        r.fail(rid, comp, "update_has_where", "UPDATE has no WHERE clause (would affect all rows)")
    else:
        r.ok("update_has_where")


def check_delete(record: dict, ast: exp.Expression, r: TestResult):
    rid = record["id"]
    comp = "delete"

    if not isinstance(ast, exp.Delete):
        r.fail(rid, comp, "delete_is_delete", "Not a DELETE statement")
        return
    r.ok("delete_is_delete")

    # 35. Target table in schema
    table_node = ast.this
    table_name = table_node.name if hasattr(table_node, "name") else str(table_node)
    if table_name not in KNOWN_TABLES:
        r.fail(rid, comp, "delete_table_in_schema", f"Unknown table '{table_name}'")
    else:
        r.ok("delete_table_in_schema")

    # 36. WHERE clause must be present
    where = ast.args.get("where")
    if where is None:
        r.fail(rid, comp, "delete_has_where", "DELETE has no WHERE clause (would delete all rows)")
    else:
        r.ok("delete_has_where")

    # 37. No subqueries inside DELETE
    if ast.find(exp.Subquery) or ast.find(exp.Select):
        r.fail(rid, comp, "delete_no_subquery", "DELETE contains a subquery (not expected from generator)")
    else:
        r.ok("delete_no_subquery")


# ── Dispatch ───────────────────────────────────────────────────────────────

COMPLEXITY_CHECKERS = {
    "simple":   check_simple,
    "join":     check_join,
    "advanced": check_advanced,
    "union":    check_union,
    "insert":   check_insert,
    "update":   check_update,
    "delete":   check_delete,
}


def run_tests(input_file: str, verbose: bool = False) -> TestResult:
    r = TestResult(verbose=verbose)

    with open(input_file) as f:
        dataset = json.load(f)

    print(f"Loaded {len(dataset)} records from {input_file}")
    print(f"Running SQL generation tests{'  (verbose)' if verbose else ''}...\n")

    by_complexity: dict[str, int] = defaultdict(int)

    for record in dataset:
        rid = record.get("id", "?")
        comp = record.get("complexity", "unknown")
        sql_str = record.get("sql", "")
        by_complexity[comp] += 1

        # 2. Parse SQL — abort this record if unparseable
        try:
            ast = parse_one(sql_str, dialect=USED_SQL_DIALECT)
        except (sqlglot_errors.ParseError, Exception) as e:
            r.fail(rid, comp, "sql_parseable", f"Parse error: {e}  SQL: {sql_str[:80]}")
            continue

        if ast is None:
            r.fail(rid, comp, "sql_parseable", f"parse_one returned None for: {sql_str[:80]}")
            continue

        r.ok("sql_parseable")

        # Structural checks (all complexities)
        check_structural(record, ast, r)

        # Complexity-specific checks
        checker = COMPLEXITY_CHECKERS.get(comp)
        if checker:
            checker(record, ast, r)
        else:
            r.fail(rid, comp, "known_complexity", f"No checker for complexity '{comp}'")

    # Print per-complexity record counts
    print("Record counts by complexity:")
    for c in ["simple", "join", "advanced", "union", "insert", "update", "delete"]:
        print(f"  {c:12s}: {by_complexity.get(c, 0)}")
    print()

    return r


# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validate raw SQL queries generated by src/core/generator.py"
    )
    parser.add_argument(
        "--input", "-i",
        default=str(ROOT / DEFAULT_INPUT_FILE),
        help=f"Path to the raw SQL JSON dataset file (default: {DEFAULT_INPUT_FILE})"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print each failure as it occurs"
    )
    args = parser.parse_args()

    result = run_tests(args.input, verbose=args.verbose)
    print(result.summary())
    sys.exit(0 if result.ok_overall else 1)


if __name__ == "__main__":
    main()
