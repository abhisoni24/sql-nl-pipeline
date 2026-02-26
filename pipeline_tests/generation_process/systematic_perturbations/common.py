"""
Common helpers for systematic perturbation test suites.
=======================================================
Provides shared schema loading, dictionary loading, helper functions,
and the TestResult class used by all 13 perturbation tests.

Usage in a test file::

    from common import (
        load_schema, load_dictionary, add_common_args, init_from_args,
        known_tables, known_columns, table_synonyms, column_synonyms,
        table_in_nl, sql_literals, numbers, get_pert, baseline, complexity,
        TestResult, ROOT,
    )
"""

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional, Set, Dict

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

# ── Dynamic state (populated by load_schema / load_dictionary) ────────────

_STATE: Dict = {
    "SCHEMA": {},                  # {table: {col, col, …}}
    "KNOWN_TABLES": set(),
    "KNOWN_COLUMNS": set(),
    "TABLE_SYNONYMS": {},          # {table: {syn, syn, …}}
    "COLUMN_SYNONYMS": {},         # {"table.col": {syn, syn, …}}
    "COLUMN_SYNONYMS_BARE": {},    # {"col": {syn, syn, …}}  (merged across tables)
}


# ── Schema loading ────────────────────────────────────────────────────────

def load_schema(schema_path: Optional[str] = None):
    """Load schema from YAML / SQLite or fall back to legacy ``src/core/schema.py``."""
    if schema_path:
        ext = Path(schema_path).suffix.lower()
        if ext in (".sqlite", ".db", ".sqlite3"):
            from src.core.schema_loader import load_from_sqlite
            cfg_obj = load_from_sqlite(schema_path, schema_name=Path(schema_path).stem)
            schema = {}
            for tname, tdef in cfg_obj.tables.items():
                schema[tname] = set(tdef.columns.keys())
            _STATE["SCHEMA"] = schema
        else:
            import yaml
            with open(schema_path) as f:
                cfg = yaml.safe_load(f)
            schema = {}
            for tname, tdata in cfg.get("tables", {}).items():
                cols = tdata.get("columns", {})
                schema[tname] = set(cols.keys())
            _STATE["SCHEMA"] = schema
    else:
        from src.core.schema import SCHEMA
        # Legacy SCHEMA may be {table: {col: type}} or {table: {col, …}}
        normalised = {}
        for tname, cols in SCHEMA.items():
            if isinstance(cols, dict):
                normalised[tname] = set(cols.keys())
            else:
                normalised[tname] = set(cols)
        _STATE["SCHEMA"] = normalised

    _STATE["KNOWN_TABLES"] = set(_STATE["SCHEMA"].keys())
    _STATE["KNOWN_COLUMNS"] = {col for cols in _STATE["SCHEMA"].values() for col in cols}

    # Initialise TABLE_SYNONYMS with just the canonical name (overridden by load_dictionary)
    _STATE["TABLE_SYNONYMS"] = {t: {t} for t in _STATE["KNOWN_TABLES"]}


def load_dictionary(dict_path: str):
    """Merge table & column synonyms from a dictionary YAML into the state."""
    import yaml
    with open(dict_path) as f:
        data = yaml.safe_load(f)

    # Merge table synonyms
    for tname, syns in data.get("table_synonyms", {}).items():
        existing = _STATE["TABLE_SYNONYMS"].get(tname, {tname})
        _STATE["TABLE_SYNONYMS"][tname] = existing | set(syns)

    # Load column synonyms (qualified: "table.col" → {syn, …})
    for qualified_col, syns in data.get("column_synonyms", {}).items():
        _STATE["COLUMN_SYNONYMS"][qualified_col] = set(syns)

    # Build bare-name index: "col" → union of all synonyms across tables
    bare: Dict[str, set] = {}
    for qualified, syns in _STATE["COLUMN_SYNONYMS"].items():
        _, col = qualified.split(".", 1) if "." in qualified else ("", qualified)
        if col not in bare:
            bare[col] = set()
        bare[col] |= syns
    _STATE["COLUMN_SYNONYMS_BARE"] = bare


# ── Accessor functions ────────────────────────────────────────────────────

def known_tables() -> Set[str]:
    return _STATE["KNOWN_TABLES"]

def known_columns() -> Set[str]:
    return _STATE["KNOWN_COLUMNS"]

def table_synonyms() -> Dict[str, Set[str]]:
    return _STATE["TABLE_SYNONYMS"]

def column_synonyms() -> Dict[str, Set[str]]:
    """Qualified column synonyms: ``{"table.col": {syn, …}}``."""
    return _STATE["COLUMN_SYNONYMS"]

def column_synonyms_bare() -> Dict[str, Set[str]]:
    """Bare column synonyms: ``{"col": {syn, …}}`` (merged across tables)."""
    return _STATE["COLUMN_SYNONYMS_BARE"]


# ── argparse helpers ──────────────────────────────────────────────────────

def add_common_args(parser):
    """Add ``--schema`` and ``--dictionary`` arguments to an argparse parser."""
    parser.add_argument(
        "--schema", "-s", default=None,
        help="Path to a YAML schema file (default: use legacy src/core/schema.py)"
    )
    parser.add_argument(
        "--dictionary", "-d", default=None,
        help="Path to a dictionary YAML to load table/column synonyms from"
    )


def init_from_args(args):
    """Call ``load_schema`` and ``load_dictionary`` based on parsed CLI args."""
    load_schema(getattr(args, "schema", None))
    dict_path = getattr(args, "dictionary", None)
    if dict_path:
        load_dictionary(dict_path)
        print(f"Loaded dictionary synonyms from {dict_path}")


# ── Shared helper functions ───────────────────────────────────────────────

def table_in_nl(table: str, nl_lower: str) -> bool:
    """Check if *table* (or any of its synonyms) appears in the NL string.

    Candidates are lowercased before matching since *nl_lower* is expected to
    be already lower-case (supports PascalCase schema names).
    """
    for candidate in _STATE["TABLE_SYNONYMS"].get(table, {table}):
        c = candidate.lower()
        for m in re.finditer(rf"\b{re.escape(c)}\b", nl_lower):
            rest   = nl_lower[m.end():]
            before = nl_lower[:m.start()]
            # Reject if part of a column name token (e.g. "like_id")
            if rest.startswith("_"):
                continue
            # Reject SQL LIKE operator usage (e.g. "like '%foo'")
            if c == "like" and re.match(r"\s*['\"%]", rest):
                continue
            # Reject string literal boundaries
            if before.endswith("'") or rest.startswith("'"):
                continue
            return True
    return False


def col_in_text(col: str, text_lower: str) -> bool:
    """Check if *col* appears as a whole word in *text_lower* (word-boundary match).

    The column name is lowercased to support PascalCase schema names.
    """
    return bool(re.search(r"\b" + re.escape(col.lower()) + r"\b", text_lower))


def is_synonym_fragment(col: str, text_lower: str) -> bool:
    """Return True if *col* appears in *text_lower* only as part of a
    multi-word synonym belonging to a **different** column.

    Example: "status" appears inside "employment status active" which is a
    synonym for ``is_active``.  Matching "status" there as a column reference
    to ``loans.status`` is a false positive — this function detects that.
    """
    col_syns = _STATE["COLUMN_SYNONYMS_BARE"]
    for bare_col, syns in col_syns.items():
        if bare_col == col:
            continue
        for syn in syns:
            if " " in syn and re.search(r"\b" + re.escape(col) + r"\b", syn) and syn in text_lower:
                return True
    return False


def sql_literals(text: str):
    """Extract SQL-style string literals from *text*."""
    return re.findall(r"(?<![a-zA-Z0-9])'([^']+)'", text)


def numbers(text: str):
    """Extract bare numeric tokens from *text*."""
    return re.findall(r"\b\d+\b", text)


def get_pert(record: dict, perturbation_name: str):
    """Return the perturbation entry matching *perturbation_name*, or ``None``."""
    for sp in record.get("generated_perturbations", {}).get("single_perturbations", []):
        if sp.get("perturbation_name") == perturbation_name:
            return sp
    return None


def baseline(record: dict) -> str:
    """Return the original (baseline) NL prompt from a record."""
    return record.get("generated_perturbations", {}).get("original", {}).get("nl_prompt", "")


def complexity(sql: str) -> str:
    """Classify a SQL statement by query complexity."""
    u = sql.upper().strip()
    if u.startswith("INSERT"):  return "insert"
    if u.startswith("UPDATE"):  return "update"
    if u.startswith("DELETE"):  return "delete"
    if "UNION" in u:            return "union"
    if "JOIN" in u:             return "join"
    if "IN (SELECT" in u or "EXISTS" in u or "FROM (" in u:
        return "advanced"
    tables = re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", u)
    flat = [t for pair in tables for t in pair if t]
    if len(flat) >= 2 and len(set(flat)) == 1:
        return "advanced"
    return "simple"


# ── TestResult class ──────────────────────────────────────────────────────

class TestResult:
    """Accumulates pass/fail results and renders a summary report."""

    def __init__(self, perturbation_name: str = "unknown", verbose: bool = False):
        self.perturbation_name = perturbation_name
        self.failures = []
        self.passed = 0
        self.verbose = verbose

    def ok(self, _):
        self.passed += 1

    def fail(self, rid, comp, check, detail):
        self.failures.append({
            "id": rid, "complexity": comp, "check": check, "detail": detail,
        })
        if self.verbose:
            print(f"  ✗ [{comp} id={rid}] {check}: {detail}")

    def summary(self):
        total = self.passed + len(self.failures)
        lines = [
            "",
            "=" * 70,
            f"Perturbation Test: {self.perturbation_name}",
            "=" * 70,
            f"  Total checks : {total}",
            f"  Passed       : {self.passed}",
            f"  Failed       : {len(self.failures)}",
        ]
        if self.failures:
            lines.append("\nFailures by check:")
            by_check = defaultdict(list)
            for f in self.failures:
                by_check[f["check"]].append(f)
            for check, items in sorted(by_check.items()):
                lines.append(f"  [{len(items):3d}x] {check}")
                for item in items[:3]:
                    lines.append(
                        f"        id={item['id']} [{item['complexity']}]: "
                        f"{item['detail'][:130]}"
                    )
                if len(items) > 3:
                    lines.append(f"        ... and {len(items)-3} more")
        lines.append("=" * 70)
        return "\n".join(lines)

    @property
    def ok_overall(self):
        return len(self.failures) == 0


# ── Run-tests skeleton ────────────────────────────────────────────────────

def run_tests(input_file, perturbation_name, check_record_fn, verbose=False):
    """Generic test runner shared by all perturbation test scripts.

    Args:
        input_file: Path to the JSON dataset.
        perturbation_name: The perturbation machine name.
        check_record_fn: ``check_record(record, comp, result)`` function.
        verbose: Print per-failure details.

    Returns:
        TestResult instance.
    """
    import json
    result = TestResult(perturbation_name=perturbation_name, verbose=verbose)
    with open(input_file) as f:
        dataset = json.load(f)
    # Support both bare-list and metadata-wrapped formats
    if isinstance(dataset, dict) and "records" in dataset:
        records = dataset["records"]
    else:
        records = dataset
    print(f"Loaded {len(records)} records from {input_file}")
    print(f"Running tests for: {perturbation_name}{'  (verbose)' if verbose else ''}\n")
    by_comp = defaultdict(int)
    for r in records:
        comp = complexity(r["sql"])
        by_comp[comp] += 1
        check_record_fn(r, comp, result)
    print("Record counts by complexity:")
    for c in ["simple", "join", "advanced", "union", "insert", "update", "delete"]:
        print(f"  {c:12s}: {by_comp.get(c, 0)}")
    print()
    return result
