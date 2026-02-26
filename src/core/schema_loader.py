"""
Schema Loaders.

Provides multiple ways to hydrate a SchemaConfig:
  - load_from_yaml()   : Parse a YAML schema definition file.
  - load_from_sqlite()  : Reflect schema from an existing SQLite database.
  - load_from_legacy()  : Load from the hardcoded src/core/schema.py (backward compat).
"""

import sqlite3
from typing import Optional
from .schema_config import SchemaConfig, TableDef, ColumnDef, ForeignKeyDef


# ---------------------------------------------------------------------------
# YAML Loader
# ---------------------------------------------------------------------------

def load_from_yaml(yaml_path: str) -> SchemaConfig:
    """
    Load a SchemaConfig from a YAML definition file.

    Expected YAML structure:
        name: social_media
        dialect: sqlite
        tables:
          users:
            columns:
              id: {type: int, is_pk: true}
              username: varchar
              ...
        foreign_keys:
          - [users, posts, id, user_id]
          ...
    """
    import yaml

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    config = SchemaConfig(
        dialect=data.get("dialect", "sqlite"),
        schema_name=data.get("name", "unnamed"),
    )

    # Parse tables
    for tname, tdata in data.get("tables", {}).items():
        cols = {}
        pks = []
        for cname, cinfo in tdata.get("columns", {}).items():
            # Columns can be specified as just a type string or as a dict
            if isinstance(cinfo, str):
                ctype = cinfo
                is_pk = False
            elif isinstance(cinfo, dict):
                ctype = cinfo.get("type", "varchar")
                is_pk = cinfo.get("is_pk", False)
            else:
                ctype = "varchar"
                is_pk = False

            cols[cname] = ColumnDef(name=cname, col_type=ctype, is_pk=is_pk)
            if is_pk:
                pks.append(cname)

        config.tables[tname] = TableDef(name=tname, columns=cols, primary_keys=pks)

    # Parse foreign keys: each entry is [source_table, target_table, source_col, target_col]
    for fk in data.get("foreign_keys", []):
        if len(fk) == 4:
            config.foreign_keys.append(ForeignKeyDef(
                source_table=fk[0],
                source_column=fk[2],
                target_table=fk[1],
                target_column=fk[3],
            ))

    # Mark FK columns
    _mark_fk_columns(config)

    return config


# ---------------------------------------------------------------------------
# Auto-detecting Loader
# ---------------------------------------------------------------------------

def load_schema(path: str, schema_name: Optional[str] = None) -> SchemaConfig:
    """
    Auto-detect file type and load a SchemaConfig.

    - If ``path`` ends with ``.yaml`` or ``.yml``, delegates to ``load_from_yaml()``.
    - If ``path`` ends with ``.sqlite``, ``.db``, or ``.sqlite3``, delegates to
      ``load_from_sqlite()``.  The *schema_name* is inferred from the filename
      stem (e.g., ``authors.sqlite`` → ``"authors"``) unless explicitly provided.
    """
    import os

    ext = os.path.splitext(path)[1].lower()
    if ext in (".yaml", ".yml"):
        return load_from_yaml(path)
    elif ext in (".sqlite", ".db", ".sqlite3"):
        if schema_name is None:
            schema_name = os.path.splitext(os.path.basename(path))[0]
        return load_from_sqlite(path, schema_name=schema_name)
    else:
        raise ValueError(
            f"Unsupported schema file extension '{ext}'. "
            f"Expected .yaml, .yml, .sqlite, .db, or .sqlite3."
        )


# ---------------------------------------------------------------------------
# SQLite Reflection Loader
# ---------------------------------------------------------------------------

def load_from_sqlite(db_path: str, schema_name: Optional[str] = None) -> SchemaConfig:
    """
    Reflect a SchemaConfig from an existing SQLite database file.

    Uses PRAGMA table_info and PRAGMA foreign_key_list to discover
    tables, columns, types, primary keys, and foreign key relationships.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    config = SchemaConfig(
        dialect="sqlite",
        schema_name=schema_name or db_path,
    )

    # Discover tables (skip SQLite internal tables)
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    table_names = [row[0] for row in cursor.fetchall()]

    for tname in table_names:
        # Get column info: (cid, name, type, notnull, dflt_value, pk)
        cursor.execute(f"PRAGMA table_info('{tname}')")
        cols = {}
        pks = []
        for row in cursor.fetchall():
            cname = row[1]
            ctype = _normalize_type(row[2]) if row[2] else "text"
            is_pk = bool(row[5])
            cols[cname] = ColumnDef(name=cname, col_type=ctype, is_pk=is_pk)
            if is_pk:
                pks.append(cname)

        config.tables[tname] = TableDef(name=tname, columns=cols, primary_keys=pks)

        # Get foreign keys: (id, seq, table, from, to, on_update, on_delete, match)
        cursor.execute(f"PRAGMA foreign_key_list('{tname}')")
        for fk_row in cursor.fetchall():
            config.foreign_keys.append(ForeignKeyDef(
                source_table=tname,
                source_column=fk_row[3],
                target_table=fk_row[2],
                target_column=fk_row[4],
            ))

    conn.close()

    # Mark FK columns
    _mark_fk_columns(config)

    return config


# ---------------------------------------------------------------------------
# Legacy Loader (backward compatibility)
# ---------------------------------------------------------------------------

def load_from_legacy() -> SchemaConfig:
    """
    Load a SchemaConfig from the existing hardcoded src/core/schema.py.

    This allows incremental migration: existing code can keep working
    while we transition consumers to the new SchemaConfig API one by one.
    """
    from src.core.schema import SCHEMA, FOREIGN_KEYS, USED_SQL_DIALECT

    config = SchemaConfig(
        dialect=USED_SQL_DIALECT,
        schema_name="social_media",
    )

    # Build tables
    for tname, tcols in SCHEMA.items():
        cols = {}
        for cname, ctype in tcols.items():
            cols[cname] = ColumnDef(name=cname, col_type=ctype)
        config.tables[tname] = TableDef(name=tname, columns=cols)

    # Build foreign keys (deduplicate — legacy dict has both forward and reverse)
    seen = set()
    for (t1, t2), (c1, c2) in FOREIGN_KEYS.items():
        # Normalize to avoid duplicates (A->B and B->A are the same FK)
        fk_key = tuple(sorted([(t1, c1), (t2, c2)]))
        if fk_key not in seen:
            seen.add(fk_key)
            config.foreign_keys.append(ForeignKeyDef(
                source_table=t1,
                source_column=c1,
                target_table=t2,
                target_column=c2,
            ))

    # Mark FK columns
    _mark_fk_columns(config)

    return config


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_type(raw_type: str) -> str:
    """Normalize SQLite type affinity names to our canonical types."""
    raw = raw_type.lower().strip()
    if "int" in raw:
        return "int"
    if "bool" in raw:
        return "boolean"
    if "date" in raw or "time" in raw:
        return "datetime"
    if "real" in raw or "floa" in raw or "doub" in raw:
        return "real"
    if "char" in raw or "text" in raw or "clob" in raw:
        return "varchar"
    return "varchar"  # SQLite default affinity


def _mark_fk_columns(config: SchemaConfig) -> None:
    """Mark columns that participate in foreign key relationships."""
    for fk in config.foreign_keys:
        # Mark source column (e.g., users.id in [users, posts, id, user_id])
        if fk.source_table in config.tables:
            tdef = config.tables[fk.source_table]
            if fk.source_column in tdef.columns:
                col = tdef.columns[fk.source_column]
                col.is_fk = True
                col.fk_references = (fk.target_table, fk.target_column)
        # Mark target column (e.g., posts.user_id in [users, posts, id, user_id])
        if fk.target_table in config.tables:
            tdef = config.tables[fk.target_table]
            if fk.target_column in tdef.columns:
                col = tdef.columns[fk.target_column]
                col.is_fk = True
                col.fk_references = (fk.source_table, fk.source_column)
