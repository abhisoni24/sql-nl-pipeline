"""
Schema Configuration Dataclasses.

Provides domain-agnostic dataclasses for representing any SQL database schema.
These replace the hardcoded dicts in src/core/schema.py and serve as the
universal internal representation consumed by the generator, renderer,
and equivalence checker.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple, Optional


@dataclass
class ColumnDef:
    """Definition of a single database column."""
    name: str
    col_type: str  # Canonical types: "int", "varchar", "text", "datetime", "boolean", "real"
    is_pk: bool = False
    is_fk: bool = False
    fk_references: Optional[Tuple[str, str]] = None  # (target_table, target_column)


@dataclass
class TableDef:
    """Definition of a single database table."""
    name: str
    columns: Dict[str, ColumnDef] = field(default_factory=dict)
    primary_keys: List[str] = field(default_factory=list)

    @property
    def column_names(self) -> Set[str]:
        """Return the set of all column names in this table."""
        return set(self.columns.keys())

    def columns_of_type(self, *types: str) -> List[str]:
        """Return column names matching any of the given type strings."""
        return [c.name for c in self.columns.values() if c.col_type in types]


@dataclass
class ForeignKeyDef:
    """Definition of a foreign key relationship."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str


@dataclass
class SchemaConfig:
    """
    Complete, domain-agnostic schema configuration.

    This is the universal schema representation used throughout the pipeline.
    It can be hydrated from YAML files, SQLite reflection, or the legacy
    hardcoded schema.py.
    """
    tables: Dict[str, TableDef] = field(default_factory=dict)
    foreign_keys: List[ForeignKeyDef] = field(default_factory=list)
    dialect: str = "sqlite"
    schema_name: str = "unnamed"

    # Column type categories (universal defaults)
    numeric_types: Set[str] = field(default_factory=lambda: {"int", "integer", "real", "float"})
    text_types: Set[str] = field(default_factory=lambda: {"varchar", "text", "char"})
    date_types: Set[str] = field(default_factory=lambda: {"datetime", "date", "timestamp"})
    boolean_types: Set[str] = field(default_factory=lambda: {"boolean", "bool"})

    @property
    def table_names(self) -> Set[str]:
        """Return the set of all table names in the schema."""
        return set(self.tables.keys())

    def get_fk_pairs(self) -> Dict[Tuple[str, str], Tuple[str, str]]:
        """
        Return foreign keys in the legacy (table_a, table_b): (col_a, col_b) format.

        This provides backward compatibility with existing code that consumes
        the FOREIGN_KEYS dict from src/core/schema.py.

        Includes both forward and reverse directions for each FK.
        """
        result = {}
        for fk in self.foreign_keys:
            # Forward direction
            result[(fk.source_table, fk.target_table)] = (fk.source_column, fk.target_column)
            # Reverse direction
            result[(fk.target_table, fk.source_table)] = (fk.target_column, fk.source_column)
        return result

    def get_legacy_schema(self) -> Dict[str, Dict[str, str]]:
        """
        Return schema in the legacy {table: {col: type}} format.

        This provides backward compatibility with existing code that consumes
        the SCHEMA dict from src/core/schema.py.
        """
        result = {}
        for tname, tdef in self.tables.items():
            result[tname] = {c.name: c.col_type for c in tdef.columns.values()}
        return result

    def get_type_sets(self) -> dict:
        """
        Return type category sets in the legacy format.

        Returns a dict with keys: 'numeric', 'text', 'date', 'boolean'.
        """
        return {
            "numeric": self.numeric_types,
            "text": self.text_types,
            "date": self.date_types,
            "boolean": self.boolean_types,
        }
