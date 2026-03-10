"""
Complexity Handler Base Class.

All SQL complexity types (simple, join, advanced, etc.) must implement
this interface to be auto-discovered and registered by the complexity registry.
"""

from abc import ABC, abstractmethod
from typing import Tuple
from sqlglot import exp


class ComplexityHandler(ABC):
    """
    Base class for all SQL complexity type handlers.

    Each handler encapsulates the generation logic for one complexity type.
    Handlers receive a reference to the generator instance so they can
    access shared helpers (generate_select, generate_where, generate_join, etc.).
    """

    # Subclasses must set these
    name: str = ""  # e.g., "simple", "join", "advanced"

    @abstractmethod
    def generate(self, gen, root_table: str, root_alias: str) -> Tuple[exp.Expression, str]:
        """
        Generate a random SQL AST of this complexity type.

        Args:
            gen: The SQLQueryGenerator instance (provides shared helpers
                 like generate_select, generate_where, generate_join, etc.
                 and carries a ``cfg: SchemaConfig`` attribute for native
                 schema access via ``gen.cfg.tables``, ``gen.fk_pairs``, etc.)
            root_table: The randomly chosen root table name.
            root_alias: The alias for the root table (e.g., "u1").

        Returns:
            Tuple of (sql_ast, complexity_name).
        """

    def is_match(self, ast: exp.Expression) -> bool:
        """
        Check if a given AST matches this complexity type.

        Default implementation returns False. Override in subclasses
        that need AST-based matching (useful for future rendering dispatch).
        """
        return False
