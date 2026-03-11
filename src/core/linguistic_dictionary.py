"""
Linguistic Dictionary.

Domain-specific vocabulary dataclass for NL rendering.
Contains synonym banks for tables, columns, operators, aggregates,
and structural connectors. Universal banks (operators, aggregates, etc.)
are schema-agnostic and shared across all domains.
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class LinguisticDictionary:
    """Domain-specific vocabulary for NL rendering.

    The dictionary has two layers:
      1. Schema-specific: table/column synonyms built per-schema.
      2. Universal: operator/aggregate/verb/connector synonyms shared across all schemas.
    """

    # --- Schema-specific synonym banks ---

    # Table synonyms: table_name -> list of English synonyms
    # e.g. {"users": ["accounts", "members", "profiles"]}
    table_synonyms: Dict[str, List[str]] = field(default_factory=dict)

    # Column synonyms: "table.column" -> list of English synonyms
    # e.g. {"users.signup_date": ["registration date", "join date"]}
    column_synonyms: Dict[str, List[str]] = field(default_factory=dict)

    # Semantic categories for tables (for pronoun/article selection)
    # e.g. {"users": "person", "posts": "object"}
    table_categories: Dict[str, str] = field(default_factory=dict)

    # --- Universal banks (same for all schemas) ---

    action_verbs: Dict[str, List[str]] = field(default_factory=lambda: {
        "get": ["Get", "Retrieve", "Find", "Pull up", "Dig out", "Go get", "Fetch me"],
        "select": ["Select", "Pick out", "Spot", "Single out", "Choose"],
        "show": ["Show", "Display", "Bring up", "Give me a look at", "Run a check for",
                 "Produce a listing of"],
        "insert": ["Add", "Insert", "Put", "Include", "Create"],
        "update": ["Update", "Change", "Modify", "Adjust", "Edit"],
        "delete": ["Remove", "Delete", "Drop", "Strip out", "Wipe out"],
    })

    operator_synonyms: Dict[str, List[str]] = field(default_factory=lambda: {
        "eq": ["equals", "is", "matches", "is equal to"],
        "neq": ["is not equal to", "differs from", "is not"],
        "gt": ["greater than", "exceeds", "more than", "above", "higher than"],
        "lt": ["less than", "below", "under", "fewer than", "lower than"],
        "gte": ["at least", "greater than or equal to", "minimum of", "no less than"],
        "lte": ["at most", "less than or equal to", "no more than", "maximum of"],
    })

    # Temporally-safe operator variants for date comparisons
    temporal_operator_synonyms: Dict[str, List[str]] = field(default_factory=lambda: {
        "gt": ["more recent than", "after", "since"],
        "lt": ["earlier than", "before", "prior to"],
        "gte": ["on or after", "starting from", "from"],
        "lte": ["on or before", "up to", "through"],
    })

    aggregate_synonyms: Dict[str, List[str]] = field(default_factory=lambda: {
        "COUNT": ["total number of", "how many", "count of", "number of"],
        "SUM": ["total", "sum of", "add up"],
        "AVG": ["average", "mean", "typical"],
        "MAX": ["maximum", "highest", "largest"],
        "MIN": ["minimum", "lowest", "smallest"],
    })

    structural_connectors: Dict[str, List[str]] = field(default_factory=lambda: {
        "where": ["where", "filtered for", "looking only at", "for which", "that have"],
        "from": ["from", "in", "within", "out of"],
        "and": ["and", "where also", "as well as", "along with"],
        "joined with": ["joined with", "linked to", "connected to", "join"],
    })

    fillers: List[str] = field(default_factory=lambda: [
        "Um", "Uh", "Well", "Okay", "So", "Alright"
    ])

    hedges: List[str] = field(default_factory=lambda: [
        "I think", "probably", "basically", "mostly", "sort of", "kind of"
    ])

    informal: List[str] = field(default_factory=lambda: [
        "you know", "like", "or something", "or whatever", "wanna", "gotta",
        "a bunch of"
    ])

    annotations: List[str] = field(default_factory=lambda: [
        "-- for the audit",
        "-- note for later",
        "-- urgent request",
        "(note: for analysis)",
        "-- needed for the report",
        "(specifically for this check)",
        "(referencing recent data)",
    ])

    urgency: Dict[str, List[str]] = field(default_factory=lambda: {
        "high": ["URGENT:", "ASAP:", "Immediately:", "Critical:", "High priority:"],
        "low": ["When you can,", "No rush,", "At your convenience,", "Low priority:"],
    })

    # --- Lookup Methods ---

    def lookup_table(self, table_name: str) -> List[str]:
        """Return synonyms for a table name, defaulting to a humanized version of the name."""
        return self.table_synonyms.get(table_name, [table_name.replace("_", " ")])

    def lookup_column(self, table_name: str, column_name: str) -> List[str]:
        """Return synonyms for a column, trying table-qualified then unqualified lookup."""
        qualified_key = f"{table_name}.{column_name}"
        if qualified_key in self.column_synonyms:
            return self.column_synonyms[qualified_key]
        # Try unqualified (column may appear in multiple tables with same synonyms)
        if column_name in self.column_synonyms:
            return self.column_synonyms[column_name]
        return [column_name.replace("_", " ")]

    def lookup_operator(self, op_key: str, is_temporal: bool = False) -> List[str]:
        """Return synonyms for an operator, using temporal variants if applicable."""
        if is_temporal and op_key in self.temporal_operator_synonyms:
            return self.temporal_operator_synonyms[op_key]
        return self.operator_synonyms.get(op_key, [op_key])

    def lookup_aggregate(self, agg_key: str) -> List[str]:
        """Return synonyms for an aggregate function."""
        return self.aggregate_synonyms.get(agg_key.upper(), [agg_key.lower()])

    def lookup_verb(self, verb_key: str) -> List[str]:
        """Return synonyms for an action verb."""
        return self.action_verbs.get(verb_key, [verb_key.capitalize()])

    def lookup_connector(self, conn_key: str) -> List[str]:
        """Return synonyms for a structural connector."""
        return self.structural_connectors.get(conn_key, [conn_key])

    def has_table_synonyms(self, table_name: str) -> bool:
        """Check if we have synonyms for a given table."""
        return table_name in self.table_synonyms

    def has_column_synonyms(self, table_name: str, column_name: str) -> bool:
        """Check if we have synonyms for a given column."""
        return (f"{table_name}.{column_name}" in self.column_synonyms
                or column_name in self.column_synonyms)
