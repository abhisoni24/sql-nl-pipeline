"""
Template Resolver (Pass 2 of Two-Pass NL Rendering)

Resolves IR template tokens produced by ``SQLToNLRenderer.render_template()``
(Pass 1) into natural language using a ``LinguisticDictionary``.

Token format
------------
    [TABLE:users]          → table synonym (e.g. "accounts", "members")
    [COL:email]            → column synonym, unqualified
    [COL:users.email]      → column synonym, table-qualified
    [OP:gt]                → operator synonym (e.g. "greater than", "exceeds")
    [AGG:COUNT]            → aggregate synonym (e.g. "total number of")
    [VAL:42]               → literal value (passed through unchanged)
    [VERB:get]             → action verb synonym (e.g. "Retrieve", "Find")
    [CONN:where]           → structural connector synonym (e.g. "filtered for")
"""

import re
import random
from typing import Optional

from src.core.linguistic_dictionary import LinguisticDictionary


class TemplateResolver:
    """Resolves IR template tokens against a LinguisticDictionary."""

    # Matches tokens like [TABLE:users], [COL:posts.content], [VAL:'hello']
    TOKEN_PATTERN = re.compile(r'\[(\w+):([^\]]+)\]')

    def __init__(self, dictionary: LinguisticDictionary, seed: int = 42):
        self.dictionary = dictionary
        self.rng = random.Random(seed)

    def resolve(self, template: str, table_context: str = "") -> str:
        """Replace all [TYPE:value] tokens with natural language.

        Parameters
        ----------
        template : str
            IR template string from ``render_template()``.
        table_context : str, optional
            Default table context for unqualified ``[COL:x]`` lookups.
            Usually the primary table in the query.

        Returns
        -------
        str
            Fully resolved natural-language prompt.
        """
        def replacer(match: re.Match) -> str:
            token_type = match.group(1)
            token_value = match.group(2)
            return self._resolve_token(token_type, token_value, table_context)

        return self.TOKEN_PATTERN.sub(replacer, template)

    def _resolve_token(self, token_type: str, value: str, table_ctx: str) -> str:
        """Resolve a single IR token to natural language text."""
        d = self.dictionary

        if token_type == "TABLE":
            candidates = d.lookup_table(value)
            return self.rng.choice(candidates)

        elif token_type == "COL":
            # Handle qualified columns: "table.column"
            if '.' in value:
                parts = value.split('.', 1)
                candidates = d.lookup_column(parts[0], parts[1])
            else:
                candidates = d.lookup_column(table_ctx, value)
            return self.rng.choice(candidates)

        elif token_type == "OP":
            candidates = d.lookup_operator(value)
            return self.rng.choice(candidates)

        elif token_type == "AGG":
            candidates = d.lookup_aggregate(value)
            return self.rng.choice(candidates)

        elif token_type == "VERB":
            candidates = d.lookup_verb(value)
            return self.rng.choice(candidates)

        elif token_type == "CONN":
            candidates = d.lookup_connector(value)
            return self.rng.choice(candidates)

        elif token_type == "VAL":
            # Values pass through unchanged
            return value

        else:
            # Unknown token type — pass through
            return value
