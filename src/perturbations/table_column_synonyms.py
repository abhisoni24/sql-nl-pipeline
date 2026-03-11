"""Table/column synonyms perturbation strategy — uses human-centric schema synonyms."""

from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer


def _is_technical_alias(name):
    """Check if a name is a technical alias (e.g. u1, p2)."""
    import re
    if not name:
        return False
    if re.match(r'^[a-z]\d+$', name.lower()):
        return True
    return False


class TableColumnSynonymsPerturbation(PerturbationStrategy):
    name = "table_column_synonyms"
    display_name = "Table/Column Synonyms"
    description = "Used human-centric schema synonyms for tables and columns."
    layer = "dictionary"

    # ── Hook overrides ─────────────────────────────────────────────
    def on_table_reference(self, table_name, default, is_repeated,
                           pronoun, use_pronoun, can_pronoun, synonym=""):
        return synonym if synonym else default

    def on_column_reference(self, col_name, table, default,
                            is_repeated, pronoun, use_pronoun, can_pronoun,
                            synonym=""):
        return synonym if synonym else default

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        schema_config = context.get("schema_config")
        if not schema_config:
            return False
        # Build synonym bank from schema config + dictionary
        renderer = SQLToNLRenderer(schema_config=schema_config, dictionary=context.get("dictionary"))
        tables = [t.this.this.lower() if hasattr(t.this, 'this') else str(t.this).lower()
                  for t in ast.find_all(exp.Table)]
        columns = [c.this.this.lower() if hasattr(c.this, 'this') else str(c.this).lower()
                   for c in ast.find_all(exp.Column)]
        tables = [t for t in tables if not _is_technical_alias(t)]
        return (any(t in renderer.schema_synonyms for t in tables) or
                any(c in renderer.schema_synonyms for c in columns))

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether any table/column synonym was substituted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        return True, ""
