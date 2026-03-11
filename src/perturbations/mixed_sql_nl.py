"""Mixed SQL/NL perturbation strategy — blends raw SQL keywords into NL."""

import re
from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer

# SQL keywords that the mixed-mode renderer may embed
_SQL_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "JOIN", "ON", "GROUP BY", "ORDER BY",
    "HAVING", "LIMIT", "INSERT", "UPDATE", "DELETE", "SET", "VALUES",
    "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "IS NULL", "IS NOT NULL",
    "ASC", "DESC", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
    "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN",
}


class MixedSqlNlPerturbation(PerturbationStrategy):
    name = "mixed_sql_nl"
    display_name = "Mixed SQL/NL"
    description = "Blended raw SQL keywords into natural language."
    layer = "template"

    # ── Hook override ──────────────────────────────────────────────
    def on_keyword(self, keyword, default):
        return keyword  # Emit raw SQL keywords (SELECT, FROM, WHERE...)

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        return not isinstance(ast, exp.Insert)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether SQL keywords are embedded in the perturbed output."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_upper = perturbed_nl.upper()
        for kw in _SQL_KEYWORDS:
            if re.search(r'\b' + re.escape(kw) + r'\b', pert_upper):
                return True, ""
        return False, "No SQL keywords found in perturbed output"
