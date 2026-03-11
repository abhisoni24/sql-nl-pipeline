"""Mixed SQL/NL perturbation strategy — blends raw SQL keywords into NL."""

import re
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType

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

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.MIXED_SQL_NL)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.MIXED_SQL_NL}, seed=seed)
        return SQLToNLRenderer(config, schema_config=context.get("schema_config")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether SQL keywords are embedded in the perturbed output."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_upper = perturbed_nl.upper()
        for kw in _SQL_KEYWORDS:
            if re.search(r'\b' + re.escape(kw) + r'\b', pert_upper):
                return True, ""
        return False, "No SQL keywords found in perturbed output"
