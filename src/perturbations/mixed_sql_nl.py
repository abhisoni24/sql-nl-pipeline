"""Mixed SQL/NL perturbation strategy — blends raw SQL keywords into NL."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


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
        return SQLToNLRenderer(config).render(ast)
