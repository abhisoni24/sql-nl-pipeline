"""Temporal expression variation perturbation strategy — uses relative temporal terms."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


class TemporalExpressionPerturbation(PerturbationStrategy):
    name = "temporal_expression_variation"
    display_name = "Temporal Expression Variation"
    description = "Used relative temporal terms instead of exact dates/times."
    layer = "dictionary"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.TEMPORAL_EXPRESSION_VARIATION)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.TEMPORAL_EXPRESSION_VARIATION}, seed=seed)
        return SQLToNLRenderer(config).render(ast)
