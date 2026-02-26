"""Operator/aggregate variation perturbation strategy — varies operator and aggregate format."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


class OperatorAggregateVariationPerturbation(PerturbationStrategy):
    name = "operator_aggregate_variation"
    display_name = "Operator/Aggregate Variation"
    description = "Varied operator/aggregate format."
    layer = "dictionary"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.OPERATOR_AGGREGATE_VARIATION)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION}, seed=seed)
        return SQLToNLRenderer(config).render(ast)
