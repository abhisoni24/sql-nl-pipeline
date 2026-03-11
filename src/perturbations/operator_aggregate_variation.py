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
        return SQLToNLRenderer(config, schema_config=context.get("schema_config")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether the operator/aggregate phrasing was changed."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        # Any text change from the operator/aggregate renderer counts
        return True, ""
