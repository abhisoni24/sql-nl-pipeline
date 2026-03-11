"""Omit obvious clauses perturbation strategy — removes explicit SQL clause keywords."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


class OmitObviousPerturbation(PerturbationStrategy):
    name = "omit_obvious_operation_markers"
    display_name = "Omit Obvious Clauses"
    description = "Removed explicit SQL clause keywords."
    layer = "template"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.OMIT_OBVIOUS_CLAUSES)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.OMIT_OBVIOUS_CLAUSES}, seed=seed)
        return SQLToNLRenderer(config, schema_config=context.get("schema_config")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether obvious clause markers were removed."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        # Any text change from the omit-obvious renderer counts
        return True, ""
