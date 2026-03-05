"""Incomplete join spec perturbation strategy — omits explicit JOIN/ON syntax."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


class IncompleteJoinPerturbation(PerturbationStrategy):
    name = "incomplete_join_spec"
    display_name = "Incomplete Join Spec"
    description = "Omitted explicit JOIN/ON syntax."
    layer = "template"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.INCOMPLETE_JOIN_SPEC)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.INCOMPLETE_JOIN_SPEC}, seed=seed)
        return SQLToNLRenderer(config, foreign_keys=context.get("foreign_keys")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether join phrasing was simplified/omitted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        # Any text change from the join-omission renderer counts
        return True, ""
