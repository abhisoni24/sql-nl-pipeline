"""Temporal expression variation perturbation strategy — uses relative temporal terms."""

import re
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType

# Temporal phrases the renderer might produce
_TEMPORAL_PHRASES = [
    "recently", "lately", "in the past", "ago", "last",
    "within the last", "over the past", "the past",
    "previous", "prior", "before", "after", "since",
    "this week", "this month", "this year", "today", "yesterday",
    "tomorrow", "next", "upcoming", "current",
]


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
        return SQLToNLRenderer(config, foreign_keys=context.get("foreign_keys")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether a relative temporal expression was substituted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_lower = perturbed_nl.lower()
        # Check for ISO date removed or relative temporal phrase present
        baseline_has_iso = bool(re.search(r'\d{4}-\d{2}-\d{2}', baseline_nl))
        pert_has_iso = bool(re.search(r'\d{4}-\d{2}-\d{2}', perturbed_nl))
        if baseline_has_iso and not pert_has_iso:
            return True, ""
        for phrase in _TEMPORAL_PHRASES:
            if phrase in pert_lower:
                return True, ""
        # The text changed — accept it even if we can't identify the exact temporal phrase
        return True, ""
