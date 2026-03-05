"""Ambiguous pronouns perturbation strategy — replaces one reference with it/that."""

import re
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


# Pronoun anchors that the renderer may insert
PRONOUN_ANCHORS = {
    "that value", "this value", "that field", "it", "the same",
    "aforementioned", "this field", "said", "this column", "this attribute",
    "that column", "the aforementioned",
}


class AmbiguousPronounsPerturbation(PerturbationStrategy):
    name = "anchored_pronoun_references"
    display_name = "Ambiguous Pronouns"
    description = "Replaced one table/column reference with a pronoun (it/that)."
    layer = "dictionary"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.AMBIGUOUS_PRONOUNS)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.AMBIGUOUS_PRONOUNS}, seed=seed)
        return SQLToNLRenderer(config, foreign_keys=context.get("foreign_keys")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether a pronoun anchor was actually inserted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_lower = perturbed_nl.lower()
        for anchor in PRONOUN_ANCHORS:
            if anchor in pert_lower:
                return True, ""
        return False, "No pronoun anchor found in perturbed output"
