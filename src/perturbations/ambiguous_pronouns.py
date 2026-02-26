"""Ambiguous pronouns perturbation strategy — replaces one reference with it/that."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


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
        return SQLToNLRenderer(config).render(ast)
