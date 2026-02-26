"""Punctuation variation perturbation strategy — modifies sentence rhythm."""

from .base import PerturbationStrategy


class PunctuationPerturbation(PerturbationStrategy):
    name = "punctuation_variation"
    display_name = "Punctuation Variation"
    description = "Modified sentence rhythm via punctuation changes."
    layer = "post_processing"

    def is_applicable(self, ast, nl_text, context):
        return True  # Punctuation changes can apply to any NL prompt

    def apply(self, nl_text, ast, rng, context):
        """Apply punctuation changes directly on the original NL text.
        Always produces at least one visible punctuation change."""
        result = nl_text
        changed = False
        roll = rng.random()

        # Strategy 1: comma → semicolon
        if ',' in result:
            result = result.replace(',', ';', 1)
            changed = True
        # Strategy 2: trailing punctuation variation
        elif roll > 0.5:
            result = result.rstrip('.') + '...'
            changed = True
        else:
            result = result.rstrip('.') + '!'
            changed = True

        return result
