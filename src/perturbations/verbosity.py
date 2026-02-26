"""Verbosity variation perturbation strategy — inserts conversational fillers."""

from .base import PerturbationStrategy


class VerbosityPerturbation(PerturbationStrategy):
    name = "verbosity_variation"
    display_name = "Verbosity Variation"
    description = "Insert conversational fillers and informal suffixes."
    layer = "post_processing"

    _FILLERS = ["Um", "Uh", "Well", "Okay", "So", "Alright"]
    _INFORMAL = ["you know", "like", "or something", "or whatever",
                 "wanna", "gotta", "a bunch of"]

    def is_applicable(self, ast, nl_text, context):
        return True  # Fillers can be added to any NL prompt

    def apply(self, nl_text, ast, rng, context):
        """Wrap the original NL text with a filler prefix and informal suffix."""
        filler = rng.choice(self._FILLERS)
        suffix = rng.choice(self._INFORMAL)
        body = nl_text.rstrip('.')
        return f"{filler} {body} {suffix}."
