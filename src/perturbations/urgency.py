"""Urgency qualifiers perturbation strategy — adds urgency markers."""

from .base import PerturbationStrategy


class UrgencyPerturbation(PerturbationStrategy):
    name = "urgency_qualifiers"
    display_name = "Urgency Qualifiers"
    description = "Add urgency markers like 'ASAP', 'immediately', etc."
    layer = "post_processing"

    _URGENCY = {
        'high': ["URGENT:", "ASAP:", "Immediately:", "Critical:", "High priority:"],
        'low': ["When you can,", "No rush,", "At your convenience,", "Low priority:"]
    }

    def is_applicable(self, ast, nl_text, context):
        return True  # Urgency prefix can be added to any NL prompt

    def apply(self, nl_text, ast, rng, context):
        """Prepend an urgency qualifier to the original NL text."""
        level = rng.choice(['high', 'low'])
        prefix = rng.choice(self._URGENCY[level])
        return f"{prefix} {nl_text}"
