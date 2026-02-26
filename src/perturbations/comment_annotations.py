"""Comment annotations perturbation strategy — adds SQL-style comments/notes."""

from .base import PerturbationStrategy


class CommentAnnotationsPerturbation(PerturbationStrategy):
    name = "comment_annotations"
    display_name = "Comment Annotations"
    description = "Added SQL comments/notes to the natural language."
    layer = "post_processing"

    _ANNOTATIONS = [
        "-- for the audit",
        "-- note for later",
        "-- urgent request",
        "(note: for analysis)",
        "-- needed for the report",
        "(specifically for this check)",
        "(referencing recent data)",
    ]

    def is_applicable(self, ast, nl_text, context):
        return True  # Comments can be appended to any NL prompt

    def apply(self, nl_text, ast, rng, context):
        """Append a comment annotation to the original NL text."""
        comment = rng.choice(self._ANNOTATIONS)
        base = nl_text if nl_text.endswith('.') else nl_text + '.'
        return f"{base} {comment}"
