"""Omit obvious clauses perturbation strategy — removes explicit SQL clause keywords."""

from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer


class OmitObviousPerturbation(PerturbationStrategy):
    name = "omit_obvious_operation_markers"
    display_name = "Omit Obvious Clauses"
    description = "Removed explicit SQL clause keywords."
    layer = "template"

    # ── Hook overrides ─────────────────────────────────────────────
    def on_keyword(self, keyword, default):
        # Omit FROM, WHERE, ALIAS markers; keep SELECT verb
        if keyword in ("FROM", "WHERE", "ALIAS"):
            return ""
        return default

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        return not isinstance(ast, exp.Insert)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether obvious clause markers were removed."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        return True, ""
