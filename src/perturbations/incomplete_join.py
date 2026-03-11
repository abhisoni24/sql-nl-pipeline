"""Incomplete join spec perturbation strategy — omits explicit JOIN/ON syntax."""

from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer


class IncompleteJoinPerturbation(PerturbationStrategy):
    name = "incomplete_join_spec"
    display_name = "Incomplete Join Spec"
    description = "Omitted explicit JOIN/ON syntax."
    layer = "template"

    # ── Hook override ──────────────────────────────────────────────
    def on_join(self, table_nl, on_str, default_phrase, context):
        # Simplified join: "and their X" if FK exists, else "with/along with X"
        if context.get("has_fk"):
            return f"and their {table_nl}"
        return f"{context.get('choice_incomplete', 'with')} {table_nl}"

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        return bool(ast.find(exp.Join))

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether join phrasing was simplified/omitted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        return True, ""
