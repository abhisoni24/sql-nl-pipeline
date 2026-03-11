"""Ambiguous pronouns perturbation strategy — replaces one reference with it/that."""

import re
from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer


# Pronoun anchors that the renderer may insert
PRONOUN_ANCHORS = {
    "that value", "this value", "that field", "it", "the same",
    "aforementioned", "this field", "said", "this column", "this attribute",
    "that column", "the aforementioned",
}


def _is_technical_alias(name):
    if not name:
        return False
    if re.match(r'^[a-z]\d+$', name.lower()):
        return True
    return False


class AmbiguousPronounsPerturbation(PerturbationStrategy):
    name = "anchored_pronoun_references"
    display_name = "Ambiguous Pronouns"
    description = "Replaced one table/column reference with a pronoun (it/that)."
    layer = "dictionary"

    # ── Hook overrides ─────────────────────────────────────────────
    def on_table_reference(self, table_name, default, is_repeated,
                           pronoun, use_pronoun, can_pronoun, synonym=""):
        if is_repeated and can_pronoun and use_pronoun:
            return pronoun
        return default

    def on_column_reference(self, col_name, table, default,
                            is_repeated, pronoun, use_pronoun, can_pronoun,
                            synonym=""):
        if is_repeated and can_pronoun and use_pronoun:
            return pronoun
        return default

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        table_names = []
        for t in ast.find_all(exp.Table):
            val = t.this.this.lower() if hasattr(t.this, 'this') else str(t.this).lower()
            if val and not _is_technical_alias(val):
                table_names.append(val)
        if len(table_names) < 2:
            return False
        if len(set(table_names)) == 1:
            return False  # self-join
        return True

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether a pronoun anchor was actually inserted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_lower = perturbed_nl.lower()
        for anchor in PRONOUN_ANCHORS:
            if anchor in pert_lower:
                return True, ""
        return False, "No pronoun anchor found in perturbed output"
