"""Table/column synonyms perturbation strategy — uses human-centric schema synonyms."""

from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType


class TableColumnSynonymsPerturbation(PerturbationStrategy):
    name = "table_column_synonyms"
    display_name = "Table/Column Synonyms"
    description = "Used human-centric schema synonyms for tables and columns."
    layer = "dictionary"

    def is_applicable(self, ast, nl_text, context):
        return SQLToNLRenderer().is_applicable(ast, PerturbationType.TABLE_COLUMN_SYNONYMS)

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        config = PerturbationConfig(active_perturbations={PerturbationType.TABLE_COLUMN_SYNONYMS}, seed=seed)
        return SQLToNLRenderer(config, schema_config=context.get("schema_config")).render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether any table/column synonym was substituted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        # Any text change from the synonym renderer counts as a synonym substitution
        return True, ""
