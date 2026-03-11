"""Operator/aggregate variation perturbation strategy — varies operator and aggregate format."""

from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer

# Operator and aggregate variation data (kept in sync with renderer data banks)
_OP_VARIATIONS = {
    "gt": ["exceeds", "more than", "above", "higher than"],
    "lt": ["below", "under", "fewer than", "lower than"],
    "gte": ["at least", "minimum of", "no less than"],
    "lte": ["at most", "maximum of", "no more than"],
}
_AGG_VARIATIONS = {
    "COUNT": ["total number of", "how many", "count of", "number of"],
    "SUM": ["total", "sum of", "add up"],
    "AVG": ["average", "mean", "typical"],
    "MAX": ["maximum", "highest", "largest"],
    "MIN": ["minimum", "lowest", "smallest"],
}


class OperatorAggregateVariationPerturbation(PerturbationStrategy):
    name = "operator_aggregate_variation"
    display_name = "Operator/Aggregate Variation"
    description = "Varied operator/aggregate format."
    layer = "dictionary"

    # ── Hook overrides ─────────────────────────────────────────────
    def on_operator(self, op_key, left, right, default_str, context):
        is_self_contained = context.get("is_self_contained", False)
        has_temporal_anchor = context.get("has_temporal_anchor", False)
        op_template = context.get("op_template", op_key)
        rng = context.get("rng")

        if is_self_contained:
            return f"{left} {right}"
        elif has_temporal_anchor:
            temporal_ops = context.get("temporal_op_variations", {}).get(op_key)
            if temporal_ops and rng:
                temporal_op = rng.choice(temporal_ops)
                suffixes = context.get("temporal_op_suffixes", {})
                suffix = suffixes.get(temporal_op, "")
                suffix_str = f" {suffix}" if suffix else ""
                return f"{left} {temporal_op} {right}{suffix_str}"
            return default_str
        else:
            return f"{left} {op_template} {right}"

    def on_aggregate(self, agg_key, inner_text, default_str, agg_template=""):
        if agg_template:
            return f"{agg_template} {inner_text}"
        return default_str

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        has_op = any(e.key in _OP_VARIATIONS for e in ast.find_all((exp.GT, exp.LT, exp.GTE, exp.LTE)))
        has_agg = any(e.key.upper() in _AGG_VARIATIONS for e in ast.find_all(exp.AggFunc))
        return has_op or has_agg

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether the operator/aggregate phrasing was changed."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        return True, ""
