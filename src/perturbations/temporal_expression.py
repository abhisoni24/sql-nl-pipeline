"""Temporal expression variation perturbation strategy — uses relative temporal terms."""

import re
import random as _random
from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer

# Temporal phrases the renderer might produce
_TEMPORAL_PHRASES = [
    "recently", "lately", "in the past", "ago", "last",
    "within the last", "over the past", "the past",
    "previous", "prior", "before", "after", "since",
    "this week", "this month", "this year", "today", "yesterday",
    "tomorrow", "next", "upcoming", "current",
]

# Relative alternatives for datetime modifiers
_PAST_ALTERNATIVES = ["within the last", "in the past", "over the last"]
_FUTURE_ALTERNATIVES = ["in", "out"]


class TemporalExpressionPerturbation(PerturbationStrategy):
    name = "temporal_expression_variation"
    display_name = "Temporal Expression Variation"
    description = "Used relative temporal terms instead of exact dates/times."
    layer = "dictionary"

    # ── Hook override ──────────────────────────────────────────────
    def on_temporal(self, raw_value, default, rng):
        """Replace literal dates / datetime modifiers with relative phrases."""
        # ISO date literal (e.g. "2024-01-15")
        if re.search(r'\d{4}-\d{2}-\d{2}', raw_value):
            return rng.choice(["recently", "since last year", "this month"])
        # Datetime modifier (e.g. "-30 days")
        modifier = raw_value.strip("'")
        match = re.search(r'([+-]?)\s*(\d+)\s*(\w+)', modifier)
        if match:
            sign, value, unit = match.groups()
            unit = unit.lower().rstrip('s')
            if not sign:
                sign = '+'
            unit_str = unit if value == '1' else f"{unit}s"
            if sign == '-':
                return rng.choice([f"{p} {value} {unit_str}" for p in _PAST_ALTERNATIVES])
            else:
                return rng.choice([f"in {value} {unit_str}", f"{value} {unit_str} out"])
        return default

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        is_insert = isinstance(ast, exp.Insert)
        is_dml = isinstance(ast, (exp.Insert, exp.Update, exp.Delete))
        if is_insert:
            return False
        if is_dml:
            where_node = ast.find(exp.Where)
            if not where_node:
                return False
            has_iso = any(re.search(r'\d{4}-\d{2}-\d{2}', str(l.this))
                          for l in where_node.find_all(exp.Literal))
            has_func = (any(str(a.this).lower() == 'datetime'
                           for a in where_node.find_all(exp.Anonymous))
                        or bool(where_node.find(exp.DateSub)))
            return has_iso or has_func
        has_iso = any(re.search(r'\d{4}-\d{2}-\d{2}', str(l.this))
                      for l in ast.find_all(exp.Literal))
        has_func = (any(str(a.this).lower() == 'datetime'
                        for a in ast.find_all(exp.Anonymous))
                    or bool(ast.find(exp.DateSub)))
        return has_iso or has_func

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        return renderer.render(ast)

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether a relative temporal expression was substituted."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        pert_lower = perturbed_nl.lower()
        baseline_has_iso = bool(re.search(r'\d{4}-\d{2}-\d{2}', baseline_nl))
        pert_has_iso = bool(re.search(r'\d{4}-\d{2}-\d{2}', perturbed_nl))
        if baseline_has_iso and not pert_has_iso:
            return True, ""
        for phrase in _TEMPORAL_PHRASES:
            if phrase in pert_lower:
                return True, ""
        return True, ""
