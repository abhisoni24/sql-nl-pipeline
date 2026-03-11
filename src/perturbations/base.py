"""
PerturbationStrategy — Abstract base class for all perturbation strategies.

Each strategy encapsulates:
  - Applicability logic (can this perturbation be applied to this query?)
  - Application logic  (produce the perturbed NL text)
  - Render-time hooks  (override specific rendering decisions)
  - Optional test checks (validation functions for the test suite)

Render-time Hook System
-----------------------
The renderer calls hook methods at each "decision point" during AST traversal.
Strategies override only the hooks they need; all others pass through the
default value unchanged.  This lets new perturbations be added as a single file
with zero edits to the renderer.

Available hooks (all return default when not overridden):
  on_keyword        — structural keywords (SELECT, FROM, WHERE)
  on_join           — JOIN clause phrasing
  on_operator       — comparison operator phrasing
  on_aggregate      — aggregate function phrasing
  on_verb           — action verb synonym selection
  on_table_reference — table name / pronoun / synonym selection
  on_column_reference — column name / pronoun / synonym / qualifier selection
  on_temporal       — date/time literal rendering
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Callable, Any, Tuple
from sqlglot import exp
import random


class PerturbationStrategy(ABC):
    """Base class for all perturbation strategies."""

    name: str           # Machine name, e.g. "typos"
    display_name: str   # Human name, e.g. "Keyboard Typos"
    description: str    # What this perturbation does
    layer: str          # "template" | "dictionary" | "post_processing"

    # ── Render-time hooks ──────────────────────────────────────────
    # Override only the hooks your perturbation needs.  All receive the
    # "default" rendering and return it unchanged unless overridden.

    def on_keyword(self, keyword: str, default: str) -> str:
        """Called when the renderer emits a structural SQL keyword.

        Args:
            keyword: Raw SQL keyword (e.g. "SELECT", "FROM", "WHERE").
            default: NL equivalent the renderer would normally use.

        Returns:
            The text to emit.  Return ``keyword`` for raw SQL,
            ``""`` to omit, or ``default`` for normal NL.
        """
        return default

    def on_join(self, table_nl: str, on_str: str, default_phrase: str,
                context: dict) -> str:
        """Called when the renderer emits a JOIN clause.

        Args:
            table_nl: Rendered table name of the joined table.
            on_str:   Rendered ON clause (e.g. " on user id equals post user_id").
            default_phrase: Full default rendering (e.g. "joined with posts on ...").
            context:  Dict with keys: left_table, right_table, has_fk,
                      join_side, join_kind, choice_incomplete, is_standard_join.

        Returns:
            The join phrase to emit.
        """
        return default_phrase

    def on_operator(self, op_key: str, left: str, right: str,
                    default_str: str, context: dict) -> str:
        """Called when the renderer emits a comparison expression.

        Args:
            op_key:      Operator key (eq, neq, gt, lt, gte, lte).
            left:        Rendered left operand.
            right:       Rendered right operand.
            default_str: Default rendering (e.g. "age greater than 30").
            context:     Dict with keys: is_temporal, has_temporal_anchor,
                         is_self_contained, op_template, temporal_op (if applicable).

        Returns:
            The comparison phrase to emit.
        """
        return default_str

    def on_aggregate(self, agg_key: str, inner_text: str,
                     default_str: str, agg_template: str = "") -> str:
        """Called when the renderer emits an aggregate function.

        Args:
            agg_key:      Aggregate name (COUNT, SUM, AVG, MAX, MIN).
            inner_text:   Rendered inner expression.
            default_str:  Default rendering (just inner_text for baseline).
            agg_template: Pre-rolled template (e.g. "total number of").

        Returns:
            The aggregate phrase to emit.
        """
        return default_str

    def on_verb(self, key: str, baseline: str, rng: random.Random) -> str:
        """Called when the renderer picks an action verb synonym.

        Args:
            key:      Verb key (get, select, show, insert, update, delete).
            baseline: The synonym the RNG selected for this render.
            rng:      A secondary deterministic RNG for picking alternatives.

        Returns:
            The verb text to emit.
        """
        return baseline

    def on_table_reference(self, table_name: str, default: str,
                           is_repeated: bool, pronoun: str,
                           use_pronoun: bool, can_pronoun: bool,
                           synonym: str = "") -> str:
        """Called when the renderer emits a table reference.

        Args:
            table_name:   Raw table name (e.g. "users").
            default:      Default rendering (table_name).
            is_repeated:  True if this table was already mentioned.
            pronoun:      Pre-selected pronoun (e.g. "it", "the former").
            use_pronoun:  Whether the RNG roll favours pronoun use.
            can_pronoun:  Whether pronoun use is structurally safe
                          (count == 0, not self-join).
            synonym:      Pre-rolled schema synonym (e.g. "clients" for "users").

        Returns:
            The table reference to emit.
        """
        return default

    def on_column_reference(self, col_name: str, table: str,
                            default: str, is_repeated: bool,
                            pronoun: str, use_pronoun: bool,
                            can_pronoun: bool,
                            synonym: str = "") -> str:
        """Called when the renderer emits a column reference.

        Args:
            col_name:     Raw column name (e.g. "email").
            table:        Table qualifier (may be empty).
            default:      Default rendering.
            is_repeated:  True if this column was already mentioned.
            pronoun:      Pre-selected pronoun.
            use_pronoun:  Whether the RNG roll favours pronoun use.
            can_pronoun:  Whether pronoun use is structurally safe.
            synonym:      Pre-rolled schema synonym (e.g. "mail" for "email").

        Returns:
            The column reference to emit.
        """
        return default

    def on_temporal(self, raw_value: str, default: str,
                    rng: random.Random) -> str:
        """Called when the renderer emits a date/time value.

        Args:
            raw_value: Raw date string (e.g. "2024-01-15") or modifier.
            default:   Default rendering (e.g. "30 days ago").
            rng:       Seeded RNG for picking alternatives.

        Returns:
            The temporal phrase to emit.
        """
        return default

    # ── Core abstract methods ──────────────────────────────────────

    @abstractmethod
    def is_applicable(self, ast: exp.Expression, nl_text: str, context: dict) -> bool:
        """Pre-generation gate: can this perturbation type be applied to this query?

        This is a **structural check only** — it examines the SQL AST and/or
        NL text to decide whether the perturbation *could* produce a
        meaningful change.  It must NOT depend on the rendering output.

        Use ``was_applied()`` for post-generation validation of whether the
        perturbation actually took effect.

        Args:
            ast: Parsed SQL AST (sqlglot expression).
            nl_text: The baseline NL prompt.
            context: Dict with keys like 'schema', 'dictionary', 'renderer', etc.

        Returns:
            True if the perturbation can produce a meaningful change.
        """

    @abstractmethod
    def apply(self, nl_text: str, ast: exp.Expression, rng: random.Random,
              context: dict) -> str:
        """Apply the perturbation and return the modified NL text.

        Args:
            nl_text: The baseline NL prompt.
            ast: Parsed SQL AST.
            rng: Seeded RNG for determinism.
            context: Dict with keys like 'schema', 'dictionary', 'renderer', 'seed', etc.

        Returns:
            The perturbed NL string.
        """

    def was_applied(self, baseline_nl: str, perturbed_nl: str,
                    context: dict) -> Tuple[bool, str]:
        """Post-generation validation: did the perturbation actually take effect?

        Called **after** ``apply()`` returns a perturbed string.  Checks
        whether the specific perturbation effect is observable in the output
        (e.g., a pronoun was actually inserted, a synonym was actually used).

        The default implementation checks that the text differs from baseline,
        which is sufficient for always-applicable post-processing perturbations.
        Renderer-backed strategies should override this with domain-specific
        validation.

        Args:
            baseline_nl: The original NL prompt.
            perturbed_nl: The perturbed NL prompt returned by ``apply()``.
            context: Dict with keys like 'schema', 'dictionary', etc.

        Returns:
            Tuple of (applied: bool, detail: str) where *detail* explains
            the outcome (empty string when applied=True).
        """
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        return True, ""

    def get_test_checks(self) -> List[Callable]:
        """Return validation check functions for this perturbation.

        Each callable has signature:
            (record: dict, baseline_nl: str, perturbed_nl: str, context: dict)
                -> (passed: bool, detail: str)

        Return an empty list if no automated checks are defined.
        """
        return []
