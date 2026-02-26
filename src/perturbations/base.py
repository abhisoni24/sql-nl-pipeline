"""
PerturbationStrategy — Abstract base class for all perturbation strategies.

Each strategy encapsulates:
  - Applicability logic (can this perturbation be applied to this query?)
  - Application logic  (produce the perturbed NL text)
  - Optional test checks (validation functions for the test suite)

Strategies delegate actual rendering to the existing SQLToNLRenderer,
which contains the deeply-integrated perturbation logic via PerturbationType
flags. This preserves all existing behavior while providing a modular,
extensible interface.
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
