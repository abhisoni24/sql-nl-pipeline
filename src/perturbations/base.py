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
from typing import Optional, List, Callable, Any
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
        """Can this perturbation be meaningfully applied to this query?

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

    def get_test_checks(self) -> List[Callable]:
        """Return validation check functions for this perturbation.

        Each callable has signature:
            (record: dict, baseline_nl: str, perturbed_nl: str, context: dict)
                -> (passed: bool, detail: str)

        Return an empty list if no automated checks are defined.
        """
        return []
