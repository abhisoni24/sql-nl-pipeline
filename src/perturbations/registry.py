"""
Perturbation Registry — auto-discovers and registers all PerturbationStrategy
subclasses within the ``src.perturbations`` package.

Usage::

    from src.perturbations.registry import all_strategies, get_strategy

    for name, strategy in all_strategies().items():
        ...

    typos = get_strategy("typos")
"""

import importlib
import pkgutil
from typing import Dict

from .base import PerturbationStrategy

_REGISTRY: Dict[str, PerturbationStrategy] = {}


def _discover():
    """Auto-discover all PerturbationStrategy subclasses in this package."""
    import src.perturbations as pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
        if modname in ("base", "registry", "__init__"):
            continue
        module = importlib.import_module(f"src.perturbations.{modname}")
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (
                isinstance(attr, type)
                and issubclass(attr, PerturbationStrategy)
                and attr is not PerturbationStrategy
            ):
                instance = attr()
                _REGISTRY[instance.name] = instance


def all_strategies() -> Dict[str, PerturbationStrategy]:
    """Return all registered strategies, discovering on first call."""
    if not _REGISTRY:
        _discover()
    return _REGISTRY


def get_strategy(name: str) -> PerturbationStrategy:
    """Retrieve a single strategy by its machine name."""
    strats = all_strategies()
    if name not in strats:
        raise KeyError(
            f"Unknown perturbation strategy '{name}'. "
            f"Available: {sorted(strats.keys())}"
        )
    return strats[name]
