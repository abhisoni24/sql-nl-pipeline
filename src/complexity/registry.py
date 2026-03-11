"""
Complexity Type Registry.

Auto-discovers and registers all ComplexityHandler subclasses in this package.
Provides lookup functions for the generator and renderer.
"""

from typing import Dict, List
from .base import ComplexityHandler
from .simple import SimpleHandler
from .join_handler import JoinHandler
from .advanced import AdvancedHandler
from .union import UnionHandler
from .insert import InsertHandler
from .update import UpdateHandler
from .delete import DeleteHandler


# The registry is populated at import time with all built-in handlers.
_REGISTRY: Dict[str, ComplexityHandler] = {}


def _init_registry():
    """Initialize the registry with all built-in handlers."""
    handlers = [
        SimpleHandler(),
        JoinHandler(),
        AdvancedHandler(),
        UnionHandler(),
        InsertHandler(),
        UpdateHandler(),
        DeleteHandler(),
    ]
    for handler in handlers:
        _REGISTRY[handler.name] = handler


def register(name: str, handler: ComplexityHandler) -> None:
    """
    Register a new complexity type handler at runtime.

    Use this for dynamically adding new complexity types (e.g., window functions)
    without modifying this file.
    """
    _REGISTRY[name] = handler


def get_handler(name: str) -> ComplexityHandler:
    """Get a registered handler by complexity name."""
    if not _REGISTRY:
        _init_registry()
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown complexity type: '{name}'. "
            f"Available: {list(_REGISTRY.keys())}"
        )
    return _REGISTRY[name]


def all_handlers() -> Dict[str, ComplexityHandler]:
    """Return all registered handlers."""
    if not _REGISTRY:
        _init_registry()
    return dict(_REGISTRY)


def all_handler_names() -> List[str]:
    """Return all registered complexity type names."""
    if not _REGISTRY:
        _init_registry()
    return list(_REGISTRY.keys())
