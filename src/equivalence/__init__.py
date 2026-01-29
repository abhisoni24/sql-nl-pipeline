"""
SQL Equivalence Framework.

A robust framework for testing semantic equivalence of SQL statements,
supporting both DQL (SELECT) and DML (INSERT, UPDATE, DELETE) operations.

Uses the TestSuiteEval methodology for distilled test suite generation
and denotation-based comparison.
"""

from .config import EquivalenceConfig, EquivalenceResult
from .equivalence_engine import SQLEquivalenceEngine

__all__ = [
    "EquivalenceConfig",
    "EquivalenceResult", 
    "SQLEquivalenceEngine",
]
