"""
Insert Complexity Handler.

Generates INSERT INTO ... VALUES statements.
Extracted from generator.py generate_insert() L114-144.
"""

from sqlglot import exp
from .base import ComplexityHandler


class InsertHandler(ComplexityHandler):
    name = "insert"

    def generate(self, gen, root_table, root_alias):
        return gen.generate_insert(root_table), self.name
