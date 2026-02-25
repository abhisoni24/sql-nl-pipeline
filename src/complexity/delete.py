"""
Delete Complexity Handler.

Generates DELETE FROM ... WHERE statements.
Extracted from generator.py generate_delete() L198-206.
"""

from sqlglot import exp
from .base import ComplexityHandler


class DeleteHandler(ComplexityHandler):
    name = "delete"

    def generate(self, gen, root_table, root_alias):
        return gen.generate_delete(root_table), self.name
