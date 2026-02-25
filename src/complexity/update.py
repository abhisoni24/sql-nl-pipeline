"""
Update Complexity Handler.

Generates UPDATE ... SET ... WHERE statements.
Extracted from generator.py generate_update() L146-196.
"""

from sqlglot import exp
from .base import ComplexityHandler


class UpdateHandler(ComplexityHandler):
    name = "update"

    def generate(self, gen, root_table, root_alias):
        return gen.generate_update(root_table), self.name
