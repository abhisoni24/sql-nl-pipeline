"""
Simple Complexity Handler.

Generates single-table SELECT queries with optional WHERE, ORDER BY, and LIMIT.
Extracted from generator.py generate_query() L321-335.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class SimpleHandler(ComplexityHandler):
    name = "simple"

    def generate(self, gen, root_table, root_alias):
        query = exp.select()
        query = query.from_(exp.to_table(root_table))

        selects = gen.generate_select(root_table)
        for s in selects:
            query = query.select(s, copy=False)

        if random.random() < 0.5:
            where = gen.generate_where(root_table)
            if where:
                query = query.where(where)

        if random.random() < 0.3:
            cols = gen._get_column_names(root_table)
            query = query.order_by(
                exp.column(random.choice(cols), table=root_table),
                desc=random.choice([True, False])
            )

        if random.random() < 0.3:
            query = query.limit(random.randint(1, 100))

        return query, self.name
