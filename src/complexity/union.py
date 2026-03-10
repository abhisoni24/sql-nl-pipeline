"""
Union Complexity Handler.

Generates UNION / UNION ALL queries from two compatible SELECT statements.
Extracted from generator.py generate_union() L208-247.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class UnionHandler(ComplexityHandler):
    name = "union"

    def generate(self, gen, root_table, root_alias):
        # Pick a table for consistent columns (use root_table from caller)
        table = root_table
        columns = gen._get_column_names(table)

        num_cols = random.randint(2, min(4, len(columns)))
        selected_cols = random.sample(columns, num_cols)

        # Build first SELECT
        alias1 = f"{table[0]}1"
        query1 = exp.select(*[exp.column(c, table=alias1) for c in selected_cols])
        query1 = query1.from_(exp.to_table(table).as_(alias1))
        where1 = gen.generate_where(table, alias=alias1)
        if where1:
            query1 = query1.where(where1)

        # Build second SELECT with different WHERE
        alias2 = f"{table[0]}2"
        query2 = exp.select(*[exp.column(c, table=alias2) for c in selected_cols])
        query2 = query2.from_(exp.to_table(table).as_(alias2))
        where2 = gen.generate_where(table, alias=alias2)
        if where2:
            query2 = query2.where(where2)

        # Combine with UNION or UNION ALL
        use_all = random.random() < 0.4
        if use_all:
            union_query = exp.union(query1, query2, distinct=False)
        else:
            union_query = exp.union(query1, query2, distinct=True)

        if random.random() < 0.3:
            union_query = union_query.order_by(exp.column(selected_cols[0]))
        if random.random() < 0.3:
            union_query = union_query.limit(random.randint(10, 50))

        return union_query, self.name
