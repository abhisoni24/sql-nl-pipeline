"""
Update Complexity Handler.

Generates UPDATE ... SET ... WHERE statements.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class UpdateHandler(ComplexityHandler):
    name = "update"

    def generate(self, gen, root_table, root_alias):
        columns = gen._get_column_names(root_table)

        # Filter out primary key columns
        if root_table in gen.composite_pks:
            pk_cols = gen.composite_pks[root_table]
            safe_columns = [c for c in columns if c not in pk_cols]
        else:
            safe_columns = [c for c in columns if c != 'id']

        if not safe_columns:
            raise ValueError(f"No updatable columns for table {root_table}")

        col_to_update = random.choice(safe_columns)
        col_type = gen._get_column_type(root_table, col_to_update)

        if col_type in gen.cfg.numeric_types:
            val = exp.Literal.number(random.randint(1, 1000))
        elif col_type in gen.cfg.text_types:
            if 'email' in col_to_update:
                val = exp.Literal.string(f"updated_user{random.randint(1,100)}@example.com")
            else:
                val = exp.Literal.string(f"Updated text {random.randint(1,100)}")
        elif col_type in gen.cfg.date_types:
            val = exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')])
        elif col_type in gen.cfg.boolean_types:
            val = exp.Literal.number(random.choice([1, 0]))
        else:
            val = exp.Literal.string("val")

        update_expr = exp.update(root_table, {exp.column(col_to_update): val})

        where = gen.generate_where(root_table)
        if where:
            update_expr = update_expr.where(where)

        return update_expr, self.name
