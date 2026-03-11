"""
Delete Complexity Handler.

Generates DELETE FROM ... WHERE statements.
"""

from sqlglot import exp
from .base import ComplexityHandler


class DeleteHandler(ComplexityHandler):
    name = "delete"

    def generate(self, gen, root_table, root_alias):
        delete_expr = exp.delete(root_table)

        # Always add WHERE clause — retry with different columns if needed
        where = None
        for _ in range(10):
            where = gen.generate_where(root_table)
            if where:
                break
        if where:
            delete_expr = delete_expr.where(where)
        else:
            # Ultimate fallback: DELETE WHERE id > 0
            if 'id' in gen.cfg.tables[root_table].columns:
                delete_expr = delete_expr.where(
                    exp.GT(this=exp.column('id', table=root_table),
                           expression=exp.Literal.number(0)))

        return delete_expr, self.name
