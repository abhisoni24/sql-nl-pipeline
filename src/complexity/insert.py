"""
Insert Complexity Handler.

Generates INSERT INTO ... VALUES statements.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class InsertHandler(ComplexityHandler):
    name = "insert"

    def generate(self, gen, root_table, root_alias):
        columns = gen._get_column_names(root_table)
        # Filter out 'id' if it's an auto-increment primary key
        columns = [c for c in columns if c != 'id']

        values = []
        for col in columns:
            col_type = gen._get_column_type(root_table, col)
            if col_type in gen.cfg.numeric_types:
                values.append(exp.Literal.number(random.randint(1, 1000)))
            elif col_type in gen.cfg.text_types:
                if 'email' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}@example.com"))
                elif 'username' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}"))
                else:
                    values.append(exp.Literal.string(f"Sample text {random.randint(1,100)}"))
            elif col_type in gen.cfg.date_types:
                values.append(exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')]))
            elif col_type in gen.cfg.boolean_types:
                values.append(exp.Literal.number(random.choice([1, 0])))
            else:
                values.append(exp.Literal.string("val"))

        return exp.insert(
            exp.Values(expressions=[exp.Tuple(expressions=values)]),
            root_table,
            columns=[exp.Identifier(this=c, quoted=True) for c in columns]
        ), self.name
