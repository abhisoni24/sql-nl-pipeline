"""
Join Complexity Handler.

Generates two-table JOIN queries (INNER/LEFT/RIGHT/FULL).
Extracted from generator.py generate_query() L336-363.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class JoinHandler(ComplexityHandler):
    name = "join"

    def generate(self, gen, root_table, root_alias):
        query = exp.select()
        query = query.from_(exp.to_table(root_table).as_(root_alias))

        join_info = gen.generate_join(root_table, root_alias, [])
        if not join_info:
            raise ValueError("Could not find a valid foreign key for join.")

        target_table, target_alias, on_clause = join_info
        join_kind = random.choice(["INNER", "LEFT", "RIGHT", "FULL"])
        query = query.join(
            exp.to_table(target_table).as_(target_alias),
            on=on_clause,
            join_type=join_kind,
        )

        # Select from both tables
        s1 = gen.generate_select(root_table, alias=root_alias)
        s2 = gen.generate_select(target_table, alias=target_alias)
        all_selects = s1 + s2
        final_selects = random.sample(all_selects, min(len(all_selects), 4))

        # Remove redundancy: if * is selected, keep only ONE *
        has_star = any(isinstance(s, exp.Star) for s in final_selects)
        if has_star:
            final_selects = [exp.Star()]

        for s in final_selects:
            query = query.select(s, copy=False)

        if random.random() < 0.5:
            where = gen.generate_where(target_table, alias=target_alias)
            if where:
                query = query.where(where)

        return query, self.name
