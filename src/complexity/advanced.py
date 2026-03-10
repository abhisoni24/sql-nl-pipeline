"""
Advanced Complexity Handler.

Generates advanced queries: subquery_where, subquery_from, self_join, exists_subquery.
Extracted from generator.py generate_query() L380-544.
"""

import random
from sqlglot import exp
from .base import ComplexityHandler


class AdvancedHandler(ComplexityHandler):
    name = "advanced"

    def generate(self, gen, root_table, root_alias):
        subtype = random.choices(
            ["subquery_where", "subquery_from", "self_join", "exists_subquery"],
            weights=[0.35, 0.35, 0.15, 0.15],
        )[0]

        if subtype == "subquery_where":
            return self._subquery_where(gen, root_table, root_alias)
        elif subtype == "subquery_from":
            return self._subquery_from(gen, root_table, root_alias)
        elif subtype == "self_join":
            return self._self_join(gen)
        elif subtype == "exists_subquery":
            return self._exists_subquery(gen, root_table, root_alias)

    def _subquery_where(self, gen, root_table, root_alias):
        """WHERE col IN (SELECT ...)"""
        query = exp.select()
        query = query.from_(exp.to_table(root_table).as_(root_alias))

        selects = gen.generate_select(root_table, alias=root_alias)
        for s in selects:
            query = query.select(s, copy=False)

        candidates = [
            (t2, k1, k2)
            for (t1, t2), (k1, k2) in gen.fk_pairs.items()
            if t1 == root_table
        ]

        if not candidates:
            raise ValueError("Could not find candidates for subquery_where")

        target_table, left_key, right_key = random.choice(candidates)
        sub_alias = f"sub_{target_table[0]}"
        subquery = exp.select(exp.column(right_key, table=sub_alias)).from_(
            exp.to_table(target_table).as_(sub_alias)
        )

        sub_where = gen.generate_where(target_table, alias=sub_alias)
        if sub_where:
            subquery = subquery.where(sub_where)

        query = query.where(
            exp.In(
                this=exp.column(left_key, table=root_alias),
                expressions=[subquery],
            )
        )
        return query, self.name

    def _subquery_from(self, gen, root_table, root_alias):
        """FROM (SELECT ...) AS sub"""
        inner_alias = f"inner_{root_table}"
        inner_query = exp.select("*").from_(
            exp.to_table(root_table).as_(inner_alias)
        )
        inner_where = gen.generate_where(root_table, alias=inner_alias)
        if inner_where:
            inner_query = inner_query.where(inner_where)

        outer_alias = "derived_table"
        query = exp.select("*").from_(inner_query.subquery(alias=outer_alias))

        if random.random() < 0.5:
            outer_where = gen.generate_where(root_table, alias=outer_alias)
            if outer_where:
                query = query.where(outer_where)

        return query, self.name

    def _self_join(self, gen):
        """Self-join with various patterns."""
        # Pick a table that can meaningfully self-join
        self_joinable = [t for t in gen.cfg.tables if len(gen.cfg.tables[t].columns) >= 3]
        if not self_joinable:
            self_joinable = gen._table_names()

        root_table = random.choice(self_joinable)
        root_alias = f"{root_table[0]}1"
        target_alias = f"{root_table[0]}2"

        query = exp.select()
        query = query.from_(exp.to_table(root_table).as_(root_alias))

        cols = gen._get_column_names(root_table)

        # Generic self-join: find columns suitable for joining
        # Prefer FK columns or columns with same type for meaningful joins
        join_col = None

        # Check if this table has FK columns pointing to itself or shared columns
        for (t1, t2), (k1, k2) in gen.fk_pairs.items():
            if t1 == root_table and t2 == root_table:
                join_col = (k1, k2)
                break
            # Check for tables like 'follows' where the FK columns
            # can be used for self-join patterns
            if t1 == root_table or t2 == root_table:
                # Use a column that exists in both aliases
                if t1 == root_table:
                    join_col = (k1, k1)
                break

        if join_col:
            left_col, right_col = join_col
            # Select interesting columns
            non_pk_cols = [c for c in cols if c != "id"][:3]
            for c in non_pk_cols:
                query = query.select(exp.column(c, table=root_alias), copy=False)

            join_cond = exp.EQ(
                this=exp.column(left_col, table=root_alias),
                expression=exp.column(right_col, table=target_alias),
            )
            # If joining on same column, add inequality to avoid identity match
            if left_col == right_col:
                id_col = "id" if "id" in cols else cols[0]
                join_cond = exp.And(
                    this=join_cond,
                    expression=exp.NEQ(
                        this=exp.column(id_col, table=root_alias),
                        expression=exp.column(id_col, table=target_alias),
                    ),
                )
        else:
            # Fallback: pick any two non-PK columns
            non_pk = [c for c in cols if c != "id"]
            if len(non_pk) >= 2:
                join_on_col = non_pk[0]
            else:
                join_on_col = cols[0]

            for c in cols[:3]:
                query = query.select(exp.column(c, table=root_alias), copy=False)

            join_cond = exp.EQ(
                this=exp.column(join_on_col, table=root_alias),
                expression=exp.column(join_on_col, table=target_alias),
            )
            if "id" in cols:
                join_cond = exp.And(
                    this=join_cond,
                    expression=exp.NEQ(
                        this=exp.column("id", table=root_alias),
                        expression=exp.column("id", table=target_alias),
                    ),
                )

        query = query.join(
            exp.to_table(root_table).as_(target_alias), on=join_cond
        )

        if random.random() < 0.3:
            query = query.limit(random.randint(5, 20))

        return query, self.name

    def _exists_subquery(self, gen, root_table, root_alias):
        """EXISTS/NOT EXISTS subquery."""
        query = exp.select()
        query = query.from_(exp.to_table(root_table).as_(root_alias))

        selects = gen.generate_select(root_table, alias=root_alias)
        for s in selects:
            query = query.select(s, copy=False)

        candidates = [
            (t2, k1, k2)
            for (t1, t2), (k1, k2) in gen.fk_pairs.items()
            if t1 == root_table
        ]
        if not candidates:
            raise ValueError("Could not find candidates for exists_subquery")

        target_table, left_key, right_key = random.choice(candidates)
        sub_alias = f"sub_{target_table[0]}"

        subquery = exp.select(exp.Literal.number(1)).from_(
            exp.to_table(target_table).as_(sub_alias)
        )
        subquery = subquery.where(
            exp.EQ(
                this=exp.column(right_key, table=sub_alias),
                expression=exp.column(left_key, table=root_alias),
            )
        )

        sub_where = gen.generate_where(target_table, alias=sub_alias)
        if sub_where:
            subquery = subquery.where(sub_where)

        exists_expr = exp.Exists(this=subquery)
        if random.random() < 0.3:
            query = query.where(exp.Not(this=exists_expr))
        else:
            query = query.where(exists_expr)

        return query, self.name
