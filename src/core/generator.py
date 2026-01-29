import random
from sqlglot import exp
from src.core.schema import SCHEMA, FOREIGN_KEYS, NUMERIC_TYPES, TEXT_TYPES, DATE_TYPES, BOOLEAN_TYPES, USED_SQL_DIALECT

class SQLQueryGenerator:
    def __init__(self, schema, foreign_keys):
        self.schema = schema
        self.foreign_keys = foreign_keys
        self.queries = []

    def _get_column_type(self, table, column):
        return self.schema[table].get(column)

    def generate_select(self, table, use_aggregate=False, group_by_cols=None, alias=None):
        table_alias = alias if alias else table
        columns = list(self.schema[table].keys())
        
        if use_aggregate:
            # If grouping, we must select group_by columns + aggregates
            select_exprs = []
            if group_by_cols:
                for col in group_by_cols:
                    # Handle if col is already an expression or just a name
                    if isinstance(col, exp.Expression):
                         select_exprs.append(col)
                    else:
                         select_exprs.append(exp.column(col, table=table_alias))
            
            # Add an aggregate
            agg_type = random.choice(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'])
            agg_col = random.choice(columns)
            
            if agg_type == 'COUNT':
                if random.random() < 0.5:
                    select_exprs.append(exp.Count(this=exp.Star()).as_("count_all"))
                else:
                    select_exprs.append(exp.Count(this=exp.column(agg_col, table=table_alias)).as_(f"count_{agg_col}"))
            elif self._get_column_type(table, agg_col) in NUMERIC_TYPES:
                 # Only sum/avg numeric types
                 if agg_type == 'SUM':
                     select_exprs.append(exp.Sum(this=exp.column(agg_col, table=table_alias)).as_(f"sum_{agg_col}"))
                 elif agg_type == 'AVG':
                     select_exprs.append(exp.Avg(this=exp.column(agg_col, table=table_alias)).as_(f"avg_{agg_col}"))
                 elif agg_type == 'MIN':
                     select_exprs.append(exp.Min(this=exp.column(agg_col, table=table_alias)).as_(f"min_{agg_col}"))
                 elif agg_type == 'MAX':
                     select_exprs.append(exp.Max(this=exp.column(agg_col, table=table_alias)).as_(f"max_{agg_col}"))
            else:
                # Fallback to count for non-numeric
                select_exprs.append(exp.Count(this=exp.Star()).as_("count_all"))
                
            return select_exprs
        else:
            # Simple select
            if random.random() < 0.3:
                return [exp.Star()]
            else:
                num_cols = random.randint(1, min(4, len(columns)))
                selected_cols = random.sample(columns, num_cols)
                return [exp.column(col, table=table_alias) for col in selected_cols]

    def generate_where(self, table, alias=None):
        table_alias = alias if alias else table
        columns = list(self.schema[table].keys())
        col_name = random.choice(columns)
        col_type = self._get_column_type(table, col_name)
        col_expr = exp.column(col_name, table=table_alias)
        
        if col_type in NUMERIC_TYPES:
            op = random.choice(['=', '!=', '>', '<', '>=', '<='])
            val = random.randint(0, 1000)
            return self._create_binary_op(op, col_expr, exp.Literal.number(val))
        elif col_type in TEXT_TYPES:
            op = random.choice(['=', '!=', 'LIKE'])
            if op == 'LIKE':
                # Use email-like pattern for email columns
                if 'email' in col_name:
                    val = random.choice(['%@example.com', '%@gmail.com', 'user%@%'])
                else:
                    val = random.choice(['%a%', 'b%', '%c'])
                return exp.Like(this=col_expr, expression=exp.Literal.string(val))
            else:
                # Use realistic email for email columns
                if 'email' in col_name:
                    val = f"user{random.randint(1,100)}@example.com"
                else:
                    val = random.choice(['test', 'user', 'admin', 'activity', '123'])
                return self._create_binary_op(op, col_expr, exp.Literal.string(val))
        elif col_type in DATE_TYPES:
            op = random.choice(['>', '<', '>=', '<='])
            # SQLite date arithmetic: datetime('now', '-X days')
            days = random.randint(1, 30)
            date_expr = exp.Anonymous(this="datetime", expressions=[
                exp.Literal.string('now'),
                exp.Literal.string(f'-{days} days')
            ])
            return self._create_binary_op(op, col_expr, date_expr)
        elif col_type in BOOLEAN_TYPES:
            # SQLite uses 1/0 for booleans
            val = random.choice([1, 0])
            return exp.EQ(this=col_expr, expression=exp.Literal.number(val))
        
        return None

    def _create_binary_op(self, op, left, right):
        if op == '=': return exp.EQ(this=left, expression=right)
        if op == '!=': return exp.NEQ(this=left, expression=right)
        if op == '>': return exp.GT(this=left, expression=right)
        if op == '<': return exp.LT(this=left, expression=right)
        if op == '>=': return exp.GTE(this=left, expression=right)
        if op == '<=': return exp.LTE(this=left, expression=right)
        return exp.EQ(this=left, expression=right)

    def generate_insert(self, table):
        columns = list(self.schema[table].keys())
        # Filter out 'id' if it's an auto-increment primary key (assuming 'id' is always PK)
        columns = [c for c in columns if c != 'id']
        
        values = []
        for col in columns:
            col_type = self._get_column_type(table, col)
            if col_type in NUMERIC_TYPES:
                values.append(exp.Literal.number(random.randint(1, 1000)))
            elif col_type in TEXT_TYPES:
                if 'email' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}@example.com"))
                elif 'username' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}"))
                else:
                    values.append(exp.Literal.string(f"Sample text {random.randint(1,100)}"))
            elif col_type in DATE_TYPES:
                # SQLite: datetime('now')
                values.append(exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')]))
            elif col_type in BOOLEAN_TYPES:
                # SQLite uses 1/0 for booleans
                values.append(exp.Literal.number(random.choice([1, 0])))
            else:
                values.append(exp.Literal.string("val"))
                
        return exp.insert(
            exp.Values(expressions=[exp.Tuple(expressions=values)]),
            table,
            columns=[exp.Identifier(this=c, quoted=False) for c in columns]
        )

    def generate_update(self, table):
        columns = list(self.schema[table].keys())
        
        # Define composite primary key columns for each table
        # These should never be updated as it causes UNIQUE constraint violations
        composite_pk_tables = {
            'follows': {'follower_id', 'followee_id'},
            'likes': {'user_id', 'post_id'}
        }
        
        # Filter out primary key columns
        if table in composite_pk_tables:
            # For composite PK tables, exclude all PK columns
            pk_cols = composite_pk_tables[table]
            safe_columns = [c for c in columns if c not in pk_cols]
        else:
            # For regular tables, just exclude 'id'
            safe_columns = [c for c in columns if c != 'id']
        
        if not safe_columns:
            # If no safe columns, raise to retry with different table
            raise ValueError(f"No updatable columns for table {table}")
        
        col_to_update = random.choice(safe_columns)
        col_type = self._get_column_type(table, col_to_update)
        
        if col_type in NUMERIC_TYPES:
            val = exp.Literal.number(random.randint(1, 1000))
        elif col_type in TEXT_TYPES:
            # Use realistic email for email columns
            if 'email' in col_to_update:
                val = exp.Literal.string(f"updated_user{random.randint(1,100)}@example.com")
            else:
                val = exp.Literal.string(f"Updated text {random.randint(1,100)}")
        elif col_type in DATE_TYPES:
            # SQLite: datetime('now')
            val = exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')])
        elif col_type in BOOLEAN_TYPES:
            # SQLite uses 1/0 for booleans
            val = exp.Literal.number(random.choice([1, 0]))
        else:
            val = exp.Literal.string("val")
            
        update_expr = exp.update(table, {col_to_update: val})
        
        # Add WHERE clause
        where = self.generate_where(table)
        if where:
            update_expr = update_expr.where(where)
            
        return update_expr

    def generate_delete(self, table):
        delete_expr = exp.delete(table)
        
        # Add WHERE clause
        where = self.generate_where(table)
        if where:
            delete_expr = delete_expr.where(where)
            
        return delete_expr

    def generate_union(self):
        """Generate a UNION or UNION ALL query from two compatible SELECT statements."""
        # Pick a table for consistent columns
        table = random.choice(list(self.schema.keys()))
        columns = list(self.schema[table].keys())
        
        # Select same columns for both queries (required for UNION)
        num_cols = random.randint(2, min(4, len(columns)))
        selected_cols = random.sample(columns, num_cols)
        
        # Build first SELECT
        alias1 = f"{table[0]}1"
        query1 = exp.select(*[exp.column(c, table=alias1) for c in selected_cols])
        query1 = query1.from_(exp.to_table(table).as_(alias1))
        where1 = self.generate_where(table, alias=alias1)
        if where1:
            query1 = query1.where(where1)
        
        # Build second SELECT with different WHERE
        alias2 = f"{table[0]}2"
        query2 = exp.select(*[exp.column(c, table=alias2) for c in selected_cols])
        query2 = query2.from_(exp.to_table(table).as_(alias2))
        where2 = self.generate_where(table, alias=alias2)
        if where2:
            query2 = query2.where(where2)
        
        # Combine with UNION or UNION ALL
        use_all = random.random() < 0.4
        if use_all:
            union_query = exp.union(query1, query2, distinct=False)  # UNION ALL
        else:
            union_query = exp.union(query1, query2, distinct=True)   # UNION
        
        # Optionally add ORDER BY and LIMIT
        if random.random() < 0.3:
            union_query = union_query.order_by(exp.column(selected_cols[0]))
        if random.random() < 0.3:
            union_query = union_query.limit(random.randint(10, 50))
        
        return union_query

    def generate_join(self, current_table, current_alias, available_tables):
        # Find potential joins
        candidates = []
        for (t1, t2), (k1, k2) in self.foreign_keys.items():
            if t1 == current_table and t2 not in available_tables: # Avoid joining same table for simplicity in this level
                candidates.append((t2, k1, k2))
        
        if not candidates:
            return None
            
        target_table, left_key, right_key = random.choice(candidates)
        target_alias = f"{target_table[0]}{random.randint(1,9)}"
        
        join_condition = exp.EQ(
            this=exp.column(left_key, table=current_alias),
            expression=exp.column(right_key, table=target_alias)
        )
        
        return target_table, target_alias, join_condition

    def generate_dataset(self, num_per_complexity=300):
        """
        Generates a fixed number of queries for each complexity type.
        types: ['simple', 'join', 'advanced', 'union', 'insert', 'update', 'delete']
        """
        complexity_types = ['simple', 'join', 'advanced', 'union', 'insert', 'update', 'delete']
        dataset = []
        
        global_id_counter = 1
        for complexity in complexity_types:
            print(f"Generating {num_per_complexity} queries for complexity: {complexity}")
            count = 0
            while count < num_per_complexity:
                try:
                    query_ast, comp = self.generate_query(complexity=complexity)
                    sql_string = query_ast.sql(dialect=USED_SQL_DIALECT)
                    dataset.append({
                        "id": global_id_counter,
                        "complexity": comp,
                        "sql": sql_string,
                        "tables": [t.name for t in query_ast.find_all(exp.Table)]
                    })
                    count += 1
                    global_id_counter += 1
                except Exception as e:
                     # print(f"Retry {complexity}: {e}")
                     pass
        
        return dataset

    def generate_query(self, complexity=None):
        root_table = random.choice(list(self.schema.keys()))
        root_alias = f"{root_table[0]}1"
        
        if complexity is None:
            complexity = random.choices(
                ['simple', 'join', 'aggregate', 'advanced', 'insert', 'update', 'delete'],
                weights=[0.35, 0.25, 0.15, 0.05, 0.1, 0.05, 0.05]
            )[0]
        
        if complexity == 'insert':
            return self.generate_insert(root_table), complexity
        elif complexity == 'update':
            return self.generate_update(root_table), complexity
        elif complexity == 'delete':
            return self.generate_delete(root_table), complexity
        elif complexity == 'union':
            return self.generate_union(), complexity
            
        query = exp.select()
        query = query.from_(exp.to_table(root_table).as_(root_alias))
        
        if complexity == 'simple':
            selects = self.generate_select(root_table, alias=root_alias)
            for s in selects: query = query.select(s, copy=False)
            
            if random.random() < 0.5:
                where = self.generate_where(root_table, alias=root_alias)
                if where: query = query.where(where)
                
            if random.random() < 0.3:
                 cols = list(self.schema[root_table].keys())
                 query = query.order_by(exp.column(random.choice(cols), table=root_alias), desc=random.choice([True, False]))
                 
            if random.random() < 0.3:
                query = query.limit(random.randint(1, 100))

        elif complexity == 'join':
            # Try to add a join
            join_info = self.generate_join(root_table, root_alias, [])
            if join_info:
                target_table, target_alias, on_clause = join_info
                join_kind = random.choice(["INNER", "LEFT", "RIGHT", "FULL"])
                query = query.join(exp.to_table(target_table).as_(target_alias), on=on_clause, join_type=join_kind)
                
                # Select from both
                s1 = self.generate_select(root_table, alias=root_alias)
                s2 = self.generate_select(target_table, alias=target_alias)
                # Flatten and pick a few
                all_selects = s1 + s2
                final_selects = random.sample(all_selects, min(len(all_selects), 4))
                
                # Remove redundancy: if * is selected, keep only ONE * and remove specific columns
                has_star = any(isinstance(s, exp.Star) for s in final_selects)
                if has_star:
                    final_selects = [exp.Star()]  # Keep only a single star
                
                for s in final_selects: query = query.select(s, copy=False)
                
                if random.random() < 0.5:
                    where = self.generate_where(target_table, alias=target_alias)
                    if where: query = query.where(where)
            else:
                # If we requested a join but cannot join, this is an error for generation
                raise ValueError("Could not find a valid foreign key for join.")
# for later: have to check if we can remove this aggregate complexity type entirely; suspicious if its add value.
# Aggregates seems to have been used in select statements already? Is it?
        # elif complexity == 'aggregate':
        #     # Group by 1 column
        #     cols = list(self.schema[root_table].keys())
        #     group_col = random.choice(cols)
            
        #     selects = self.generate_select(root_table, use_aggregate=True, group_by_cols=[group_col], alias=root_alias)
        #     for s in selects: query = query.select(s, copy=False)
            
        #     query = query.group_by(exp.column(group_col, table=root_alias))
            
        #     if random.random() < 0.4:
        #         # Having count > 5
        #         query = query.having(exp.GT(this=exp.Count(this=exp.Star()), expression=exp.Literal.number(5)))

        elif complexity == 'advanced':
            # More variety in subtypes with weighted probability
            subtype = random.choices(
                ['subquery_where', 'subquery_from', 'self_join', 'exists_subquery'],
                weights=[0.35, 0.35, 0.15, 0.15]
            )[0]
            
            if subtype == 'subquery_where':
                # Subquery in WHERE: WHERE col IN (SELECT ...)
                selects = self.generate_select(root_table, alias=root_alias)
                for s in selects: query = query.select(s, copy=False)
                
                # Find a foreign key to filter on
                candidates = []
                for (t1, t2), (k1, k2) in self.foreign_keys.items():
                    if t1 == root_table:
                        candidates.append((t2, k1, k2))
                
                if candidates:
                    target_table, left_key, right_key = random.choice(candidates)
                    # Create a meaningful subquery
                    sub_alias = f"sub_{target_table[0]}"
                    subquery = exp.select(exp.column(right_key, table=sub_alias)).from_(exp.to_table(target_table).as_(sub_alias))
                    
                    # Add a filter to the subquery to make it interesting
                    sub_where = self.generate_where(target_table, alias=sub_alias)
                    if sub_where:
                        subquery = subquery.where(sub_where)
                    
                    query = query.where(exp.In(this=exp.column(left_key, table=root_alias), expressions=[subquery]))
                else:
                     # Fallback
                     raise ValueError("Could not find candidates for subquery_where")

            elif subtype == 'subquery_from':
                # Subquery in FROM: FROM (SELECT ...) AS sub
                # 1. Generate inner query
                inner_alias = f"inner_{root_table}"
                inner_query = exp.select("*").from_(exp.to_table(root_table).as_(inner_alias))
                inner_where = self.generate_where(root_table, alias=inner_alias)
                if inner_where:
                    inner_query = inner_query.where(inner_where)
                
                # 2. Wrap in outer query
                outer_alias = "derived_table"
                query = exp.select("*").from_(inner_query.subquery(alias=outer_alias))
                
                # Add a filter on the outer query if possible
                if random.random() < 0.5:
                    # Pick a column from root_table (which is in * of derived table)
                    cols = list(self.schema[root_table].keys())
                    col_name = random.choice(cols)
                    # We need to manually construct the where because generate_where expects a table name for schema lookup
                    # We can reuse generate_where but pass root_table and outer_alias
                    outer_where = self.generate_where(root_table, alias=outer_alias)
                    if outer_where:
                        query = query.where(outer_where)

            elif subtype == 'self_join':
                # Self-join with variety - pick a table that can self-join
                self_joinable = ['follows', 'users', 'posts', 'comments']
                root_table = random.choice(self_joinable)
                root_alias = f"{root_table[0]}1"
                target_alias = f"{root_table[0]}2"
                query = exp.select().from_(exp.to_table(root_table).as_(root_alias))
                
                # Different self-join patterns based on table
                if root_table == 'follows':
                    # Friend of friend pattern
                    query = query.select(
                        exp.column('follower_id', table=root_alias).as_('user'),
                        exp.column('followee_id', table=target_alias).as_('friend_of_friend')
                    )
                    join_cond = exp.EQ(
                        this=exp.column('followee_id', table=root_alias),
                        expression=exp.column('follower_id', table=target_alias)
                    )
                elif root_table == 'users':
                    # Find users with same country
                    query = query.select(
                        exp.column('username', table=root_alias).as_('user1'),
                        exp.column('username', table=target_alias).as_('user2'),
                        exp.column('country_code', table=root_alias)
                    )
                    join_cond = exp.And(
                        this=exp.EQ(
                            this=exp.column('country_code', table=root_alias),
                            expression=exp.column('country_code', table=target_alias)
                        ),
                        expression=exp.NEQ(
                            this=exp.column('id', table=root_alias),
                            expression=exp.column('id', table=target_alias)
                        )
                    )
                elif root_table == 'posts':
                    # Find posts by same user
                    query = query.select(
                        exp.column('id', table=root_alias).as_('post1'),
                        exp.column('id', table=target_alias).as_('post2'),
                        exp.column('user_id', table=root_alias)
                    )
                    join_cond = exp.And(
                        this=exp.EQ(
                            this=exp.column('user_id', table=root_alias),
                            expression=exp.column('user_id', table=target_alias)
                        ),
                        expression=exp.LT(
                            this=exp.column('id', table=root_alias),
                            expression=exp.column('id', table=target_alias)
                        )
                    )
                else:  # comments
                    # Find comments on same post
                    query = query.select(
                        exp.column('id', table=root_alias).as_('comment1'),
                        exp.column('id', table=target_alias).as_('comment2'),
                        exp.column('post_id', table=root_alias)
                    )
                    join_cond = exp.And(
                        this=exp.EQ(
                            this=exp.column('post_id', table=root_alias),
                            expression=exp.column('post_id', table=target_alias)
                        ),
                        expression=exp.NEQ(
                            this=exp.column('id', table=root_alias),
                            expression=exp.column('id', table=target_alias)
                        )
                    )
                
                query = query.join(exp.to_table(root_table).as_(target_alias), on=join_cond)
                
                if random.random() < 0.3:
                    query = query.limit(random.randint(5, 20))

            elif subtype == 'exists_subquery':
                # EXISTS/NOT EXISTS subquery
                selects = self.generate_select(root_table, alias=root_alias)
                for s in selects: query = query.select(s, copy=False)
                
                # Find a related table for EXISTS check
                candidates = [(t2, k1, k2) for (t1, t2), (k1, k2) in self.foreign_keys.items() if t1 == root_table]
                if candidates:
                    target_table, left_key, right_key = random.choice(candidates)
                    sub_alias = f"sub_{target_table[0]}"
                    
                    # Build EXISTS subquery
                    subquery = exp.select(exp.Literal.number(1)).from_(exp.to_table(target_table).as_(sub_alias))
                    subquery = subquery.where(exp.EQ(
                        this=exp.column(right_key, table=sub_alias),
                        expression=exp.column(left_key, table=root_alias)
                    ))
                    
                    # Add extra filter
                    sub_where = self.generate_where(target_table, alias=sub_alias)
                    if sub_where:
                        subquery = subquery.where(sub_where)
                    
                    # Randomly use EXISTS or NOT EXISTS
                    exists_expr = exp.Exists(this=subquery)
                    if random.random() < 0.3:
                        query = query.where(exp.Not(this=exists_expr))
                    else:
                        query = query.where(exists_expr)
                else:
                    raise ValueError("Could not find candidates for exists_subquery")

        return query, complexity
