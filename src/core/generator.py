import random
from sqlglot import exp
from src.core.schema_config import SchemaConfig


class SQLQueryGenerator:
    def __init__(self, schema_config: SchemaConfig):
        self.cfg = schema_config
        self.dialect = self.cfg.dialect
        self.queries = []

        # Precompute FK pairs in the {(t1,t2): (c1,c2)} shape used by
        # generate_join and the complexity handlers.
        self.fk_pairs = self.cfg.get_fk_pairs()

        # Derive composite PK tables: tables without an 'id' column whose
        # FK columns form a composite key.
        self.composite_pks = {}
        for tname, tdef in self.cfg.tables.items():
            if "id" not in tdef.columns:
                fk_cols = {c.name for c in tdef.columns.values() if c.is_fk}
                if fk_cols:
                    self.composite_pks[tname] = fk_cols

    # ── column helpers ────────────────────────────────────────────────

    def _get_column_type(self, table, column):
        return self.cfg.tables[table].columns[column].col_type

    def _get_column_names(self, table):
        """Return the list of column names for *table*."""
        return list(self.cfg.tables[table].columns.keys())

    def _table_names(self):
        """Return the list of all table names in the schema."""
        return list(self.cfg.tables.keys())

    def generate_select(self, table, use_aggregate=False, group_by_cols=None, alias=None):
        table_alias = alias if alias else table
        columns = self._get_column_names(table)
        
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
            elif self._get_column_type(table, agg_col) in self.cfg.numeric_types:
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
        columns = self._get_column_names(table)
        col_name = random.choice(columns)
        col_type = self._get_column_type(table, col_name)
        col_expr = exp.column(col_name, table=table_alias)
        
        if col_type in self.cfg.numeric_types:
            op = random.choice(['=', '!=', '>', '<', '>=', '<='])
            val = random.randint(0, 1000)
            return self._create_binary_op(op, col_expr, exp.Literal.number(val))
        elif col_type in self.cfg.text_types:
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
        elif col_type in self.cfg.date_types:
            op = random.choice(['>', '<', '>=', '<='])
            # SQLite date arithmetic: datetime('now', '-X days')
            days = random.randint(1, 30)
            date_expr = exp.Anonymous(this="datetime", expressions=[
                exp.Literal.string('now'),
                exp.Literal.string(f'-{days} days')
            ])
            return self._create_binary_op(op, col_expr, date_expr)
        elif col_type in self.cfg.boolean_types:
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
        columns = self._get_column_names(table)
        # Filter out 'id' if it's an auto-increment primary key (assuming 'id' is always PK)
        columns = [c for c in columns if c != 'id']
        
        values = []
        for col in columns:
            col_type = self._get_column_type(table, col)
            if col_type in self.cfg.numeric_types:
                values.append(exp.Literal.number(random.randint(1, 1000)))
            elif col_type in self.cfg.text_types:
                if 'email' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}@example.com"))
                elif 'username' in col:
                    values.append(exp.Literal.string(f"user{random.randint(1,1000)}"))
                else:
                    values.append(exp.Literal.string(f"Sample text {random.randint(1,100)}"))
            elif col_type in self.cfg.date_types:
                # SQLite: datetime('now')
                values.append(exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')]))
            elif col_type in self.cfg.boolean_types:
                # SQLite uses 1/0 for booleans
                values.append(exp.Literal.number(random.choice([1, 0])))
            else:
                values.append(exp.Literal.string("val"))
                
        return exp.insert(
            exp.Values(expressions=[exp.Tuple(expressions=values)]),
            table,
            columns=[exp.Identifier(this=c, quoted=True) for c in columns]
        )

    def generate_update(self, table):
        columns = self._get_column_names(table)
        
        # Filter out primary key columns
        if table in self.composite_pks:
            # For composite PK tables, exclude all PK columns
            pk_cols = self.composite_pks[table]
            safe_columns = [c for c in columns if c not in pk_cols]
        else:
            # For regular tables, just exclude 'id'
            safe_columns = [c for c in columns if c != 'id']
        
        if not safe_columns:
            # If no safe columns, raise to retry with different table
            raise ValueError(f"No updatable columns for table {table}")
        
        col_to_update = random.choice(safe_columns)
        col_type = self._get_column_type(table, col_to_update)
        
        if col_type in self.cfg.numeric_types:
            val = exp.Literal.number(random.randint(1, 1000))
        elif col_type in self.cfg.text_types:
            # Use realistic email for email columns
            if 'email' in col_to_update:
                val = exp.Literal.string(f"updated_user{random.randint(1,100)}@example.com")
            else:
                val = exp.Literal.string(f"Updated text {random.randint(1,100)}")
        elif col_type in self.cfg.date_types:
            # SQLite: datetime('now')
            val = exp.Anonymous(this="datetime", expressions=[exp.Literal.string('now')])
        elif col_type in self.cfg.boolean_types:
            # SQLite uses 1/0 for booleans
            val = exp.Literal.number(random.choice([1, 0]))
        else:
            val = exp.Literal.string("val")
            
        update_expr = exp.update(table, {exp.column(col_to_update): val})
        
        # Add WHERE clause
        where = self.generate_where(table)
        if where:
            update_expr = update_expr.where(where)
            
        return update_expr

    def generate_delete(self, table):
        delete_expr = exp.delete(table)
        
        # Always add WHERE clause — retry with different columns if needed
        where = None
        for _ in range(10):
            where = self.generate_where(table)
            if where:
                break
        if where:
            delete_expr = delete_expr.where(where)
        else:
            # Ultimate fallback: DELETE WHERE id > 0
            if 'id' in self.cfg.tables[table].columns:
                delete_expr = delete_expr.where(
                    exp.GT(this=exp.column('id', table=table),
                           expression=exp.Literal.number(0)))
            
        return delete_expr

    def generate_union(self):
        """Generate a UNION or UNION ALL query from two compatible SELECT statements."""
        # Pick a table for consistent columns
        table = random.choice(self._table_names())
        columns = self._get_column_names(table)
        
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
        for (t1, t2), (k1, k2) in self.fk_pairs.items():
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
        Uses the complexity registry for extensibility.
        """
        from src.complexity.registry import all_handler_names
        
        complexity_types = all_handler_names()
        dataset = []
        
        global_id_counter = 1
        for complexity in complexity_types:
            print(f"Generating {num_per_complexity} queries for complexity: {complexity}")
            count = 0
            while count < num_per_complexity:
                try:
                    query_ast, comp = self.generate_query(complexity=complexity)
                    sql_string = query_ast.sql(dialect=self.dialect)
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
        """
        Generate a single SQL query using the complexity registry.
        
        Each complexity type is handled by a registered ComplexityHandler
        that receives this generator instance for access to shared helpers.
        """
        from src.complexity.registry import get_handler, all_handler_names
        
        root_table = random.choice(self._table_names())
        root_alias = f"{root_table[0]}1"
        
        if complexity is None:
            complexity = random.choice(all_handler_names())
        
        handler = get_handler(complexity)
        return handler.generate(self, root_table, root_alias)
