"""
Syntax-Directed Translation (SDT) Framework for SQL to Natural Language
This module provides deterministic, template-based rendering of SQL ASTs to NL prompts.
"""

import random
import re
from typing import Dict, Optional, Any
from sqlglot import exp
from src.core.schema_config import SchemaConfig
from src.core.linguistic_dictionary import LinguisticDictionary
from src.perturbations.base import PerturbationStrategy


class _NullStrategy(PerturbationStrategy):
    """Default no-op strategy — all hooks return the default value."""
    name = "_null"
    display_name = "No Perturbation"
    description = "No perturbation applied."
    layer = "template"

    def is_applicable(self, ast, nl_text, context):
        return False

    def apply(self, nl_text, ast, rng, context):
        return nl_text

    def was_applied(self, baseline_nl, perturbed_nl, context):
        return False, ""


class SQLToNLRenderer:
    """Renders SQL AST nodes to natural language using deterministic templates."""
    
    def __init__(self, seed: int = 42, *,
                 schema_config: Optional[SchemaConfig] = None,
                 strategy: Optional[PerturbationStrategy] = None,
                 dictionary: Optional[LinguisticDictionary] = None):
        self._seed = seed
        self._strategy = strategy or _NullStrategy()
        self._ambig_pronoun_count = 0
        self._emit_mode = 'text'  # 'text' for final NL, 'template' for IR tokens
        # Derive FK lookup from SchemaConfig (if provided)
        self.foreign_keys = schema_config.get_fk_pairs() if schema_config else {}
        
        # Data Banks: The first element MUST be the canonical word from the original dataset
        self.synonyms = {
            'get': ["Get", "Retrieve", "Find", "Pull up", "Dig out", "Go get", "Fetch me"],
            'select': ["Select", "Pick out", "Spot", "Single out", "choose"],
            'show': ["Show", "Display", "Bring up", "Give me a look at", "Run a check for", "Produce a listing of"],
            'where': ["where", "filtered for", "looking only at", "for which", "that have"],
            'from': ['from', 'in', 'within', 'out of'],
            'equals': ["equals", "is", "matches", "is equal to", "="],
            'and': ["and", "where also", "as well as", "along with"],
            'joined with': ['joined with', 'linked to', 'connected to', 'join'],
            'insert': ["Add", "Insert", "Put", "Include", "Create"],
            'update': ["Update", "Change", "Modify", "Adjust", "Edit"],
            'delete': ["Remove", "Delete", "Drop", "Strip out", "Wipe out"],
        }
        
        self.canonical_ops = {
            'eq': 'equals',
            'neq': 'is not equal to',
            'gt': 'greater than',
            'lt': 'less than',
            'gte': 'greater than or equal to',
            'lte': 'less than or equal to'
        }
        
        # Tracking for pronoun logic
        self._mentions = set()
        self._recent_mentions = [] # List of (type, name) for "former/latter"
        self._use_pronouns = False

        self.operators = {
            'eq': 'equals',
            'neq': 'is not equal to',
            'gt': 'greater than',
            'lt': 'less than',
            'gte': 'greater than or equal to',
            'lte': 'less than or equal to'
        }

        # Schema-specific table/column synonyms.
        # Populated from a LinguisticDictionary when provided.
        self.schema_synonyms = {}
        if dictionary:
            self._load_synonyms_from_dictionary(dictionary)

        self.op_variations = {
            "gt": ["exceeds", "more than", "above", "higher than"],
            "lt": ["below", "under", "fewer than", "lower than"],
            "gte": ["at least", "minimum of", "no less than"],
            "lte": ["at most", "maximum of", "no more than"]
        }

        # Temporal-safe operator variants (semantically correct for date comparisons)
        self.temporal_op_variations = {
            "gt":  ["more recent than", "after", "since"],           # > date = newer
            "lt":  ["earlier than", "before", "prior to"],           # < date = older
            "gte": ["on or after", "starting from", "from"],         # >= date = newer inclusive
            "lte": ["on or before", "up to", "through"],             # <= date = older inclusive
        }
        # Temporal ops that need a directional suffix (e.g., "from X ago" -> "from X ago onwards")
        self.temporal_op_suffixes = {
            "from": "onwards",
            "starting from": "onwards",
        }

        self.agg_variations = {
            "COUNT": ["total number of", "how many", "count of", "number of"],
            "SUM": ["total", "sum of", "add up"],
            "AVG": ["average", "mean", "typical"],
            "MAX": ["maximum", "highest", "largest"],
            "MIN": ["minimum", "lowest", "smallest"]
        }

    def _is_technical_alias(self, alias: str) -> bool:
        """Check if an alias is likely machine-generated or boilerplate."""
        if not alias:
            return False
        # Matches u1, f1, p2, etc. (table first letter + number)
        if re.match(r'^[a-z]\d+$', alias.lower()):
            return True
        # Matches inner_..., sub_..., subquery, derived_table, subq
        if any(alias.lower().startswith(prefix) for prefix in ['inner_', 'sub_', 'subquery', 'derived_table', 'subq']):
            return True
        return False

    def _analyze_aliases(self, node) -> Dict[str, int]:
        """
        Analyze the query to count occurrences of table types.
        Returns a dict mapping base table type to count (e.g., {'users': 1, 'posts': 2}).
        Used for deciding if 'u1' can be safely mapped to "the user's" (count=1) or must be preserved (count>1).
        """
        counts = {}
        for table in node.find_all(exp.Table):
            alias = table.alias
            if alias:
                base_type = self._get_base_type_from_alias(alias)
                if base_type:
                    counts[base_type] = counts.get(base_type, 0) + 1
        return counts

    def _is_single_table_context(self, node) -> bool:
        """Check if the query involves only one table."""
        tables = list(node.find_all(exp.Table))
        if len(tables) != 1:
            return False
        return True

    def _get_base_type_from_alias(self, alias: str) -> Optional[str]:
        """Heuristic to get base table type from alias (u1 -> users)."""
        if not alias: return None
        a = alias.lower()
        if a.startswith('u') and a[1:].isdigit(): return 'users'
        if a.startswith('p') and a[1:].isdigit(): return 'posts'
        if a.startswith('c') and a[1:].isdigit(): return 'comments'
        if a.startswith('l') and a[1:].isdigit(): return 'likes'
        if a.startswith('f') and a[1:].isdigit(): return 'follows'
        return None

    def _load_synonyms_from_dictionary(self, dictionary: LinguisticDictionary):
        """Populate schema_synonyms from a LinguisticDictionary."""
        for tname, syns in dictionary.table_synonyms.items():
            self.schema_synonyms[tname.lower()] = syns
        for qualified_key, syns in dictionary.column_synonyms.items():
            # "table.column" -> column (renderer looks up by column name)
            col_name = qualified_key.split(".", 1)[-1]
            key = col_name.lower()
            if key not in self.schema_synonyms:
                self.schema_synonyms[key] = syns
            else:
                # Merge, keeping unique entries in order
                existing = set(s.lower() for s in self.schema_synonyms[key])
                for s in syns:
                    if s.lower() not in existing:
                        self.schema_synonyms[key].append(s)
                        existing.add(s.lower())

    def _get_rng(self, context: str = "") -> random.Random:
        return self._rng

    def _choose_word(self, key, context):
        """Chooses a word randomly from the synonym bank for a given key."""
        options = self.synonyms.get(key.lower(), [key])
        
        # ALWAYS consume exactly one choice from the main RNG to keep sequence in sync
        baseline_choice = self._rng.choice(options)
        
        # Template mode: emit IR token instead of resolved text
        if self._emit_mode == 'template':
            _verb_keys = {'get', 'select', 'show', 'insert', 'update', 'delete'}
            if key.lower() in _verb_keys:
                return f'[VERB:{key}]'
            return f'[CONN:{key}]'
        
        # Hook: let strategy pick verb synonyms
        _verb_keys = {'get', 'select', 'show', 'insert', 'update', 'delete'}
        if key.lower() in _verb_keys:
            return self._strategy.on_verb(
                key, baseline_choice,
                random.Random(f"{self._seed}_{key}_{context}_alt"))
            
        return baseline_choice

    def render(self, ast) -> str:
        self._rng = random.Random(self._seed)
        self._ambig_pronoun_count = 0 
        self._mentions = set()
        self._recent_mentions = [] 

        # Detect self-joins — pronouns are unresolvable when the same table appears twice
        self._is_self_join = False
        table_names = []
        for t in ast.find_all(exp.Table):
            tname = t.name.lower() if hasattr(t, 'name') else ''
            if tname and not tname.startswith('inner_') and tname != 'derived_table':
                table_names.append(tname)
        self._is_self_join = len(table_names) != len(set(table_names))
        
        if isinstance(ast, exp.Select):
            base_nl = self.render_select(ast)
        elif isinstance(ast, exp.Union):
            base_nl = self.render_union(ast)
        elif isinstance(ast, exp.Insert):
            base_nl = self.render_insert(ast)
        elif isinstance(ast, exp.Update):
            base_nl = self.render_update(ast)
        elif isinstance(ast, exp.Delete):
            base_nl = self.render_delete(ast)
        else:
            base_nl = str(ast)

        return base_nl

    def render_template(self, ast) -> str:
        """Pass 1 of two-pass rendering: produce IR template with [TYPE:value] tokens.

        Token format:
            [TABLE:users]   - Table reference
            [COL:email]     - Column reference (unqualified)
            [COL:users.email] - Column reference (table-qualified)
            [OP:gt]         - Comparison operator
            [AGG:COUNT]     - Aggregate function
            [VAL:42]        - Literal value
            [VERB:get]      - Action verb key
            [CONN:where]    - Structural connector

        The template preserves all structural decisions (join type, clause
        ordering, qualification) but defers word-choice to the resolver
        (Pass 2).
        """
        # Save state
        saved_strategy = self._strategy
        saved_emit = self._emit_mode

        # Set template mode with clean strategy (no perturbations)
        self._strategy = _NullStrategy()
        self._emit_mode = 'template'
        self._rng = random.Random(self._seed)
        self._ambig_pronoun_count = 0
        self._mentions = set()
        self._recent_mentions = []
        self._is_self_join = False

        try:
            if isinstance(ast, exp.Select):
                result = self.render_select(ast)
            elif isinstance(ast, exp.Union):
                result = self.render_union(ast)
            elif isinstance(ast, exp.Insert):
                result = self.render_insert(ast)
            elif isinstance(ast, exp.Update):
                result = self.render_update(ast)
            elif isinstance(ast, exp.Delete):
                result = self.render_delete(ast)
            else:
                result = str(ast)
            return result
        finally:
            # Restore state
            self._strategy = saved_strategy
            self._emit_mode = saved_emit


    def render_select(self, node):
        self._alias_counts = self._analyze_aliases(node)
        self._is_single_table = self._is_single_table_context(node)
        
        parts = []
        # ALWAYS roll a verb to keep the RNG sequence in sync
        intent_key = self._rng.choice(['get', 'select', 'show'])
        intent_verb = self._choose_word(intent_key, 'verb')
        
        # Hook: strategy chooses between raw SQL keyword or NL verb
        parts.append(self._strategy.on_keyword("SELECT", intent_verb))
        
        # Columns
        col_list = ", ".join([self._render_expression(e, f"col_{i}") for i, e in enumerate(node.expressions)])
        if "all columns" in col_list:
             parts.append(col_list)
        else:
             # Avoid doubled article ("the the user's")
             if col_list.lower().startswith("the "):
                 parts.append(col_list)
             else:
                 parts.append(f"the {col_list}")
        
        # FROM keyword
        from_word = self._choose_word('from', 'from_kw')
        from_kw = self._strategy.on_keyword("FROM", from_word)
        if from_kw:
            parts.append(from_kw)
        
        # Table reference with alias handling
        from_node = node.args.get('from_')
        raw_table_name = None
        if from_node:
            table_name = self._render_table(from_node.this, "main_table")
            raw_table_name = from_node.this.this.name if hasattr(from_node.this, 'this') and hasattr(from_node.this.this, 'name') else str(from_node.this.this)
            alias = from_node.this.alias if hasattr(from_node.this, 'alias') else ""
            show_alias = self._strategy.on_keyword("ALIAS", "show") != ""
            if alias and show_alias and not self._is_technical_alias(alias):
                parts.append(f"{table_name} (as {alias})")
            else:
                parts.append(table_name)
            
        # JOINs
        current_table = raw_table_name
        for i, join in enumerate(node.args.get('joins', [])):
            join_str, next_table = self._render_join(join, f"join_{i}", current_table)
            parts.append(join_str)
            current_table = next_table
            
        # WHERE
        where_node = node.args.get('where')
        if where_node:
            where_word = self._choose_word('where', 'where_kw')
            where_kw = self._strategy.on_keyword("WHERE", where_word)
            if where_kw:
                parts.append(where_kw)
            parts.append(self._render_expression(where_node.this, "where_cond"))
        
        order_parts = self._render_order_clause(node, "select_ord")
        if order_parts:
            parts.append(order_parts)
        
        limit_parts = self._render_limit_clause(node)
        if limit_parts:
            parts.append(limit_parts)
            
        return " ".join(parts)

    def render_union(self, node) -> str:
        """Render UNION/UNION ALL queries by combining left and right sides."""
        left = self.render(node.left) if node.left else ""
        right = self.render(node.right) if node.right else ""
        
        # Remove trailing periods from sub-queries for cleaner joining
        left = left.rstrip('.')
        right = right.rstrip('.')
        
        # Distinguish UNION ALL (keep duplicates) from UNION (remove duplicates)
        if node.args.get('distinct') is False:
            connector = "combined with (including duplicates)"
        else:
            connector = "combined with (removing duplicates)"
        
        result = f"{left}, {connector} {right}"
        
        # Add top-level clauses (ORDER BY, LIMIT) for UNION
        order_parts = self._render_order_clause(node, "union_ord")
        if order_parts:
            result += f" {order_parts}"
            
        limit_parts = self._render_limit_clause(node)
        if limit_parts:
            result += f" {limit_parts}"
            
        return result

    def render_insert(self, node) -> str:
        """Render INSERT statement to natural language."""
        self._is_single_table = self._is_single_table_context(node)

        # Get table name from Schema node
        schema = node.this
        table = schema.this.name if hasattr(schema, 'this') else str(schema)
        table_nl = self._render_table(schema.this if hasattr(schema, 'this') else schema, "ins_table")
        
        # Get column names
        columns = []
        if hasattr(schema, 'expressions') and schema.expressions:
            columns = [self._render_column(col, f"ins_col_{i}") for i, col in enumerate(schema.expressions)]
        
        # Get values
        values_expr = node.expression
        values = []
        if values_expr:
            for tuple_expr in values_expr.expressions:
                tuple_vals = [self._render_expression(v, f"val_{i}") for i, v in enumerate(tuple_expr.expressions)]
                values.append(f"({', '.join(tuple_vals)})")
        
        verb = self._choose_word('insert', 'ins_verb')
        if not columns or not values:
             return f"{verb} a new record to {table_nl}."

        # Simplify for single record insert
        if len(values) == 1:
             val_list = values[0].strip('()').split(',')
             assignments = [f"{c} as {v.strip()}" for c, v in zip(columns, val_list)]
             return f"{verb} a new {self._singularize(table_nl)} with {', '.join(assignments)}."
        
        col_str = f"({', '.join(columns)})"
        val_str = ", ".join(values)
        
        return f"{verb} into {table_nl} {col_str} values {val_str}"

    def render_update(self, node) -> str:
        """Render UPDATE statement to natural language."""
        self._is_single_table = self._is_single_table_context(node)

        # Get table name
        table_nl = self._render_table(node.this, "upd_table")
        
        # Get SET assignments (node.expressions contains EQ nodes)
        assignments = []
        for eq_expr in node.expressions:
            col = self._render_expression(eq_expr.this, "upd_col")
            val = self._render_expression(eq_expr.expression, "upd_val")
            assignments.append(f"{col} to {val}")
        
        set_str = " and ".join(assignments) if assignments else ""
        
        # Get WHERE clause
        where_node = node.args.get('where')
        where_str = ""
        if where_node:
            kw = self._strategy.on_keyword("WHERE", "where")
            singular = self._singularize(table_nl)
            if kw:
                where_str = f" for the {singular} {kw} {self._render_expression(where_node.this, 'upd_where')}"
            else:
                where_str = f" for the {singular} {self._render_expression(where_node.this, 'upd_where')}"
        else:
            where_str = f" for all {table_nl}"
        
        verb = self._choose_word('update', 'upd_verb')
        return f"{verb} {set_str}{where_str}"

    def render_delete(self, node) -> str:
        """Render DELETE statement to natural language."""
        self._is_single_table = self._is_single_table_context(node)

        # Get table name
        table_nl = self._render_table(node.this, "del_table")
        
        # Get WHERE clause
        where_node = node.args.get('where')
        where_str = ""
        if where_node:
            kw = self._strategy.on_keyword("WHERE", "where")
            if kw:
                 where_str = f" {kw} {self._render_expression(where_node.this, 'del_where')}"
            else:
                 where_str = f" {self._render_expression(where_node.this, 'del_where')}"
        
        verb = self._choose_word('delete', 'del_verb')
        return f"{verb} {table_nl}{where_str}"

    def _render_join(self, join_node, context, left_table_name=None):
        table = self._render_table(join_node.this, context)
        # Attempt to get the raw table name (not the NL version)
        right_table_name = join_node.this.this.name if hasattr(join_node.this, 'this') and hasattr(join_node.this.this, 'name') else str(join_node.this.this)
        
        # ALWAYS roll for incomplete join spec to keep sequence in sync
        choice_incomplete = self._rng.choice(['with', 'along with'])
        
        # Compute full join rendering (default path)
        on = join_node.args.get('on')
        on_str = ""
        is_standard_join = False
        if on:
             on_str = f" on {self._render_expression(on, context + '_on')}"
             # Check if this is a standard FK join
             if left_table_name and right_table_name:
                 fk = self.foreign_keys.get((left_table_name, right_table_name))
                 if fk:
                      on_text = str(on).lower()
                      if fk[0].lower() in on_text and fk[1].lower() in on_text:
                           is_standard_join = True
                 else:
                      fk = self.foreign_keys.get((right_table_name, left_table_name))
                      if fk and fk[0].lower() in str(on).lower() and fk[1].lower() in str(on).lower():
                           is_standard_join = True

        # Preserve join type (LEFT, RIGHT, INNER, etc.)
        join_side = join_node.side.upper() if join_node.side else ""
        join_kind = join_node.kind.upper() if join_node.kind else ""
        
        # Natural rendering for common joins
        if not join_side and join_kind == "INNER":
             if is_standard_join:
                  default_phrase = f"and their {table}"
             else:
                  default_phrase = f"joined with {table}{on_str}"
        elif join_side == "LEFT" and join_kind == "OUTER":
             if is_standard_join:
                  default_phrase = f"along with their {table} if any"
             else:
                  default_phrase = f"left-joined with {table}{on_str}"
        else:
            # Combine side and kind, defaulting to just "JOIN" if neither exists
            join_type = f"{join_side} {join_kind}".strip()
            if not join_type:
                join_type = "JOIN"
            elif "JOIN" not in join_type:
                join_type = f"{join_type} JOIN"
            default_phrase = f"{join_type} {table}{on_str}"
        
        # Hook: let strategy override join phrasing
        has_fk = ((left_table_name, right_table_name) in self.foreign_keys or 
                   (right_table_name, left_table_name) in self.foreign_keys) if left_table_name and right_table_name else False
        final = self._strategy.on_join(
            table_nl=table,
            on_str=on_str,
            default_phrase=default_phrase,
            context={
                "left_table": left_table_name,
                "right_table": right_table_name,
                "has_fk": has_fk,
                "join_side": join_side,
                "join_kind": join_kind,
                "choice_incomplete": choice_incomplete,
                "is_standard_join": is_standard_join,
            },
        )
        return final, right_table_name

    def _render_expression(self, expr, context):
        rng = self._get_rng(context)
        
        # Aggregates — ALWAYS roll to keep RNG in sync
        agg_key = expr.key.upper() if getattr(expr, 'key', None) else ""
        agg_options = self.agg_variations.get(agg_key, ["value of"])
        agg_template = self._rng.choice(agg_options)
        
        if isinstance(expr, exp.AggFunc):
            if self._emit_mode == 'template':
                inner = self._render_expression(expr.this, context)
                return f"[AGG:{agg_key}] {inner}"
            inner_text = self._render_expression(expr.this, context)
            # Hook: default is just the inner text (baseline behavior)
            return self._strategy.on_aggregate(agg_key, inner_text, inner_text, agg_template)

        if isinstance(expr, exp.Star):
            return "all columns"
        
        if isinstance(expr, exp.DateSub):
            rendered = self._render_date_sub(expr, context)
            return f"[VAL:{rendered}]" if self._emit_mode == 'template' else rendered
        
        # Handle SQLite datetime() function: datetime('now') or datetime('now', '-X days')
        if isinstance(expr, exp.Anonymous) and str(expr.this).lower() == 'datetime':
            rendered = self._render_sqlite_datetime(expr, context)
            return f"[VAL:{rendered}]" if self._emit_mode == 'template' else rendered
        
        if isinstance(expr, exp.Datetime):
            rendered = self._render_datetime_node(expr, context)
            return f"[VAL:{rendered}]" if self._emit_mode == 'template' else rendered
        
        # Handle standalone NOW() function calls
        if isinstance(expr, exp.CurrentTimestamp):
            return "[VAL:the current time]" if self._emit_mode == 'template' else "the current time"

        # Operators — ALWAYS roll to keep RNG in sync
        op_options = self.op_variations.get(expr.key, ["matches"])
        op_template = self._rng.choice(op_options)
        
        if isinstance(expr, (exp.GT, exp.LT, exp.GTE, exp.LTE, exp.EQ, exp.NEQ)):
            left = self._render_expression(expr.left, context+'_l')
            right = self._render_expression(expr.right, context+'_r')

            # Template mode: emit clean operator token
            if self._emit_mode == 'template':
                return f"{left} [OP:{expr.key}] {right}"

            # Detect if right-hand side is a temporal phrase
            self_contained_temporal = ["within the last", "in the past", "older than", "over the last"]
            is_self_contained = any(t in right for t in self_contained_temporal)
            has_temporal_anchor = "ago" in right or "from now" in right

            # Compute baseline default (no perturbation)
            if is_self_contained:
                default_str = f"{left} {right}"
            elif expr.key in ('gt', 'gte') and "ago" in right and "within" not in right:
                parts = right.split(" ")
                if len(parts) >= 2:
                    duration = " ".join(parts[:-1])
                    suffix = " inclusive" if expr.key == 'gte' else ""
                    default_str = f"{left} within the last {duration}{suffix}"
                else:
                    op_str = self.operators.get(expr.key, expr.key)
                    default_str = f"{left} {op_str} {right}"
            elif expr.key in ('lt', 'lte') and "ago" in right:
                parts = right.split(" ")
                if len(parts) >= 2:
                    duration = " ".join(parts[:-1])
                    suffix = " inclusive" if expr.key == 'lte' else ""
                    default_str = f"{left} older than {duration}{suffix}"
                else:
                    op_str = self.operators.get(expr.key, expr.key)
                    default_str = f"{left} {op_str} {right}"
            else:
                op_str = self.operators.get(expr.key, expr.key) 
                default_str = f"{left} {op_str} {right}"

            # Hook: let strategy vary operator phrasing
            return self._strategy.on_operator(
                expr.key, left, right, default_str,
                context={
                    "is_temporal": has_temporal_anchor or is_self_contained,
                    "has_temporal_anchor": has_temporal_anchor,
                    "is_self_contained": is_self_contained,
                    "op_template": op_template,
                    "temporal_op_variations": self.temporal_op_variations,
                    "temporal_op_suffixes": self.temporal_op_suffixes,
                    "rng": self._rng,
                })

        # Temporal — ALWAYS roll to keep RNG in sync
        temp_options = ["recently", "since last year", "this month"]
        temp_choice = self._rng.choice(temp_options)
        
        if isinstance(expr, exp.Literal):
            val = str(expr.this)
            if re.search(r'\d{4}-\d{2}-\d{2}', val):
                # Hook: let strategy replace ISO dates with temporal expressions
                default_literal = f"'{val}'" if expr.is_string else val
                return self._strategy.on_temporal(val, default_literal, self._rng)

        if isinstance(expr, exp.In):
            left = self._render_expression(expr.this, context + '_l')

            # sqlglot stores IN subqueries differently depending on origin:
            #   - parsed from SQL:       expr.args['query'] = exp.Subquery(this=Select(...))
            #   - built programmatically: expr.expressions = [Select(...)] or [Subquery(...)]
            raw_subquery = expr.args.get('query')

            # If 'query' is None, look for a Select/Subquery node in expr.expressions
            if raw_subquery is None and expr.expressions:
                first = expr.expressions[0]
                if isinstance(first, (exp.Select, exp.Subquery)):
                    raw_subquery = first

            if raw_subquery is not None:
                # Unwrap exp.Subquery wrapper if present
                inner_query = raw_subquery.this if isinstance(raw_subquery, exp.Subquery) else raw_subquery

                # Narratize IN (SELECT ...) naturally
                if isinstance(inner_query, exp.Select):
                    target_cols = [self._render_expression(e, f"{context}_in_col") for e in inner_query.expressions]
                    target_col = target_cols[0] if target_cols else "value"

                    from_node = inner_query.args.get('from_')
                    table_name = ""
                    if from_node:
                        table_name = self._render_table(from_node.this, context + "_in_tbl")

                    where_node = inner_query.args.get('where')
                    where_str = ""
                    if where_node:
                        where_str = f" where {self._render_expression(where_node.this, context + '_in_where')}"

                    return f"{left} matches any of the {target_col}s from {table_name}{where_str}"

                subquery_nl = self._render_subquery(inner_query, context + '_sub')
                return f"{left} is in ({subquery_nl})"

            # Handle IN with a scalar literal list (IN (1, 2, 3) / IN ('a', 'b'))
            # Guard: skip if any entry is a Select/Subquery (should have been handled above)
            if expr.expressions and not any(isinstance(e, (exp.Select, exp.Subquery)) for e in expr.expressions):
                values = ", ".join([self._render_expression(e, f"{context}_v{i}") for i, e in enumerate(expr.expressions)])
                return f"{left} is in [{values}]"
            return f"{left} is in (unknown)"

        if isinstance(expr, exp.Exists):
            subquery = expr.this
            # Narratize EXISTS: "where there is a corresponding [table] who [where]"
            if isinstance(subquery, exp.Select):
                from_node = subquery.args.get('from_')
                if from_node:
                    table_name = self._render_table(from_node.this, context + "_ex_tbl")
                    where_node = subquery.args.get('where')
                    where_str = ""
                    if where_node:
                        # Omit "WHERE" keyword for flow
                        where_str = f" who {self._render_expression(where_node.this, context + '_ex_where')}"
                    return f"where there is a corresponding {table_name}{where_str}"
            
            subquery_nl = self._render_subquery(subquery, context + '_exists')
            return f"exists ({subquery_nl})"
        
        if isinstance(expr, exp.Not):
            # Handle NOT EXISTS naturally
            if isinstance(expr.this, exp.Exists):
                 inner = self._render_expression(expr.this, context + '_not_exists')
                 return inner.replace("where there is a corresponding", "where there is no corresponding")
            
            inner = self._render_expression(expr.this, context + '_not')
            return f"NOT {inner}"

        # Base recursions
        if isinstance(expr, exp.Column): return self._render_column(expr, context)
        if isinstance(expr, exp.Table): return self._render_table(expr, context)
        
        if isinstance(expr, exp.Literal):
            if self._emit_mode == 'template':
                if expr.is_string:
                    return f"[VAL:'{expr.this}']"
                return f"[VAL:{expr.this}]"
            if expr.is_string:
                return f"'{expr.this}'"  # Quote string literals
            return str(expr.this)
        
        if isinstance(expr, exp.Boolean):
            if self._emit_mode == 'template':
                return f"[VAL:{'TRUE' if expr.this else 'FALSE'}]"
            return "TRUE" if expr.this else "FALSE"
        
        if isinstance(expr, exp.Binary):
            return f"{self._render_expression(expr.left, context+'_l')} {expr.key} {self._render_expression(expr.right, context+'_r')}"
        
        return str(expr.this) if hasattr(expr, 'this') else str(expr)

    # ── singularisation helper ──────────────────────────────────────
    @staticmethod
    def _singularize(word: str) -> str:
        """Best-effort singular form of an English table name.

        Handles the common patterns that appear in SQL table names while
        being conservative enough not to mangle non-plural words.
        """
        if not word or len(word) <= 2:
            return word

        low = word.lower()

        # Don't touch words that aren't really plural
        # (ending in 'ss', 'us', 'is' — e.g. Address, Status, Analysis)
        if low.endswith(('ss', 'us', 'is')):
            return word

        # -ies  →  -y   (breweries → brewery, cities → city)
        if low.endswith('ies'):
            return word[:-3] + ('Y' if word[-4].isupper() else 'y')

        # -ses / -zes / -xes / -ches / -shes  →  drop 'es'
        if low.endswith(('ses', 'zes', 'xes', 'ches', 'shes')):
            return word[:-2]

        # regular -s  (users → user)
        if low.endswith('s') and not low.endswith('ss'):
            return word[:-1]

        return word

    def _render_table(self, table_node, context) -> str:
        rng = self._get_rng(context)
        
        # Handle subqueries (derived tables)
        if isinstance(table_node, exp.Subquery):
            inner_query = table_node.this
            source_table = ""
            if hasattr(inner_query, 'args') and inner_query.args.get('from_'):
                from_node = inner_query.args['from_'].this
                if hasattr(from_node, 'name'):
                    source_table = from_node.name
                elif hasattr(from_node, 'this') and hasattr(from_node.this, 'name'):
                    source_table = from_node.this.name
            
            inner_where = ""
            if hasattr(inner_query, 'args') and inner_query.args.get('where'):
                inner_where = self._render_expression(inner_query.args['where'].this, context + "_inner_where")
            
            if self._emit_mode == 'template':
                table_ref = f"[TABLE:{source_table}]" if source_table else ""
                if source_table and inner_where:
                    return f"{table_ref} where {inner_where}"
                elif source_table:
                    return f"{table_ref} results"
                return "a derived query"

            if source_table and inner_where:
                return f"{source_table} where {inner_where}"
            elif source_table:
                return f"{source_table} results"
            return "a derived query"
        
        table_name = table_node.name if isinstance(table_node, exp.Table) else str(table_node)
        
        # Strip 'inner_' prefix (e.g., inner_users -> users)
        if table_name.startswith('inner_'):
             table_name = table_name.replace('inner_', '')
        
        # Rename derived_table
        if table_name == 'derived_table':
             return "the results"

        # Template mode: emit token and skip synonym/pronoun logic
        if self._emit_mode == 'template':
            return f"[TABLE:{table_name}]"
        
        # Pronoun logic — ALWAYS roll to keep RNG in sync
        roll = self._rng.random()
        use_pron = roll < 0.6
        
        # Former/latter diversity
        same_type_mentions = [m for m in self._recent_mentions if m[0] == 'table']
        is_former = len(same_type_mentions) == 2 and same_type_mentions[0][1] == table_name.lower()
        is_latter = len(same_type_mentions) == 2 and same_type_mentions[1][1] == table_name.lower()
        
        pronoun_options = ["it", "that", "that table", "the aforementioned table"]
        if is_former: pronoun_options.append("the former")
        elif is_latter: pronoun_options.append("the latter")
        pronoun = self._rng.choice(pronoun_options)
        
        entity_key = ('table', table_name.lower())
        is_repeated = entity_key in self._mentions
        can_pronoun = (is_repeated
                       and self._ambig_pronoun_count == 0
                       and not self._is_self_join
                       and not self._is_technical_alias(table_name))
        
        # SYNONYM LOGIC: ALWAYS roll to keep sequence in sync
        syns = self.schema_synonyms.get(table_name.lower(), [table_name])
        synonym = self._rng.choice(syns)
        
        # Hook: let strategy decide (pronoun, synonym, or raw table name)
        result = self._strategy.on_table_reference(
            table_name=table_name,
            default=table_name,
            is_repeated=is_repeated,
            pronoun=pronoun,
            use_pronoun=use_pron,
            can_pronoun=can_pronoun,
            synonym=synonym,
        )
        
        if result != table_name and can_pronoun and use_pron:
            # A pronoun was used
            self._ambig_pronoun_count += 1
            return result
        
        # Track first mention
        self._mentions.add(entity_key)
        if entity_key not in self._recent_mentions:
            self._recent_mentions.append(entity_key)
        
        return result

    def _render_column(self, col_node, context) -> str:
        rng = self._get_rng(context)
        # Extract column name: handle Column, Identifier, and fallback
        if isinstance(col_node, exp.Column):
            col_name = col_node.name
        elif isinstance(col_node, exp.Identifier):
            col_name = col_node.name  # .name strips quotes automatically
        else:
            col_name = str(col_node)
        table = col_node.table if hasattr(col_node, 'table') else ""

        # Template mode: emit IR token with optional table qualifier
        if self._emit_mode == 'template':
            actual_table = table
            if table:
                if table.startswith('inner_'):
                    actual_table = table.replace('inner_', '')
                elif self._is_technical_alias(table):
                    base = self._get_base_type_from_alias(table)
                    if base:
                        actual_table = base
                if actual_table == 'derived_table':
                    actual_table = ""
            if actual_table:
                return f"[COL:{actual_table}.{col_name}]"
            return f"[COL:{col_name}]"
        
        # Pronoun logic — ALWAYS roll to keep RNG in sync
        roll = self._rng.random()
        use_pron = roll < 0.6
        
        # Former/latter diversity
        same_type_mentions = [m for m in self._recent_mentions if m[0] == 'column']
        is_former = len(same_type_mentions) == 2 and same_type_mentions[0][1] == col_name.lower()
        is_latter = len(same_type_mentions) == 2 and same_type_mentions[1][1] == col_name.lower()

        pronoun_options = ["it", "that value", "this field", "the aforementioned column"]
        if is_former: pronoun_options.append("the former")
        elif is_latter: pronoun_options.append("the latter")
        pronoun = self._rng.choice(pronoun_options)
        
        entity_key = ('column', col_name.lower())
        is_repeated = entity_key in self._mentions
        prior_columns = [m for m in self._mentions if m[0] == 'column']
        can_pronoun = (is_repeated
                       and len(prior_columns) == 1
                       and self._ambig_pronoun_count == 0
                       and not self._is_self_join
                       and not self._is_technical_alias(col_name))

        # SYNONYM LOGIC: ALWAYS roll
        syns = self.schema_synonyms.get(col_name.lower(), [col_name])
        synonym = self._rng.choice(syns)
        
        # Hook: let strategy decide (pronoun, synonym, or raw col name)
        result = self._strategy.on_column_reference(
            col_name=col_name,
            table=table,
            default=col_name,
            is_repeated=is_repeated,
            pronoun=pronoun,
            use_pronoun=use_pron,
            can_pronoun=can_pronoun,
            synonym=synonym,
        )
        
        if result != col_name and can_pronoun and use_pron:
            # A pronoun was used
            self._ambig_pronoun_count += 1
            return result
        
        # Track mention
        self._mentions.add(entity_key)
        if entity_key not in self._recent_mentions:
            self._recent_mentions.append(entity_key)

        # If hook returned a different name (synonym), use that
        col_name = result
            
        # Disambiguate columns by keeping table qualifier when needed
        if table:
            # Strip 'inner_' prefix for internal tables
            if table.startswith('inner_'):
                table = table.replace('inner_', '')
            
            # Map 'derived_table' to "the result's"
            if table == 'derived_table':
                return f"the result's {col_name}"
            
            # In single-table context, omit redundant table prefix
            is_single = getattr(self, '_is_single_table', False)
            if is_single and not self._is_technical_alias(table):
                 return col_name
        
            # For technical aliases (e.g. p1, u5), map to real table names
            # to avoid ambiguous output like "id equals id"
            if self._is_technical_alias(table):
                base_table = None
                if table.lower().startswith('u'): base_table = 'users'
                elif table.lower().startswith('p'): base_table = 'posts'
                elif table.lower().startswith('c'): base_table = 'comments'
                elif table.lower().startswith('l'): base_table = 'likes'
                elif table.lower().startswith('f'): base_table = 'follows'
                
                # Only map alias to NL if it is unambiguous (count == 1);
                # if ambiguous (e.g. self-join), preserve the raw alias
                if base_table:
                    count = getattr(self, '_alias_counts', {}).get(base_table, 1)
                    
                    if count == 1:
                        noun = self._singularize(base_table)
                        return f"the {noun}'s {col_name}"
                    else:
                        return f"{table}.{col_name}"
        
            # Hook: strategy can decide whether to keep or drop the table qualifier
            qualified = f"{table}.{col_name}"
            unqualified = col_name if not self._is_technical_alias(table) else qualified
            qual_decision = self._strategy.on_keyword("COL_QUALIFIER", qualified)
            if qual_decision == "":
                return unqualified
            return qualified

        return col_name


    def _render_order_key(self, order_expr, context: str) -> str:
        """Render an ORDER BY key expression (e.g., 'name ASC')."""
        col = self._render_expression(order_expr.this, context)
        # Use args.get('desc') because .desc is a method in sqlglot
        if order_expr.args.get('desc'):
            return f"{col} descending"
        return col  # Default is ascending, no need to specify

    def _render_subquery(self, subquery, context: str) -> str:
        """
        Render a subquery SELECT statement to natural language.
        
        Creates a concise description of the nested query.
        """
        if isinstance(subquery, exp.Select):
            # Extract key parts of the subquery
            cols = ", ".join([self._render_expression(e, f"{context}_col{i}") for i, e in enumerate(subquery.expressions)])
            
            from_node = subquery.args.get('from_')
            table = ""
            if from_node:
                table = self._render_table(from_node.this, context + "_tbl")
            
            where_node = subquery.args.get('where')
            where_str = ""
            if where_node:
                where_str = f" where {self._render_expression(where_node.this, context + '_w')}"
            
            return f"Select {cols} from {table}{where_str}"
        
        # Handle Union within subqueries (nesting)
        if isinstance(subquery, exp.Union):
            return self.render_union(subquery)
        
        # Fallback for non-Select subqueries
        return str(subquery)

    def _render_order_clause(self, node, context: str) -> Optional[str]:
        """Helper to render ORDER BY clause for Select or Set operations."""
        order_node = node.args.get('order')
        if order_node:
            order_cols = ", ".join([
                self._render_order_key(e, f"{context}_{i}") 
                for i, e in enumerate(order_node.expressions)
            ])
            return f"ordered by {order_cols}"
        return None

    def _render_limit_clause(self, node) -> Optional[str]:
        """Helper to render LIMIT clause for Select or Set operations."""
        limit_node = node.args.get('limit')
        if limit_node:
            limit_expr = limit_node.expression
            limit_val = limit_expr.this if hasattr(limit_expr, 'this') else str(limit_expr)
            return f"limited to {limit_val} results"
        return None

    def _render_date_sub(self, expr: exp.DateSub, context: str) -> str:
        """
        Render DATE_SUB expressions with proper interval description.
        
        Converts patterns like DATE_SUB(NOW(), INTERVAL 30 DAY) to "30 days ago".
        
        sqlglot DateSub structure:
          - expr.this: base date (e.g., NOW())
          - expr.expression: the interval value as Literal (e.g., '30')
          - expr.unit: the unit as Var (e.g., DAY)
        """
        try:
            # Extract the numeric value from expr.expression (Literal node)
            value_node = expr.expression
            if hasattr(value_node, 'this'):
                value = value_node.this
            else:
                value = str(value_node)
            
            # Extract the unit from expr.unit (Var node) 
            unit_node = expr.args.get('unit')
            if unit_node and hasattr(unit_node, 'this'):
                unit = str(unit_node.this).lower()
            else:
                unit = "day"  # Default fallback
            
            # Pluralize the unit if value != 1
            unit_str = unit if str(value) == '1' else f"{unit}s"
            
            return f"{value} {unit_str} ago"
        except Exception:
            # Ultimate fallback: return a reasonable description
            return "a past date"

    def _render_sqlite_datetime(self, expr: exp.Anonymous, context: str) -> str:
        """
        Render SQLite datetime() expressions to natural language.
        
        Handles patterns like:
          - datetime('now') -> "the current time"
          - datetime('now', '-30 days') -> "30 days ago"
        """
        try:
            expressions = expr.expressions or []
            if not expressions:
                return "a date"
            
            # First argument is typically 'now' or a date string
            base = str(expressions[0].this) if hasattr(expressions[0], 'this') else str(expressions[0])
            
            # If there's a modifier (second argument like '-30 days')
            if len(expressions) >= 2:
                modifier = str(expressions[1].this) if hasattr(expressions[1], 'this') else str(expressions[1])
                # Parse modifier (e.g. '-30 days')
                import re
                modifier = modifier.strip("'")
                match = re.search(r'([+-]?)\s*(\d+)\s*(\w+)', modifier)
                if match:
                    sign, value, unit = match.groups()
                    unit = unit.lower().rstrip('s')  # Normalize: days -> day
                    if not sign: sign = '+' # Default to add if no sign
                    
                    unit_str = unit if value == '1' else f"{unit}s"
                    if sign == '-':
                        default_rendering = f"{value} {unit_str} ago"
                        return self._strategy.on_temporal(modifier, default_rendering, self._rng)
                    else: 
                        default_rendering = f"{value} {unit_str} from now"
                        return self._strategy.on_temporal(modifier, default_rendering, self._rng)
                return f"{base} with modifier"
            
            if base.lower() == 'now':
                return "the current time"
            return base
        except Exception:
            return "a date"

    def _render_datetime_node(self, expr: exp.Datetime, context: str) -> str:
        """
        Render sqlglot.expressions.Datetime nodes (e.g. DATETIME('now', '-25 days'))
        Structure:
          - expr.this: base date (Literal 'now')
          - expr.expression: modifier (Literal '-25 days')
        """
        try:
            base = str(expr.this.this) if hasattr(expr.this, 'this') else str(expr.this)
            
            # If modifier exists
            if expr.expression:
                modifier = str(expr.expression.this) if hasattr(expr.expression, 'this') else str(expr.expression)
                
                # Logic copied from _render_sqlite_datetime
                import re
                modifier = modifier.strip("'")
                match = re.search(r'([+-]?)\s*(\d+)\s*(\w+)', modifier)
                if match:
                    sign, value, unit = match.groups()
                    unit = unit.lower().rstrip('s') 
                    if not sign: sign = '+' 
                    
                    unit_str = unit if value == '1' else f"{unit}s"
                    if sign == '-':
                        default_rendering = f"{value} {unit_str} ago"
                        return self._strategy.on_temporal(modifier, default_rendering, self._rng)
                    else: 
                        default_rendering = f"{value} {unit_str} from now"
                        return self._strategy.on_temporal(modifier, default_rendering, self._rng)
                return f"{base} with modifier" 
            
            if base.lower() == 'now':
                return "the current time"
            return base
        except Exception:
            return "a date"