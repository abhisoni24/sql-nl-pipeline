"""
Syntax-Directed Translation (SDT) Framework for SQL to Natural Language
This module provides deterministic, template-based rendering of SQL ASTs to NL prompts.
"""

from enum import Enum
from dataclasses import dataclass, field
import random
import re
from typing import List, Dict, Optional, Set, Any
from sqlglot import exp
from src.core.schema import FOREIGN_KEYS

class PerturbationType(Enum):
    """Enumeration of the 13 active perturbation categories."""
    OMIT_OBVIOUS_CLAUSES = "omit_obvious_clauses"              # ID 1
    SYNONYM_SUBSTITUTION = "synonym_substitution"              # ID 2
    VERBOSITY_VARIATION = "verbosity_variation"                # ID 4
    OPERATOR_AGGREGATE_VARIATION = "operator_aggregate_variation" # ID 5
    TYPOS = "typos"                                            # ID 6
    COMMENT_ANNOTATIONS = "comment_annotations"                # ID 7
    TEMPORAL_EXPRESSION_VARIATION = "temporal_expression_variation" # ID 8
    PUNCTUATION_VARIATION = "punctuation_variation"            # ID 9
    URGENCY_QUALIFIERS = "urgency_qualifiers"                  # ID 10
    MIXED_SQL_NL = "mixed_sql_nl"                              # ID 11
    TABLE_COLUMN_SYNONYMS = "table_column_synonyms"            # ID 12
    INCOMPLETE_JOIN_SPEC = "incomplete_join_spec"              # ID 13
    AMBIGUOUS_PRONOUNS = "ambiguous_pronouns"                  # ID 14

@dataclass
class PerturbationConfig:
    """Configuration for perturbations controlling active types and determinism."""
    active_perturbations: Set[PerturbationType] = field(default_factory=set)
    seed: int = 42

    def is_active(self, p_type: PerturbationType) -> bool:
        return p_type in self.active_perturbations

class SQLToNLRenderer:
    """Renders SQL AST nodes to natural language using deterministic templates."""
    
    def __init__(self, config: Optional[PerturbationConfig] = None):
        self.config = config or PerturbationConfig()
        self._ambig_pronoun_count = 0 
        
        # Data Banks: The first element MUST be the canonical word from the original dataset
        self.synonyms = {
            'get': ["Get", "Retrieve", "Find", "Pull up", "Dig out", "Go get", "Fetch me"],
            'select': ["Select", "Pick out", "Spot", "Single out", "choose"],
            'show': ["Show", "Display", "Bring up", "Give me a look at", "Run a check for", "Produce a listing of"],
            'where': ["where", "filtered for", "looking only at", "for which", "that have"],
            'from': ['from', 'in', 'within', 'out of'],
            'equals': ["equals", "is", "matches", "is equal to", "="],
            'and': ["and", "where also", "as well as", "along with"],
            'joined with': ['joined with', 'linked to', 'connected to', 'join']
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

        self.fillers = ["Um", "Uh", "Well", "Okay", "So", "Alright"]
        self.hedges = ["I think", "probably", "basically", "mostly", "sort of", "kind of"]
        self.informal = ["you know", "like", "or something", "or whatever", "wanna", "gotta", "a bunch of"]

        self.annotations = [
            "-- for the audit",
            "-- note for later",
            "-- urgent request",
            "(note: for analysis)",
            "-- needed for the report",
            "(specifically for this check)",
            "(referencing recent data)"
        ]

        self.urgency = {
            'high': ["URGENT:", "ASAP:", "Immediately:", "Critical:", "High priority:"],
            'low': ["When you can,", "No rush,", "At your convenience,", "Low priority:"]
        }

        self.schema_synonyms = {
            'users': ["accounts", "members", "profiles", "users", "clients"],
            'posts': ["articles", "entries", "content", "posts", "updates"],
            'comments': ["feedback", "responses", "remarks", "comments", "messages"],
            'likes': ["reactions", "approvals", "favorites", "likes", "interests"],
            'follows': ["subscriptions", "connections", "follows", "followers", "following"],
            'id': ["unique id", "identifier", "record id"],
            'insert': ["Add", "Insert", "Put", "Include", "Create"],
            'update': ["Update", "Change", "Modify", "Adjust", "Edit"],
            'delete': ["Remove", "Delete", "Drop", "Strip out", "Wipe out"],
            'email': ["contact", "email_address", "electronic mail"],
            'signup_date': ["registration date", "join date", "member since"],
            'post_id': ["article id", "content id"],
            'user_id': ["member id", "account id"]
        }

        self.op_variations = {
            "gt": ["exceeds", "more than", "above", "higher than"],
            "lt": ["below", "under", "fewer than", "lower than"],
            "gte": ["at least", "minimum of", "no less than"],
            "lte": ["at most", "maximum of", "no more than"]
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

    def _get_rng(self, context: str = "") -> random.Random:
        # For backward compatibility with existing calls, but we use the stateful self._rng
        return self._rng

    def _choose_word(self, key, context):
        """Chooses a word randomly from the synonym bank for a given key."""
        options = self.synonyms.get(key.lower(), [key])
        
        # ALWAYS consume exactly one choice from the main RNG to keep sequence in sync
        baseline_choice = self._rng.choice(options)
        
        # If SYNONYM_SUBSTITUTION is the ONLY thing we are doing, we MUST pick something else
        # to ensure the perturbation is visible.
        if self.config.is_active(PerturbationType.SYNONYM_SUBSTITUTION) and len(options) > 1:
            # Use a secondary deterministic RNG to pick the alternative so we don't 
            # pollute the main RNG sequence and de-sync subsequent calls.
            alt_rng = random.Random(f"{self.config.seed}_{key}_{context}_alt")
            remaining = [o for o in options if o != baseline_choice]
            return alt_rng.choice(remaining)
            
        return baseline_choice

    def render(self, ast) -> str:
        self._rng = random.Random(self.config.seed)
        self._ambig_pronoun_count = 0 
        self._mentions = set()
        self._recent_mentions = [] 
        self._use_pronouns = self.config.is_active(PerturbationType.AMBIGUOUS_PRONOUNS)
        
        if isinstance(ast, exp.Select):
            base_nl = self.render_select(ast)
        # Bug 7 Fix: Handle UNION queries
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

        # Apply global-style perturbations - ALWAYS roll to keep sequence in sync
        # ID 4: Verbosity (Fillers)
        choice_filler = self._rng.choice(self.fillers)
        choice_informal = self._rng.choice(self.informal)
        if self.config.is_active(PerturbationType.VERBOSITY_VARIATION):
            base_nl = f"{choice_filler} {base_nl} {choice_informal}." if not base_nl.endswith('.') else f"{choice_filler} {base_nl.rstrip('.')} {choice_informal}."

        # ID 9: Punctuation
        roll_punct = self._rng.random()
        if self.config.is_active(PerturbationType.PUNCTUATION_VARIATION):
            if ',' in base_nl and roll_punct > 0.5:
                base_nl = base_nl.replace(',', ';', 1)
            elif roll_punct > 0.7:
                base_nl = base_nl.rstrip('.') + "..."

        # ID 6: Typos
        if self.config.is_active(PerturbationType.TYPOS):
            words = base_nl.split()
            if words:
                # Try long words first, then any word
                targets = [i for i, w in enumerate(words) if len(w) > 3]
                idx = self._rng.choice(targets) if targets else self._rng.randint(0, len(words) - 1)
                word = words[idx]
                if len(word) >= 2:
                    char_idx = self._rng.randint(0, len(word) - 2)
                    word_list = list(word)
                    word_list[char_idx], word_list[char_idx+1] = word_list[char_idx+1], word_list[char_idx]
                    words[idx] = "".join(word_list)
                    base_nl = " ".join(words)

        # ID 7: Comments (Meta-comments only, no semantic drift)
        choice_comment = self._rng.choice(self.annotations)
        if self.config.is_active(PerturbationType.COMMENT_ANNOTATIONS):
            if not base_nl.endswith('.'):
                 base_nl += "."
            base_nl = f"{base_nl} {choice_comment}"

        # ID 10: Urgency
        urgency_level = self._rng.choice(['high', 'low'])
        urgency_prefix = self._rng.choice(self.urgency[urgency_level])
        if self.config.is_active(PerturbationType.URGENCY_QUALIFIERS):
            base_nl = f"{urgency_prefix} {base_nl}"

        return base_nl


    def render_select(self, node):
        parts = []
        # SELECT keyword - ALWAYS keep a verb even if omitting clauses
        # We must ALWAYS roll the dice to keep the sequence in sync
        intent_key = self._rng.choice(['get', 'select', 'show'])
        intent_verb = self._choose_word(intent_key, 'verb')
        
        if self.config.is_active(PerturbationType.MIXED_SQL_NL):
             parts.append("SELECT")
        else:
             parts.append(intent_verb)
        
        # Columns
        col_list = ", ".join([self._render_expression(e, f"col_{i}") for i, e in enumerate(node.expressions)])
        if "all columns" in col_list:
             parts.append(col_list)
        else:
             parts.append(f"the {col_list}")
        
        # FROM keyword
        if not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
            parts.append("FROM" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else self._choose_word('from', 'from_kw'))
        
        # Table reference with alias handling
        from_node = node.args.get('from_')
        raw_table_name = None
        if from_node:
            table_name = self._render_table(from_node.this, "main_table")
            raw_table_name = from_node.this.this.name if hasattr(from_node.this, 'this') and hasattr(from_node.this.this, 'name') else str(from_node.this.this)
            alias = from_node.this.alias if hasattr(from_node.this, 'alias') else ""
            if alias and not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES) and not self._is_technical_alias(alias):
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
            if not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
                parts.append("WHERE" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else self._choose_word('where', 'where_kw'))
            parts.append(self._render_expression(where_node.this, "where_cond"))
        
        # Bug 4 Fix: ORDER BY clause
        order_parts = self._render_order_clause(node, "select_ord")
        if order_parts:
            parts.append(order_parts)
        
        # Bug 4 Fix: LIMIT clause
        limit_parts = self._render_limit_clause(node)
        if limit_parts:
            parts.append(limit_parts)
            
        return " ".join(parts)

    # Bug 7 Fix: Handle UNION queries
    def render_union(self, node) -> str:
        """Render UNION/UNION ALL queries by combining left and right sides."""
        left = self.render(node.left) if node.left else ""
        right = self.render(node.right) if node.right else ""
        
        # Remove trailing periods from sub-queries for cleaner joining
        left = left.rstrip('.')
        right = right.rstrip('.')
        
        # V4 Bug 2 Fix: Clearly distinguish UNION ALL from UNION
        # distinct=False means UNION ALL (keep duplicates)
        # distinct=True or None means UNION (remove duplicates)
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

    # Bug 8 Fix: Handle INSERT statements
    def render_insert(self, node) -> str:
        """Render INSERT statement to natural language."""
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
             return f"{verb} a new {table_nl.rstrip('s')} with {', '.join(assignments)}."
        
        col_str = f"({', '.join(columns)})"
        val_str = ", ".join(values)
        
        return f"{verb} into {table_nl} {col_str} values {val_str}"

    # Bug 8 Fix: Handle UPDATE statements
    def render_update(self, node) -> str:
        """Render UPDATE statement to natural language."""
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
            kw = "WHERE" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else "where"
            if self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
                where_str = f" for the {table_nl.rstrip('s')} {self._render_expression(where_node.this, 'upd_where')}"
            else:
                where_str = f" for the {table_nl.rstrip('s')} {kw} {self._render_expression(where_node.this, 'upd_where')}"
        else:
            where_str = f" for all {table_nl}"
        
        verb = self._choose_word('update', 'upd_verb')
        return f"{verb} {set_str}{where_str}"

    # Bug 8 Fix: Handle DELETE statements
    def render_delete(self, node) -> str:
        """Render DELETE statement to natural language."""
        # Get table name
        table_nl = self._render_table(node.this, "del_table")
        
        # Get WHERE clause
        where_node = node.args.get('where')
        where_str = ""
        if where_node:
            kw = "WHERE" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else "where"
            if self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
                 where_str = f" {self._render_expression(where_node.this, 'del_where')}"
            else:
                 where_str = f" {kw} {self._render_expression(where_node.this, 'del_where')}"
        
        verb = self._choose_word('delete', 'del_verb')
        return f"{verb} {table_nl}{where_str}"

    def _render_join(self, join_node, context, left_table_name=None):
        table = self._render_table(join_node.this, context)
        # Attempt to get the raw table name (not the NL version)
        right_table_name = join_node.this.this.name if hasattr(join_node.this, 'this') and hasattr(join_node.this.this, 'name') else str(join_node.this.this)
        
        # ALWAYS roll for incomplete join spec to keep sequence in sync
        choice_incomplete = self._rng.choice(['with', 'along with'])
        
        if self.config.is_active(PerturbationType.INCOMPLETE_JOIN_SPEC):
             # For incomplete join spec, we almost always want "and their" or "along with"
             if left_table_name and right_table_name:
                 if (left_table_name, right_table_name) in FOREIGN_KEYS or (right_table_name, left_table_name) in FOREIGN_KEYS:
                      return f"and their {table}", right_table_name
             return f"{choice_incomplete} {table}", right_table_name
        
        on = join_node.args.get('on')
        # V5: Make ON rendering more natural if it's just id matching
        on_str = ""
        is_standard_join = False
        if on:
             on_str = f" on {self._render_expression(on, context + '_on')}"
             # Check if this is a standard FK join
             if left_table_name and right_table_name:
                 fk = FOREIGN_KEYS.get((left_table_name, right_table_name))
                 if fk:
                      # Check if ON clause matches the FK (simplistic check)
                      on_text = str(on).lower()
                      if fk[0].lower() in on_text and fk[1].lower() in on_text:
                           is_standard_join = True
                 else:
                      fk = FOREIGN_KEYS.get((right_table_name, left_table_name))
                      if fk and fk[0].lower() in on_text and fk[1].lower() in on_text:
                           is_standard_join = True

        # Bug 5 Fix: Preserve join type (LEFT, RIGHT, INNER, etc.)
        join_side = join_node.side.upper() if join_node.side else ""
        join_kind = join_node.kind.upper() if join_node.kind else ""
        
        # Natural rendering for common joins
        if not join_side and join_kind == "INNER":
             if is_standard_join:
                  return f"and their {table}", right_table_name
             return f"joined with {table}{on_str}", right_table_name
        elif join_side == "LEFT" and join_kind == "OUTER":
             if is_standard_join:
                  return f"along with their {table} if any", right_table_name
             return f"left-joined with {table}{on_str}", right_table_name
        
        # Combine side and kind, defaulting to just "JOIN" if neither exists
        join_type = f"{join_side} {join_kind}".strip()
        if not join_type:
            join_type = "JOIN"
        elif "JOIN" not in join_type:
            join_type = f"{join_type} JOIN"
        
        return f"{join_type} {table}{on_str}", right_table_name

    def _render_expression(self, expr, context):
        rng = self._get_rng(context)
        
        # ID 5: Aggregates - ALWAYS roll
        agg_key = expr.key.upper() if getattr(expr, 'key', None) else ""
        agg_options = self.agg_variations.get(agg_key, ["value of"])
        agg_template = self._rng.choice(agg_options)
        
        if isinstance(expr, exp.AggFunc) and self.config.is_active(PerturbationType.OPERATOR_AGGREGATE_VARIATION):
            return f"{agg_template} {self._render_expression(expr.this, context)}"

        # Bug 1 Fix: Handle SELECT * wildcard
        if isinstance(expr, exp.Star):
            return "all columns"
        
        # Bug 2 Fix: Handle DATE_SUB temporal expressions
        if isinstance(expr, exp.DateSub):
            return self._render_date_sub(expr, context)
        
        # Handle SQLite datetime() function: datetime('now') or datetime('now', '-X days')
        if isinstance(expr, exp.Anonymous) and str(expr.this).lower() == 'datetime':
            return self._render_sqlite_datetime(expr, context)
        
        # Handle standalone NOW() function calls
        if isinstance(expr, exp.CurrentTimestamp):
            return "the current time"

        # ID 5: Operators - ALWAYS roll
        op_options = self.op_variations.get(expr.key, ["matches"])
        op_template = self._rng.choice(op_options)
        
        if isinstance(expr, (exp.GT, exp.LT, exp.GTE, exp.LTE, exp.EQ, exp.NEQ)):
            left = self._render_expression(expr.left, context+'_l')
            right = self._render_expression(expr.right, context+'_r')
            if self.config.is_active(PerturbationType.OPERATOR_AGGREGATE_VARIATION):
                return f"{left} {op_template} {right}"
            else:
                op_str = self.operators.get(expr.key, expr.key) 
                return f"{left} {op_str} {right}"

        # ID 8: Temporal - ALWAYS roll
        temp_options = ["recently", "since last year", "this month"]
        temp_choice = self._rng.choice(temp_options)
        
        if isinstance(expr, exp.Literal) and self.config.is_active(PerturbationType.TEMPORAL_EXPRESSION_VARIATION):
            if re.search(r'\d{4}-\d{2}-\d{2}', str(expr.this)):
                return temp_choice

        # Bug 3 Fix: Handle IN expressions with subqueries
        if isinstance(expr, exp.In):
            left = self._render_expression(expr.this, context + '_l')
            # Check for subquery (IN (SELECT ...))
            subquery = expr.args.get('query')
            if subquery:
                # V2 Bug 4 Fix: Unwrap Subquery node to get inner Select and avoid double parens
                inner_query = subquery.this if isinstance(subquery, exp.Subquery) else subquery
                subquery_nl = self._render_subquery(inner_query, context + '_sub')
                return f"{left} is in ({subquery_nl})"
            # Handle IN with literal list (IN (1, 2, 3))
            if expr.expressions:
                values = ", ".join([self._render_expression(e, f"{context}_v{i}") for i, e in enumerate(expr.expressions)])
                return f"{left} is in [{values}]"
            return f"{left} is in (unknown)"

        # Bug 7 Fix: Handle EXISTS and NOT EXISTS expressions
        if isinstance(expr, exp.Exists):
            subquery = expr.this
            subquery_nl = self._render_subquery(subquery, context + '_exists')
            return f"exists ({subquery_nl})"
        
        if isinstance(expr, exp.Not):
            inner = self._render_expression(expr.this, context + '_not')
            return f"NOT {inner}"

        # Base recursions
        if isinstance(expr, exp.Column): return self._render_column(expr, context)
        if isinstance(expr, exp.Table): return self._render_table(expr, context)
        
        # Bug 6 Fix: Handle literals with proper formatting
        if isinstance(expr, exp.Literal):
            if expr.is_string:
                return f"'{expr.this}'"  # Quote string literals
            return str(expr.this)
        
        # Bug 6 Fix: Handle Boolean values (TRUE/FALSE)
        if isinstance(expr, exp.Boolean):
            return "TRUE" if expr.this else "FALSE"
        
        if isinstance(expr, exp.Binary):
            return f"{self._render_expression(expr.left, context+'_l')} {expr.key} {self._render_expression(expr.right, context+'_r')}"
        
        return str(expr.this) if hasattr(expr, 'this') else str(expr)

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
            
            if source_table and inner_where:
                return f"{source_table} where {inner_where}"
            elif source_table:
                return f"{source_table} results"
            return "a derived query"
        
        table_name = table_node.name if isinstance(table_node, exp.Table) else str(table_node)
        
        # ID 14 Pronoun Logic: ALWAYS roll to keep sequence in sync
        roll = self._rng.random()
        use_pron = roll < 0.6
        
        # Diversity logic for former/latter
        same_type_mentions = [m for m in self._recent_mentions if m[0] == 'table']
        is_former = len(same_type_mentions) == 2 and same_type_mentions[0][1] == table_name.lower()
        is_latter = len(same_type_mentions) == 2 and same_type_mentions[1][1] == table_name.lower()
        
        pronoun_options = ["it", "that", "that table", "the aforementioned table"]
        if is_former: pronoun_options.append("the former")
        elif is_latter: pronoun_options.append("the latter")
        pronoun = self._rng.choice(pronoun_options)
        
        entity_key = ('table', table_name.lower())
        if self._use_pronouns and entity_key in self._mentions and not self._is_technical_alias(table_name):
             if self._ambig_pronoun_count == 0 and use_pron:
                 self._ambig_pronoun_count += 1
                 return pronoun
        
        self._mentions.add(entity_key)
        if entity_key not in self._recent_mentions:
            self._recent_mentions.append(entity_key)
        
        # SYNONYM LOGIC: ALWAYS roll to keep sequence in sync
        syns = self.schema_synonyms.get(table_name.lower(), [table_name])
        synonym = self._rng.choice(syns)
        
        if self.config.is_active(PerturbationType.TABLE_COLUMN_SYNONYMS) and not self._is_technical_alias(table_name):
            return synonym
        return table_name

    def _render_column(self, col_node, context) -> str:
        rng = self._get_rng(context)
        col_name = col_node.name if isinstance(col_node, exp.Column) else str(col_node)
        table = col_node.table if hasattr(col_node, 'table') else ""
        
        # ID 14 Pronoun Logic: ALWAYS roll
        roll = self._rng.random()
        use_pron = roll < 0.6
        
        # Diversity logic for former/latter
        same_type_mentions = [m for m in self._recent_mentions if m[0] == 'column']
        is_former = len(same_type_mentions) == 2 and same_type_mentions[0][1] == col_name.lower()
        is_latter = len(same_type_mentions) == 2 and same_type_mentions[1][1] == col_name.lower()

        pronoun_options = ["it", "that value", "this field", "the aforementioned column"]
        if is_former: pronoun_options.append("the former")
        elif is_latter: pronoun_options.append("the latter")
        pronoun = self._rng.choice(pronoun_options)
        
        entity_key = ('column', col_name.lower())
        if self._use_pronouns and entity_key in self._mentions and not self._is_technical_alias(col_name):
             if self._ambig_pronoun_count == 0 and use_pron:
                 self._ambig_pronoun_count += 1
                 return pronoun
        
        self._mentions.add(entity_key)
        if entity_key not in self._recent_mentions:
            self._recent_mentions.append(entity_key)

        # SYNONYM LOGIC: ALWAYS roll
        syns = self.schema_synonyms.get(col_name.lower(), [col_name])
        synonym = self._rng.choice(syns)
        
        if self.config.is_active(PerturbationType.TABLE_COLUMN_SYNONYMS) and not self._is_technical_alias(col_name):
            col_name = synonym
            
        # Default to table.column prefix if appropriate
        if table and not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES) and not self._is_technical_alias(table):
            return f"{table}.{col_name}"
        return col_name


    def _render_order_key(self, order_expr, context: str) -> str:
        """
        Render an ORDER BY key expression (e.g., 'name ASC').
        
        Handles the Ordered node which wraps column + direction info.
        """
        # order_expr is an Ordered node with .this (column) and args['desc'] (direction)
        col = self._render_expression(order_expr.this, context)
        # Note: use args.get('desc') because .desc is a method in sqlglot
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
            
            # V3 Bug 3 Fix: Capitalize 'Select' for casing consistency
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
                # Parse modifier like '-30 days'
                import re
                # Fix: Make the modifier parsing more robust for spaces and signs
                match = re.match(r'([+-]?)\s*(\d+)\s*(\w+)', modifier)
                if match:
                    sign, value, unit = match.groups()
                    unit = unit.lower().rstrip('s')  # Normalize: days -> day
                    unit_str = unit if value == '1' else f"{unit}s"
                    if sign == '-':
                        if self.config.is_active(PerturbationType.TEMPORAL_EXPRESSION_VARIATION):
                             return self._rng.choice([f"within the last {value} {unit_str}", f"in the past {value} {unit_str}", f"since {value} {unit_str} ago"])
                        return f"{value} {unit_str} ago"
                    else: # Assuming this else is for the sign, not the match
                        if self.config.is_active(PerturbationType.TEMPORAL_EXPRESSION_VARIATION):
                             return self._rng.choice([f"in {value} {unit_str}", f"{value} {unit_str} out"])
                        return f"{value} {unit_str} from now"
                return f"{base} with modifier" # This line was outside the if/else for sign, keeping it that way
            
            if base.lower() == 'now':
                return "the current time"
            return base
        except Exception:
            return "a date"

    def is_applicable(self, ast: exp.Expression, p_type: PerturbationType) -> bool:
        if p_type in {PerturbationType.TYPOS, PerturbationType.URGENCY_QUALIFIERS, PerturbationType.VERBOSITY_VARIATION, PerturbationType.PUNCTUATION_VARIATION, PerturbationType.COMMENT_ANNOTATIONS, PerturbationType.SYNONYM_SUBSTITUTION, PerturbationType.MIXED_SQL_NL, PerturbationType.OMIT_OBVIOUS_CLAUSES}: 
            return True
        
        if p_type == PerturbationType.INCOMPLETE_JOIN_SPEC: 
            return bool(ast.find(exp.Join))
        
        if p_type == PerturbationType.TEMPORAL_EXPRESSION_VARIATION: 
            # Detect ISO dates in literals
            has_iso = any(re.search(r'\d{4}-\d{2}-\d{2}', str(l.this)) for l in ast.find_all(exp.Literal))
            # Detect DATETIME/NOW functions
            has_func = any(str(a.this).lower() == 'datetime' for a in ast.find_all(exp.Anonymous)) or bool(ast.find(exp.DateSub))
            return has_iso or has_func

        if p_type == PerturbationType.TABLE_COLUMN_SYNONYMS:
            # Check if any table or column in the query is in our synonym bank
            tables = [t.this.this.lower() if hasattr(t.this, 'this') else str(t.this).lower() for t in ast.find_all(exp.Table)]
            columns = [c.this.this.lower() if hasattr(c.this, 'this') else str(c.this).lower() for c in ast.find_all(exp.Column)]
            # Exclude technical aliases
            tables = [t for t in tables if not self._is_technical_alias(t)]
            return any(t in self.schema_synonyms for t in tables) or any(c in self.schema_synonyms for c in columns)

        if p_type == PerturbationType.OPERATOR_AGGREGATE_VARIATION:
            # Check for operators or aggregates with variations
            has_op = any(expr.key in self.op_variations for expr in ast.find_all((exp.GT, exp.LT, exp.GTE, exp.LTE)))
            has_agg = any(expr.key.upper() in self.agg_variations for expr in ast.find_all(exp.AggFunc))
            return has_op or has_agg

        if p_type == PerturbationType.AMBIGUOUS_PRONOUNS:
            # Anchored Substitutions: Needs at least one repeated entity (type-aware)
            freq = {}
            for t in ast.find_all(exp.Table):
                 val = t.this.this.lower() if hasattr(t.this, 'this') else str(t.this).lower()
                 key = ('table', val)
                 freq[key] = freq.get(key, 0) + 1
            for c in ast.find_all(exp.Column):
                 val = c.this.this.lower() if hasattr(c.this, 'this') else str(c.this).lower()
                 key = ('column', val)
                 freq[key] = freq.get(key, 0) + 1
            
            return any(count > 1 for count in freq.values())

        return True