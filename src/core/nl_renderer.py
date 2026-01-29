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
            'get': ["Get", "fetch", "retrieve", "pull", "extract", "obtain", "find"],
            'select': ["Select", "choose", "pick", "grab", "find"],
            'show': ["Show", "display", "present", "list"],
            'where': ["where", "having", "with", "for which", "that have", "filtering by"],
            'from': ['from', 'in', 'within', 'out of'],
            'equals': ["equals", "is", "matches", "is equal to", "="],
            'joined with': ['joined with', 'linked to', 'connected to', 'join']
        }

        # Canonical Operators for base rendering
        self.canonical_ops = {
            "gt": "greater than",
            "lt": "less than",
            "gte": "greater than or equal to",
            "lte": "less than or equal to",
            "eq": "equals",
            "neq": "is not equal to"
        }
        
        self.fillers = {
            'hedging': ["basically", "kind of", "sort of", "like", "you know", "I think", "probably"],
            'conversational': ["So", "Well", "Okay", "Alright", "Um", "Uh"],
            'informal': ["gonna", "wanna", "gotta", "a bunch of", "or something", "or whatever"],
            'redundant': ["all the", "any and all", "each and every"]
        }

        self.urgency = {
            'high': ["URGENT:", "ASAP:", "Immediately:", "Critical:", "High priority:"],
            'low': ["When you can,", "No rush,", "At your convenience,", "Low priority:"]
        }

        self.schema_synonyms = {
            "users": ["members", "accounts", "profiles", "customers"],
            "posts": ["articles", "entries", "content", "publications"],
            "comments": ["replies", "responses", "feedback"],
            "view_count": ["views", "visit_count", "impressions"],
            "email": ["email_address", "contact", "mail"],
            "is_verified": ["verified", "is_confirmed", "confirmed"]
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

    def _get_rng(self, extra_seed: str = "") -> random.Random:
        return random.Random(f"{self.config.seed}_{extra_seed}")

    def render(self, ast) -> str:
        self._ambig_pronoun_count = 0 
        
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
            base_nl = f"Execute statement: {ast.sql()}"
        
        return self._apply_global_perturbations(base_nl)

    def _apply_global_perturbations(self, text: str) -> str:
        rng = self._get_rng("global")
        
        # ID 4: Verbosity
        if self.config.is_active(PerturbationType.VERBOSITY_VARIATION):
            parts = text.split()
            if len(parts) > 2:
                parts.insert(rng.randint(1, len(parts)-1), rng.choice(self.fillers['hedging']))
                text = f"{rng.choice(self.fillers['conversational'])} " + " ".join(parts) + f" {rng.choice(self.fillers['informal'])}"

        # ID 9: Punctuation
        if self.config.is_active(PerturbationType.PUNCTUATION_VARIATION):
            text = text.replace(" where ", ", where ").replace(" and ", "... and ")

        # ID 7: Comments
        if self.config.is_active(PerturbationType.COMMENT_ANNOTATIONS):
            text += f" -- (note: for analysis)" if rng.random() < 0.5 else " (specifically active records)"

        # ID 10: Urgency
        if self.config.is_active(PerturbationType.URGENCY_QUALIFIERS):
            level = 'high' if rng.random() < 0.5 else 'low'
            text = f"{rng.choice(self.urgency[level])} {text}"

        # ID 6: Typos
        if self.config.is_active(PerturbationType.TYPOS):
            words = text.split()
            if words:
                idx = rng.randint(0, len(words)-1)
                if len(words[idx]) > 3:
                    w = words[idx]
                    p = rng.randint(1, len(w)-2)
                    words[idx] = w[:p] + w[p+1] + w[p] + w[p+2:]
                text = " ".join(words)

        # Cleanup: Ensure punctuation is natural
        text = text.strip()
        if not any(text.endswith(p) for p in ['.', '!', '?', '...']):
            text += "."
        return text

    def render_select(self, node):
        parts = []
        # SELECT keyword
        if not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
            kw = "SELECT" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else self._choose_word('get', 'verb')
            parts.append(kw)
        
        # Columns
        col_list = ", ".join([self._render_expression(e, f"col_{i}") for i, e in enumerate(node.expressions)])
        parts.append(col_list)
        
        # FROM keyword
        if not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
            parts.append("FROM" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else self._choose_word('from', 'from_kw'))
        
        # Table reference with alias handling
        from_node = node.args.get('from_')
        if from_node:
            table_name = self._render_table(from_node.this, "main_table")
            alias = from_node.this.alias if hasattr(from_node.this, 'alias') else ""
            if alias and not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
                parts.append(f"{table_name} (as {alias})")
            else:
                parts.append(table_name)
            
        # JOINs
        for i, join in enumerate(node.args.get('joins', [])):
            parts.append(self._render_join(join, f"join_{i}"))
            
        # WHERE
        where_node = node.args.get('where')
        if where_node:
            if not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
                parts.append("WHERE" if self.config.is_active(PerturbationType.MIXED_SQL_NL) else self._choose_word('where', 'where_kw'))
            parts.append(self._render_expression(where_node.this, "where_cond"))
        
        # Bug 4 Fix: ORDER BY clause
        order_node = node.args.get('order')
        if order_node:
            order_cols = ", ".join([
                self._render_order_key(e, f"ord_{i}") 
                for i, e in enumerate(order_node.expressions)
            ])
            parts.append(f"ordered by {order_cols}")
        
        # Bug 4 Fix: LIMIT clause
        # V2 Bug 2 Fix: Use grammatically correct phrasing
        limit_node = node.args.get('limit')
        if limit_node:
            limit_expr = limit_node.expression
            limit_val = limit_expr.this if hasattr(limit_expr, 'this') else str(limit_expr)
            parts.append(f"limited to {limit_val} results")
            
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
        
        return f"{left}, {connector} {right}"

    # Bug 8 Fix: Handle INSERT statements
    def render_insert(self, node) -> str:
        """Render INSERT statement to natural language."""
        # Get table name from Schema node
        schema = node.this
        table = schema.this.name if hasattr(schema, 'this') else str(schema)
        
        # Get column names
        columns = []
        if hasattr(schema, 'expressions') and schema.expressions:
            columns = [str(col.this) if hasattr(col, 'this') else str(col) for col in schema.expressions]
        
        # Get values
        values_expr = node.expression
        values = []
        if values_expr:
            for tuple_expr in values_expr.expressions:
                tuple_vals = [self._render_expression(v, f"val_{i}") for i, v in enumerate(tuple_expr.expressions)]
                values.append(f"({', '.join(tuple_vals)})")
        
        col_str = f"({', '.join(columns)})" if columns else ""
        val_str = ", ".join(values) if values else "(values)"
        
        return f"Insert into {table} {col_str} values {val_str}"

    # Bug 8 Fix: Handle UPDATE statements
    def render_update(self, node) -> str:
        """Render UPDATE statement to natural language."""
        # Get table name
        table = node.this.name if hasattr(node.this, 'name') else str(node.this)
        
        # Get SET assignments (node.expressions contains EQ nodes)
        assignments = []
        for eq_expr in node.expressions:
            col = self._render_expression(eq_expr.this, "upd_col")
            val = self._render_expression(eq_expr.expression, "upd_val")
            assignments.append(f"{col} = {val}")
        
        set_str = ", ".join(assignments) if assignments else ""
        
        # Get WHERE clause
        where_node = node.args.get('where')
        where_str = ""
        if where_node:
            where_str = f" where {self._render_expression(where_node.this, 'upd_where')}"
        
        return f"Update {table} set {set_str}{where_str}"

    # Bug 8 Fix: Handle DELETE statements
    def render_delete(self, node) -> str:
        """Render DELETE statement to natural language."""
        # Get table name
        table = node.this.name if hasattr(node.this, 'name') else str(node.this)
        
        # Get WHERE clause
        where_node = node.args.get('where')
        where_str = ""
        if where_node:
            where_str = f" where {self._render_expression(where_node.this, 'del_where')}"
        
        return f"Delete from {table}{where_str}"

    def _render_join(self, join_node, context):
        if self.config.is_active(PerturbationType.INCOMPLETE_JOIN_SPEC):
            return f"{self._get_rng(context).choice(['with', 'along with'])} {self._render_table(join_node.this, context)}"
        
        table = self._render_table(join_node.this, context)
        on = join_node.args.get('on')
        on_str = f" on {self._render_expression(on, context + '_on')}" if on else ""
        
        # Bug 5 Fix: Preserve join type (LEFT, RIGHT, INNER, etc.)
        join_side = join_node.side.upper() if join_node.side else ""
        join_kind = join_node.kind.upper() if join_node.kind else ""
        # Combine side and kind, defaulting to just "JOIN" if neither exists
        join_type = f"{join_side} {join_kind}".strip()
        if not join_type:
            join_type = "JOIN"
        elif "JOIN" not in join_type:
            join_type = f"{join_type} JOIN"
        
        return f"{join_type} {table}{on_str}"

    def _render_expression(self, expr, context):
        rng = self._get_rng(context)
        
        # ID 5: Aggregates
        if isinstance(expr, exp.AggFunc) and self.config.is_active(PerturbationType.OPERATOR_AGGREGATE_VARIATION):
            template = rng.choice(self.agg_variations.get(expr.key.upper(), ["value of"]))
            return f"{template} {self._render_expression(expr.this, context)}"

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

        # ID 5: Operators
        if isinstance(expr, (exp.GT, exp.LT, exp.GTE, exp.LTE)):
            left = self._render_expression(expr.left, context+'_l')
            right = self._render_expression(expr.right, context+'_r')
            if self.config.is_active(PerturbationType.OPERATOR_AGGREGATE_VARIATION):
                op_str = rng.choice(self.op_variations.get(expr.key, ["matches"]))
            else:
                op_str = self.canonical_ops.get(expr.key, expr.key) # Default to "less than or equal to"
            return f"{left} {op_str} {right}"

        # ID 8: Temporal
        if isinstance(expr, exp.Literal) and self.config.is_active(PerturbationType.TEMPORAL_EXPRESSION_VARIATION):
            if re.search(r'\d{4}-\d{2}-\d{2}', str(expr.this)):
                return rng.choice(["recently", "since last year", "this month"])

        # ID 14: Pronouns
        if isinstance(expr, (exp.Column, exp.Table)) and self.config.is_active(PerturbationType.AMBIGUOUS_PRONOUNS):
            if self._ambig_pronoun_count == 0 and rng.random() < 0.3:
                self._ambig_pronoun_count += 1
                return rng.choice(["it", "that", "this field"])

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

    def _render_table(self, table_node, context):
        # V4 Bug 1 Fix: Handle subqueries (derived tables) preserving inner WHERE
        if isinstance(table_node, exp.Subquery):
            inner_query = table_node.this
            # Extract source table name
            source_table = ""
            if hasattr(inner_query, 'args') and inner_query.args.get('from_'):
                from_node = inner_query.args['from_'].this
                if hasattr(from_node, 'name'):
                    source_table = from_node.name
                elif hasattr(from_node, 'this') and hasattr(from_node.this, 'name'):
                    source_table = from_node.this.name
            
            # V4 Bug 1 Fix: Extract and include inner WHERE condition
            inner_where = ""
            if hasattr(inner_query, 'args') and inner_query.args.get('where'):
                inner_where = self._render_expression(inner_query.args['where'].this, context + "_inner_where")
            
            # Build description preserving conditions
            if source_table and inner_where:
                return f"{source_table} filtered by {inner_where}"
            elif source_table:
                return f"{source_table} results"
            return "a derived query"
        
        name = table_node.name if isinstance(table_node, exp.Table) else str(table_node)
        if self.config.is_active(PerturbationType.TABLE_COLUMN_SYNONYMS):
            syns = self.schema_synonyms.get(name.lower(), [])
            if syns: return self._get_rng(context).choice(syns)
        return name

    def _render_column(self, col_node, context):
        name = col_node.name if isinstance(col_node, exp.Column) else str(col_node)
        table = col_node.table if hasattr(col_node, 'table') else ""
        
        # Schema Synonym Perturbation
        if self.config.is_active(PerturbationType.TABLE_COLUMN_SYNONYMS):
            syns = self.schema_synonyms.get(name.lower(), [])
            if syns: name = self._get_rng(context).choice(syns)
            
        # Default to table.column prefix to match original prompt style
        if table and not self.config.is_active(PerturbationType.OMIT_OBVIOUS_CLAUSES):
            return f"{table}.{name}"
        return name

    def _choose_word(self, key, context):
        options = self.synonyms.get(key, [key])
        if self.config.is_active(PerturbationType.SYNONYM_SUBSTITUTION):
            return self._get_rng(context).choice(options)
        return options[0] # Returns canonical default

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
        
        # Fallback for non-Select subqueries
        return str(subquery)

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
                match = re.match(r'([+-]?)(\d+)\s*(\w+)', modifier)
                if match:
                    sign, value, unit = match.groups()
                    unit = unit.lower().rstrip('s')  # Normalize: days -> day
                    unit_str = unit if value == '1' else f"{unit}s"
                    if sign == '-':
                        return f"{value} {unit_str} ago"
                    else:
                        return f"{value} {unit_str} from now"
                return f"{base} with modifier"
            
            if base.lower() == 'now':
                return "the current time"
            return base
        except Exception:
            return "a date"

    def is_applicable(self, ast: exp.Expression, p_type: PerturbationType) -> bool:
        if p_type in {PerturbationType.TYPOS, PerturbationType.URGENCY_QUALIFIERS, PerturbationType.VERBOSITY_VARIATION}: return True
        if p_type == PerturbationType.INCOMPLETE_JOIN_SPEC: return bool(ast.find(exp.Join))
        if p_type == PerturbationType.TEMPORAL_EXPRESSION_VARIATION: 
            return any(re.search(r'\d{4}-\d{2}-\d{2}', str(l.this)) for l in ast.find_all(exp.Literal))
        return True