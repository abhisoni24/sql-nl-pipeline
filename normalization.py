"""
Output Normalization Component.
Responsible for cleaning raw LLM outputs and extracting valid SQL.
Enhanced with semantic normalization for fair comparison.
"""
import re
import sqlglot

def normalize_sql(raw_output: str) -> str:
    """
    Normalize raw LLM output to extract and standardize executable SQL.
    
    Strategy:
    1. Remove Markdown code blocks (```sql ... ```).
    2. Strip whitespace and trailing semicolons.
    3. Normalize SQL syntax for semantic comparison.
    
    Returns:
        Cleaned and normalized SQL string.
    """
    if not raw_output:
        return ""

    cleaned = raw_output.strip()

    # 1. Extract from Code Blocks if present
    # Matches ```sql ... ``` or ``` ... ```
    # The regex needs to be careful about newlines
    code_block_pattern = re.compile(r"```(?:\w+)?\s+(.*?)```", re.DOTALL)
    match = code_block_pattern.search(cleaned)
    if match:
        cleaned = match.group(1).strip()
    else:
        # Sometimes the block is not fully closed or structured differently, 
        # but if it starts with ```sql, we should try to take the content
        # This handles cases where only the opening tag exists or is malformed
        start_pattern = re.compile(r"^```(?:\w+)?\s+(.*)", re.DOTALL)
        match_start = start_pattern.match(cleaned)
        if match_start:
            # If we matched the start, assume the rest is content (stripping trailing backticks if any)
            content = match_start.group(1).strip()
            if content.endswith("```"):
                content = content[:-3].strip()
            cleaned = content
    
    # 2. Strip trailing semicolons (FIX #1: Critical bug - now actually implemented!)
    cleaned = cleaned.rstrip(';').strip()
    
    # 3. Apply semantic normalization
    try:
        cleaned = semantic_normalize_sql(cleaned)
    except Exception:
        # If normalization fails, return the basic cleaned version
        pass
    
    return cleaned


def semantic_normalize_sql(sql: str) -> str:
    """
    Normalize SQL to a canonical form for semantic comparison.
    
    Handles:
    - Keyword case normalization (SELECT -> select)
    - Whitespace normalization  
    - JOIN variant normalization (INNER JOIN -> JOIN)
    - Table prefix removal when unambiguous
    - SELECT * vs SELECT table.* equivalence
    - MySQL vs SQLite function equivalence
    
    Returns:
        Semantically normalized SQL string.
    """
    if not sql:
        return ""
    
    try:
        # Parse SQL using sqlglot
        # Use 'mysql' dialect as our gold SQL is MySQL-based
        parsed = sqlglot.parse_one(sql, dialect='mysql')
        
        # Transform to normalized form
        normalized_ast = normalize_ast(parsed)
        
        # Convert back to SQL string in standardized format
        # Use lowercase keywords, consistent spacing
        normalized_sql = normalized_ast.sql(dialect='mysql', pretty=False)
        
        # Additional text-level normalizations
        normalized_sql = post_process_normalization(normalized_sql)
        
        return normalized_sql.strip()
        
    except Exception as e:
        # If parsing fails, fall back to basic text normalization
        return basic_text_normalization(sql)


def normalize_ast(ast):
    """
    Apply semantic normalizations to the SQL AST.
    
    Transformations:
    - INNER JOIN -> JOIN (they're identical)
    - SELECT table.* -> SELECT * in single-table queries
    - Normalize function names (DATE_SUB vs datetime arithmetic)
    """
    from sqlglot import exp
    
    # Normalize JOIN types
    for join in ast.find_all(exp.Join):
        # INNER JOIN and JOIN are the same - normalize to JOIN
        if join.args.get('kind') == 'INNER':
            join.args['kind'] = None  # Default JOIN
    
    # PRIORITY 1 FIX: Normalize SELECT table.* to SELECT * in single-table queries
    for select in ast.find_all(exp.Select):
        # Check if this is a single-table query (no joins)
        from_clause = select.args.get('from')
        if from_clause:
            joins = select.args.get('joins')
            is_single_table = not joins or len(joins) == 0
            
            if is_single_table and select.expressions:
                # In single-table context, normalize SELECT table.* to SELECT *
                for expr in select.expressions:
                    if isinstance(expr, exp.Star):
                        # Remove table qualifier
                        expr.set('table', None)
    
    return ast


def normalize_mysql_functions(sql: str) -> str:
    """
    PRIORITY 2: Normalize MySQL-specific function equivalences.
    
    Transformations:
    - DATE_SUB(NOW(), INTERVAL X unit) -> NOW() - INTERVAL X unit
    - DATE_ADD(NOW(), INTERVAL X unit) -> NOW() + INTERVAL X unit
    """
    # DATE_SUB(NOW(), INTERVAL X unit) -> NOW() - INTERVAL X unit
    sql = re.sub(
        r'DATE_SUB\s*\(\s*NOW\s*\(\s*\)\s*,\s*INTERVAL\s+([^)]+)\)',
        r'NOW() - INTERVAL \1',
        sql,
        flags=re.IGNORECASE
    )
    
    # DATE_ADD(NOW(), INTERVAL X unit) -> NOW() + INTERVAL X unit  
    sql = re.sub(
        r'DATE_ADD\s*\(\s*NOW\s*\(\s*\)\s*,\s*INTERVAL\s+([^)]+)\)',
        r'NOW() + INTERVAL \1',
        sql,
        flags=re.IGNORECASE
    )
    
    return sql


def post_process_normalization(sql: str) -> str:
    """
    Apply final text-level normalizations after AST conversion.
    """
    # PRIORITY 2: Normalize MySQL function equivalences first
    sql = normalize_mysql_functions(sql)
    
    # PRIORITY 1 (text-level fallback): Normalize SELECT table.* to SELECT *
    # Handles cases where AST normalization doesn't persist through sqlglot rendering
    sql = re.sub(r'SELECT\s+([a-zA-Z0-9_]+)\.\*\s+FROM', r'SELECT * FROM', sql, flags=re.IGNORECASE)
    
    # Normalize whitespace
    sql = re.sub(r'\\s+', ' ', sql)
    
    # Remove unnecessary parentheses around single column in SELECT
    # e.g., SELECT (col) -> SELECT col
    sql = re.sub(r'SELECT\\s+\\(([a-zA-Z0-9_\\.]+)\\)', r'SELECT \\1', sql)
    
    # Normalize INNER JOIN to JOIN
    sql = re.sub(r'\\bINNER\\s+JOIN\\b', 'JOIN', sql, flags=re.IGNORECASE)
    
    # Normalize case
    sql = sql.upper()
    
    return sql


def basic_text_normalization(sql: str) -> str:
    """
    Fallback normalization when AST parsing fails.
    Applies basic text-level transformations.
    """
    # Strip and normalize whitespace
    sql = sql.strip()
    sql = re.sub(r'\\s+', ' ', sql)
    
    # Remove trailing semicolon
    sql = sql.rstrip(';')
    
    # Normalize common variations
    sql = re.sub(r'\\bINNER\\s+JOIN\\b', 'JOIN', sql, flags=re.IGNORECASE)
    
    # Uppercase keywords (basic attempt)
    keywords = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'INSERT', 'INTO', 
                'UPDATE', 'SET', 'DELETE', 'VALUES', 'ORDER', 'BY', 'GROUP',
                'HAVING', 'LIMIT', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE']
    
    for keyword in keywords:
        sql = re.sub(r'\\b' + keyword + r'\\b', keyword.upper(), sql, flags=re.IGNORECASE)
    
    return sql
