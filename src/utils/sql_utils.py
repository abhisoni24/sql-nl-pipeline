import re

def extract_sql(text: str) -> str:
    """
    Extract SQL from LLM response.
    
    Priority:
    1. Extract from ```sql...``` blocks
    2. Extract from ```...``` blocks
    3. Find first SELECT/INSERT/UPDATE/DELETE to last ;
    4. Return cleaned raw text
    """
    if not text or text.startswith('ERROR:'):
        return ''
    
    # Try ```sql...``` block (greedy match for unclosed)
    # 1. Closed block
    sql_block = re.search(r'```sql\s*(.+?)```', text, re.DOTALL | re.IGNORECASE)
    if sql_block:
        return sql_block.group(1).strip()

    # 2. Unclosed ```sql block (captures rest of string)
    sql_block_unclosed = re.search(r'```sql\s*(.+)', text, re.DOTALL | re.IGNORECASE)
    if sql_block_unclosed:
        return sql_block_unclosed.group(1).strip()
    
    # Try generic code block
    code_block = re.search(r'```\s*(.+?)```', text, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()

    # Generic unclosed code block
    code_block_unclosed = re.search(r'```\s*(.+)', text, re.DOTALL)
    if code_block_unclosed:
        return code_block_unclosed.group(1).strip()
    
    # Find SQL keywords
    sql_pattern = r'(SELECT|INSERT|UPDATE|DELETE|WITH)\s+.+?;'
    match = re.search(sql_pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0).strip()
    
    # Last resort: clean and return
    return text.strip()
