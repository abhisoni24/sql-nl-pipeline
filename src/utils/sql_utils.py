import re

def extract_sql(text: str) -> str:
    """
    Extract SQL from LLM response.
    
    Priority:
    1. Strip <think>...</think> reasoning blocks
    2. Extract from ```sql...``` blocks
    3. Extract from ```...``` blocks
    4. Find first SELECT/INSERT/UPDATE/DELETE to last ; (or end of statement)
    5. Return cleaned raw text
    """
    if not text or text.startswith('ERROR:'):
        return ''

    # Strip <think>...</think> reasoning blocks (Qwen3, etc.)
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    # Strip unterminated <think> blocks (model ran out of tokens mid-thought)
    text = re.sub(r'<think>.*', '', text, flags=re.DOTALL).strip()

    # Strip leading "analysis" token and "assistantfinal" marker (gpt-oss-20b)
    text = re.sub(r'^analysis\s*', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'assistantfinal\s*', '', text, flags=re.IGNORECASE).strip()

    if not text:
        return ''

    # Try ```sql...``` block (closed)
    sql_block = re.search(r'```sql\s*(.+?)```', text, re.DOTALL | re.IGNORECASE)
    if sql_block:
        return sql_block.group(1).strip()

    # Try ```sql...``` block (unclosed — captures rest of string)
    sql_block_unclosed = re.search(r'```sql\s*(.+)', text, re.DOTALL | re.IGNORECASE)
    if sql_block_unclosed:
        return sql_block_unclosed.group(1).strip()

    # Try generic code block (closed)
    code_block = re.search(r'```\s*(.+?)```', text, re.DOTALL)
    if code_block:
        return code_block.group(1).strip()

    # Generic unclosed code block
    code_block_unclosed = re.search(r'```\s*(.+)', text, re.DOTALL)
    if code_block_unclosed:
        return code_block_unclosed.group(1).strip()

    # Strip trailing ``` artifacts (model emitted closer without opener)
    text = re.sub(r'```\s*$', '', text).strip()

    # Find SQL statement with semicolon — use LAST match to skip preamble text
    # that may contain SQL keywords as English words (e.g. "update names of...")
    sql_pattern = r'((?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+.+?;)'
    matches = list(re.finditer(sql_pattern, text, re.DOTALL | re.IGNORECASE))
    if matches:
        return matches[-1].group(1).strip()

    # Find SQL statement without semicolon (vLLM stop=[";"] strips it)
    # Use LAST match for same preamble-skipping reason
    sql_no_semi = list(re.finditer(
        r'((?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+.+)',
        text, re.DOTALL | re.IGNORECASE,
    ))
    if sql_no_semi:
        return sql_no_semi[-1].group(1).strip()

    # Last resort: clean and return
    return text.strip()
