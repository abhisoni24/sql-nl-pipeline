import re
from typing import Optional

_SQL_START = re.compile(
    r'^(?:SELECT|INSERT|UPDATE|DELETE|WITH)\b', re.IGNORECASE
)


def _extract_last_sql_block(text: str) -> Optional[str]:
    """Split on paragraph breaks and return the last block that starts with a SQL keyword.

    Many models (especially gpt-oss-20b) emit reasoning prose containing SQL
    keywords as English words, then put the actual SQL after a double-newline.
    Scanning blocks in reverse avoids grabbing the prose.
    """
    blocks = re.split(r'\n\n+', text)
    for block in reversed(blocks):
        block = block.strip()
        if _SQL_START.match(block):
            return block
    return None


def extract_sql(text: str) -> str:
    """
    Extract SQL from LLM response.

    Priority:
    1. Strip <think>...</think> reasoning blocks
    2. Strip model-specific markers (analysis, assistantfinal)
    3. Extract from ```sql...``` blocks
    4. Extract from ```...``` blocks
    5. Paragraph-level: split on \\n\\n and take last SQL-starting block
    6. Find semicolon-terminated SQL statement (last match)
    7. Find SQL statement without semicolon (line-level, last match)
    8. Return cleaned raw text
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

    # Paragraph-level extraction: split on double-newlines and scan in reverse
    # for the last block that starts with a SQL keyword.  This cleanly separates
    # reasoning prose from the final SQL in models like gpt-oss-20b.
    para_sql = _extract_last_sql_block(text)
    if para_sql:
        return para_sql

    # Find SQL statement with semicolon — use LAST match to skip preamble text
    # that may contain SQL keywords as English words (e.g. "update names of...")
    sql_pattern = r'((?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+.+?;)'
    matches = list(re.finditer(sql_pattern, text, re.DOTALL | re.IGNORECASE))
    if matches:
        return matches[-1].group(1).strip()

    # Find SQL statement without semicolon (vLLM stop=[";"] strips it)
    # Match per-line (no DOTALL) so each keyword starts a separate match,
    # preventing a keyword in early prose from greedily consuming the rest.
    sql_no_semi = list(re.finditer(
        r'((?:SELECT|INSERT|UPDATE|DELETE|WITH)\s+.+)',
        text, re.IGNORECASE,
    ))
    if sql_no_semi:
        return sql_no_semi[-1].group(1).strip()

    # Last resort: clean and return
    return text.strip()
