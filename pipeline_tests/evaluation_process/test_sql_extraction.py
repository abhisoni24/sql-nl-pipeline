"""
Test SQL extraction quality on real LLM result files.

For each record, verifies that extract_sql produces output that:
  1. Is non-empty
  2. Starts with a valid SQL keyword (SELECT, INSERT, UPDATE, DELETE, WITH)
  3. Does not contain <think> blocks or markdown fences
  4. Does not contain English preamble text before the SQL

Failed records are logged to a JSONL file in this directory.

Usage:
  python pipeline_tests/evaluation_process/test_sql_extraction.py \
      experiment_workspace/runs/20260311_190551/outputs

  # Test a single file
  python pipeline_tests/evaluation_process/test_sql_extraction.py \
      --files experiment_workspace/runs/.../results_model_timestamp.jsonl
"""
import argparse
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

import sqlglot

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.sql_utils import extract_sql

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SQL_KEYWORDS = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CREATE', 'ALTER', 'DROP', 'PRAGMA'}


def check_extraction(record: dict) -> list:
    """Check a single record's extraction. Returns list of issue strings (empty = pass)."""
    response = record.get('generated_response', '')
    extracted = extract_sql(response)
    issues = []

    # 1. Non-empty check
    if not extracted:
        issues.append('empty_extraction')
        return issues

    # 2. Starts with SQL keyword, comment (--), or parenthesized subquery
    first_word = extracted.strip().split()[0].upper() if extracted.strip() else ''
    first_char = extracted.strip()[0] if extracted.strip() else ''
    if first_word not in SQL_KEYWORDS and first_char not in ('(', '-'):
        issues.append(f'bad_start:{first_word[:30]}')

    # 3. Contains <think> block remnants
    if '<think>' in extracted.lower() or '</think>' in extracted.lower():
        issues.append('contains_think_block')

    # 4. Contains markdown fence artifacts
    if '```' in extracted:
        issues.append('contains_markdown_fence')

    # 5. Contains obvious English preamble (heuristic: check if first line has
    #    3+ consecutive lowercase alpha words before any SQL keyword)
    first_line = extracted.split('\n')[0].strip()
    preamble_match = re.match(
        r'^(?!SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP)([a-zA-Z]+\s+){3,}',
        first_line, re.IGNORECASE,
    )
    if preamble_match:
        # Verify this isn't a SQL clause like "LEFT JOIN Paper ON..."
        preamble_text = preamble_match.group(0).strip().upper()
        sql_clause_words = {
            'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'FULL', 'JOIN',
            'FROM', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION',
            'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AS', 'ON', 'AND', 'OR',
            'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'SET', 'VALUES', 'INTO',
            'DISTINCT', 'ALL', 'ANY', 'SOME', 'NULL', 'IS', 'ASC', 'DESC',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
        }
        first_preamble_word = preamble_text.split()[0] if preamble_text else ''
        if first_preamble_word not in sql_clause_words:
            issues.append('english_preamble')

    # 6. Parsable by sqlglot
    try:
        parsed = sqlglot.parse(extracted, read='sqlite', error_level=sqlglot.ErrorLevel.RAISE)
        if not parsed or all(s is None for s in parsed):
            issues.append('parse_empty')
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as e:
        issues.append(f'parse_error:{str(e)[:80]}')
    except Exception as e:
        # sqlglot can raise AttributeError/TypeError on severely malformed SQL
        issues.append(f'parse_crash:{type(e).__name__}:{str(e)[:60]}')

    return issues


def run_tests(result_files: list) -> dict:
    """Run extraction tests on all records. Returns summary dict."""
    all_records = []
    for fpath in result_files:
        with open(fpath, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        r = json.loads(line)
                        r['_source_file'] = os.path.basename(fpath)
                        all_records.append(r)
                    except json.JSONDecodeError:
                        continue

    if not all_records:
        print("No records found.")
        return {}

    # Run checks
    failures = []
    issue_counts = Counter()
    model_stats = defaultdict(lambda: {'total': 0, 'pass': 0, 'fail': 0})
    issue_by_model = defaultdict(Counter)

    for r in all_records:
        model = r.get('model_name', 'unknown')
        model_stats[model]['total'] += 1
        issues = check_extraction(r)

        if issues:
            model_stats[model]['fail'] += 1
            for issue in issues:
                issue_counts[issue] += 1
                issue_by_model[model][issue] += 1
            failures.append({
                'model_name': model,
                'schema_name': r.get('schema_name', ''),
                'job_id': r.get('job_id', ''),
                'query_id': r.get('query_id', ''),
                'perturbation_type': r.get('perturbation_type', ''),
                'issues': issues,
                'raw_response_preview': r.get('generated_response', '')[:500],
                'extracted_preview': extract_sql(r.get('generated_response', ''))[:500],
                'source_file': r.get('_source_file', ''),
            })
        else:
            model_stats[model]['pass'] += 1

    # Log failures
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = os.path.join(SCRIPT_DIR, f'extraction_failures_{timestamp}.jsonl')
    if failures:
        with open(log_path, 'w') as f:
            for fail in failures:
                f.write(json.dumps(fail) + '\n')

    # Print report
    total = len(all_records)
    total_fail = len(failures)
    total_pass = total - total_fail

    print(f"\n{'=' * 60}")
    print(f"SQL Extraction Test Report")
    print(f"{'=' * 60}")
    print(f"Total records:  {total:,}")
    print(f"Passed:         {total_pass:,}  ({100*total_pass/total:.1f}%)")
    print(f"Failed:         {total_fail:,}  ({100*total_fail/total:.1f}%)")

    print(f"\n{'─' * 60}")
    print(f"Results per model:")
    print(f"{'─' * 60}")
    for model in sorted(model_stats):
        s = model_stats[model]
        short = model.split('/')[-1]
        pct = 100 * s['pass'] / s['total'] if s['total'] else 0
        print(f"  {short:40s}  {s['pass']:6,}/{s['total']:6,}  ({pct:.1f}% pass)")
        if model in issue_by_model:
            for issue, cnt in issue_by_model[model].most_common():
                print(f"    - {issue}: {cnt:,}")

    if issue_counts:
        print(f"\n{'─' * 60}")
        print(f"Issue breakdown (all models):")
        print(f"{'─' * 60}")
        for issue, cnt in issue_counts.most_common():
            print(f"  {issue:40s}  {cnt:,}")

    if failures:
        print(f"\n⚠  {total_fail:,} failures logged → {log_path}")
    else:
        print(f"\n✅ All extractions passed!")

    # Exit code: 0 if pass rate >= 95%, 1 otherwise
    pass_rate = total_pass / total if total else 0
    if pass_rate < 0.95:
        print(f"\n✗ FAIL — pass rate {pass_rate:.1%} < 95% threshold")
        return {'exit_code': 1}
    else:
        print(f"\n✓ PASS — pass rate {pass_rate:.1%} >= 95% threshold")
        return {'exit_code': 0}


def main():
    parser = argparse.ArgumentParser(
        description="Test SQL extraction quality on LLM result files.",
    )
    parser.add_argument('outputs_dir', nargs='?',
                        help='Run outputs directory (scans for results_*.jsonl)')
    parser.add_argument('--files', nargs='+',
                        help='Specific JSONL result files to test')
    args = parser.parse_args()

    if args.files:
        result_files = args.files
    elif args.outputs_dir:
        result_files = sorted(glob.glob(os.path.join(args.outputs_dir, 'results_*.jsonl')))
    else:
        parser.print_help()
        sys.exit(1)

    if not result_files:
        print(f"No result files found.")
        sys.exit(1)

    print(f"Testing {len(result_files)} result file(s):")
    for f in result_files:
        print(f"  • {os.path.basename(f)}")

    summary = run_tests(result_files)
    sys.exit(summary.get('exit_code', 0))


if __name__ == '__main__':
    main()


# ─────────────────────────────────────────────────────────────────────────
# Unit tests for extract_sql (run with pytest)
# ─────────────────────────────────────────────────────────────────────────

class TestExtractSQL:
    """Unit tests for src.utils.sql_utils.extract_sql edge cases."""

    # ── Code fence extraction ──────────────────────────────────────

    def test_sql_code_fence(self):
        text = "Here is your query:\n```sql\nSELECT * FROM users;\n```"
        assert extract_sql(text) == "SELECT * FROM users;"

    def test_unclosed_sql_code_fence(self):
        text = "```sql\nSELECT id FROM orders"
        assert extract_sql(text) == "SELECT id FROM orders"

    def test_generic_code_fence(self):
        text = "```\nDELETE FROM logs WHERE age > 30;\n```"
        assert extract_sql(text) == "DELETE FROM logs WHERE age > 30;"

    # ── Think block stripping ─────────────────────────────────────

    def test_think_block_stripped(self):
        text = "<think>I need to think about this...</think>SELECT name FROM employees"
        result = extract_sql(text)
        assert "think" not in result.lower()
        assert result.startswith("SELECT")

    def test_unterminated_think_block(self):
        text = "<think>reasoning that never ends... SELECT * FROM users"
        result = extract_sql(text)
        assert result == ""

    # ── gpt-oss-20b analysis prefix ───────────────────────────────

    def test_analysis_prefix_stripped(self):
        text = "analysisThe user wants a simple query.\n\nSELECT * FROM products"
        result = extract_sql(text)
        assert result == "SELECT * FROM products"

    # ── Paragraph-level extraction (the main fix) ─────────────────

    def test_paragraph_extraction_delete_with_prose(self):
        """gpt-oss-20b: reasoning uses 'delete' as English, real SQL after \\n\\n."""
        text = (
            'We need to delete all authors with Id < 493. But also need to '
            'delete related entries in PaperAuthor? The query says "Remove all '
            'authors". So we need to delete from Author where Id < 493. Also '
            'delete from PaperAuthor.\n\n'
            'DELETE FROM PaperAuthor WHERE AuthorId < 493'
        )
        result = extract_sql(text)
        assert result == "DELETE FROM PaperAuthor WHERE AuthorId < 493"

    def test_paragraph_extraction_select_after_reasoning(self):
        """Reasoning with 'select' used as English, SQL after paragraph break."""
        text = (
            "We need to select the right approach. Let me select the columns "
            "that matter. The user wants to select from the table.\n\n"
            "SELECT name, age FROM employees WHERE age > 30"
        )
        result = extract_sql(text)
        assert result == "SELECT name, age FROM employees WHERE age > 30"

    def test_paragraph_extraction_update_after_reasoning(self):
        text = (
            "We should update the records. Let me update the salary.\n\n"
            "UPDATE employees SET salary = 50000 WHERE department = 'HR'"
        )
        result = extract_sql(text)
        assert result == "UPDATE employees SET salary = 50000 WHERE department = 'HR'"

    def test_paragraph_extraction_insert_after_reasoning(self):
        text = (
            "We need to insert a new record. Let me insert into the table.\n\n"
            "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
        )
        result = extract_sql(text)
        assert result == "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"

    def test_paragraph_extraction_picks_last_sql_block(self):
        """When multiple paragraph blocks start with SQL, pick the last one."""
        text = (
            "SELECT 1 -- just testing\n\n"
            "Actually that's wrong.\n\n"
            "SELECT id, name FROM customers WHERE active = 1"
        )
        result = extract_sql(text)
        assert result == "SELECT id, name FROM customers WHERE active = 1"

    def test_paragraph_extraction_with_analysis_prefix(self):
        """Full gpt-oss-20b pattern: analysis + prose + \\n\\n + SQL."""
        text = (
            "analysisWe need to produce SQL code to remove all authors with Id "
            "less than 493. In SQLite, we can delete from Author. Provide only "
            "SQL. No explanation. So output:\n\n"
            "DELETE FROM Author WHERE Id < 493"
        )
        result = extract_sql(text)
        assert result == "DELETE FROM Author WHERE Id < 493"

    def test_paragraph_extraction_with_so_output_marker(self):
        """Model says 'So output:' then gives SQL after newlines."""
        text = (
            "The user wants all papers. We need to select from Paper. "
            "So output:\n\n"
            "SELECT * FROM Paper"
        )
        result = extract_sql(text)
        assert result == "SELECT * FROM Paper"

    # ── No-semicolon regex without re.DOTALL ──────────────────────

    def test_no_semicolon_single_line_sql(self):
        """Bare SQL with no semicolon, no paragraph breaks — still extracted."""
        text = "SELECT name FROM users WHERE id = 5"
        result = extract_sql(text)
        assert result == "SELECT name FROM users WHERE id = 5"

    def test_no_dotall_prevents_greedy_prose_capture(self):
        """Without re.DOTALL, each line is matched independently."""
        text = (
            "We need to delete something.\n"
            "DELETE FROM Author WHERE Id < 10"
        )
        result = extract_sql(text)
        assert result == "DELETE FROM Author WHERE Id < 10"

    # ── Edge cases ────────────────────────────────────────────────

    def test_empty_input(self):
        assert extract_sql("") == ""

    def test_error_prefix(self):
        assert extract_sql("ERROR: timeout") == ""

    def test_semicolon_terminated_takes_last(self):
        text = "Some preamble text.\n\nSELECT 1;\n\nSELECT id FROM orders;"
        result = extract_sql(text)
        assert result == "SELECT id FROM orders;"

    def test_with_clause(self):
        text = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        result = extract_sql(text)
        assert "WITH" in result and "cte" in result
