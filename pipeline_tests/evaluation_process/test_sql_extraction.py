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
