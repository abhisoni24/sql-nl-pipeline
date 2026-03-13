"""
Detailed SQL Parse Failure Report
==================================
Categorises every sqlglot parse failure from the extraction test,
cross-references with the evaluated dataset (is_equivalent, gold_sql etc.),
and produces a CSV report with per-error-category / per-LLM breakdowns.

Outputs (in experiment_workspace/runs/<latest>/outputs/):
  - parse_failure_report.csv          — one row per failure with full traceability
  - parse_failure_summary.csv         — per (error_category, model) counts
  - parse_failure_report_summary.txt  — human-readable narrative report

Usage:
  python generate_parse_failure_report.py
"""

import csv
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, os.path.abspath('.'))

import sqlglot
from src.utils.sql_utils import extract_sql

# ── Paths ────────────────────────────────────────────────────────────────

RUNS_DIR = Path("experiment_workspace/runs")

def find_latest_run():
    run_dirs = sorted([d for d in RUNS_DIR.iterdir() if d.is_dir()], reverse=True)
    for rd in run_dirs:
        if (rd / "outputs" / "evaluated_results_aggregated.jsonl").exists():
            return rd / "outputs"
    return None

OUTPUTS_DIR = find_latest_run()
EVAL_PATH = OUTPUTS_DIR / "evaluated_results_aggregated.jsonl"
REPORT_CSV = OUTPUTS_DIR / "parse_failure_report.csv"
SUMMARY_CSV = OUTPUTS_DIR / "parse_failure_summary.csv"
REPORT_TXT = OUTPUTS_DIR / "parse_failure_report_summary.txt"


# ── Error categorisation ────────────────────────────────────────────────

def categorise_parse_error(extracted_sql: str, issues: list, gold_sql: str) -> str:
    """Assign a human-readable error category to a parse failure."""
    issue_str = " | ".join(issues)

    # 1. No SQL at all (pure prose / empty)
    if "empty_extraction" in issues:
        return "empty_extraction"
    if any(i.startswith("bad_start:WE") or i.startswith("bad_start:THE") for i in issues):
        if "english_preamble" in issues:
            return "no_sql_generated"

    # 2. Unquoted multi-word column names  (e.g. "Date of Birth", "Character Name")
    multi_word_cols = re.findall(
        r'(?:SELECT|FROM|JOIN|ON|WHERE|SET|BY|,)\s+(?:\w+\.)?'
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
        extracted_sql, re.IGNORECASE
    )
    if multi_word_cols:
        return "unquoted_multiword_column"

    # 3. Escaped quote tokenisation errors  ('U\'pdated text')
    if re.search(r"\\['\"]", extracted_sql):
        return "backslash_escaped_quotes"

    # 4. Broken/mismatched string literals
    #    e.g.  WHERE Title != '12'3   or  VALUES ('Sample text 91''
    single_quotes = extracted_sql.count("'")
    if single_quotes % 2 != 0:
        return "mismatched_string_quotes"

    # 5. Ellipsis / placeholder in output
    if "..." in extracted_sql or "…" in extracted_sql:
        return "ellipsis_placeholder"

    # 6. English prose mixed into SQL body (not at start)
    prose_patterns = [
        r'\b(?:we need|the user|parse the|produce SQL|provide only|the query)',
        r'\b(?:so the SQL|let me|I think|that is|not sure|but unclear)',
    ]
    for pat in prose_patterns:
        if re.search(pat, extracted_sql, re.IGNORECASE):
            return "prose_mixed_in_sql"

    # 7. CTE / WITH clause errors
    if "Expected CTE to have alias" in issue_str or "Expecting (" in issue_str:
        upper = extracted_sql.strip().upper()
        if upper.startswith("WITH") and not re.match(r'^WITH\s+\w+\s+AS\s*\(', upper):
            return "malformed_cte"

    # 8. Unsupported syntax (RIGHT JOIN, FULL JOIN in SQLite)
    if re.search(r'\b(RIGHT\s+JOIN|FULL\s+(OUTER\s+)?JOIN)\b', extracted_sql, re.IGNORECASE):
        return "unsupported_join_type"

    # 9. DROP TABLE (not expected in our benchmark)
    if extracted_sql.strip().upper().startswith("DROP"):
        return "unexpected_drop"

    # 10. Catch-all for remaining parse errors
    if any("parse_error:" in i or "parse_crash:" in i for i in issues):
        return "other_parse_error"

    return "unknown"


# ── Load evaluated dataset into lookup ───────────────────────────────────

def load_evaluated_lookup():
    """Build key→record lookup from evaluated results."""
    lookup = {}
    with open(EVAL_PATH) as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            key = (r.get("model_name", ""), r.get("schema_name", ""),
                   str(r.get("query_id", "")), r.get("perturbation_type", ""),
                   r.get("job_id", ""))
            lookup[key] = r
    return lookup


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    import glob
    print("Loading evaluated dataset for cross-reference...")
    eval_lookup = load_evaluated_lookup()
    print(f"  {len(eval_lookup):,} records indexed")

    # Discover result files
    result_files = sorted(glob.glob(str(OUTPUTS_DIR / "results_*.jsonl")))
    print(f"Scanning {len(result_files)} result files...")

    # Totals tracking
    total_records = 0
    model_totals = Counter()          # model → total count
    model_schema_totals = Counter()   # (model, schema) → total count

    # Re-run extraction + parse on every record
    failures = []

    for fpath in result_files:
        with open(fpath) as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                total_records += 1
                model = r.get("model_name", "unknown")
                model_short = model.split("/")[-1]
                schema = r.get("schema_name", "unknown")
                model_totals[model_short] += 1
                model_schema_totals[(model_short, schema)] += 1

                response = r.get("generated_response", "")
                extracted = extract_sql(response)

                # Run same checks as test script
                issues = []
                if not extracted:
                    issues.append("empty_extraction")
                else:
                    first_word = extracted.strip().split()[0].upper() if extracted.strip() else ""
                    first_char = extracted.strip()[0] if extracted.strip() else ""
                    sql_keywords = {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH",
                                    "CREATE", "ALTER", "DROP", "PRAGMA"}
                    if first_word not in sql_keywords and first_char not in ("(", "-"):
                        issues.append(f"bad_start:{first_word[:30]}")

                    if "<think>" in extracted.lower() or "</think>" in extracted.lower():
                        issues.append("contains_think_block")
                    if "```" in extracted:
                        issues.append("contains_markdown_fence")

                    first_line = extracted.split("\n")[0].strip()
                    preamble_match = re.match(
                        r"^(?!SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP)"
                        r"([a-zA-Z]+\s+){3,}", first_line, re.IGNORECASE)
                    if preamble_match:
                        preamble_text = preamble_match.group(0).strip().upper()
                        sql_clause_words = {
                            "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "JOIN",
                            "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION",
                            "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "ON", "AND", "OR",
                            "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "SET", "VALUES", "INTO",
                            "DISTINCT", "ALL", "ANY", "SOME", "NULL", "IS", "ASC", "DESC",
                            "COUNT", "SUM", "AVG", "MIN", "MAX",
                        }
                        first_preamble_word = preamble_text.split()[0] if preamble_text else ""
                        if first_preamble_word not in sql_clause_words:
                            issues.append("english_preamble")

                    # sqlglot parse check
                    try:
                        parsed = sqlglot.parse(extracted, read="sqlite",
                                               error_level=sqlglot.ErrorLevel.RAISE)
                        if not parsed or all(s is None for s in parsed):
                            issues.append("parse_empty")
                    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as e:
                        issues.append(f"parse_error:{str(e)[:80]}")
                    except Exception as e:
                        issues.append(f"parse_crash:{type(e).__name__}:{str(e)[:60]}")

                if not issues:
                    continue  # passed

                # Cross-reference with evaluated dataset
                key = (model, schema, str(r.get("query_id", "")),
                       r.get("perturbation_type", ""), r.get("job_id", ""))
                eval_rec = eval_lookup.get(key, {})

                gold_sql = r.get("gold_sql", eval_rec.get("gold_sql", ""))
                category = categorise_parse_error(extracted or "", issues, gold_sql)

                failures.append({
                    "model": model_short,
                    "schema": schema,
                    "query_id": r.get("query_id", ""),
                    "job_id": r.get("job_id", ""),
                    "perturbation_source": r.get("perturbation_source", ""),
                    "perturbation_type": r.get("perturbation_type", ""),
                    "complexity": eval_rec.get("complexity", r.get("complexity", "")),
                    "error_category": category,
                    "issues": " | ".join(issues),
                    "is_equivalent": eval_rec.get("is_equivalent", ""),
                    "gold_sql": gold_sql[:200],
                    "extracted_sql": (extracted or "")[:300],
                    "raw_response_preview": response[:300],
                })

    print(f"\nTotal records scanned: {total_records:,}")
    print(f"Total failures: {len(failures):,}")

    # ── Write detailed CSV ───────────────────────────────────────────────
    fieldnames = [
        "model", "schema", "query_id", "job_id",
        "perturbation_source", "perturbation_type", "complexity",
        "error_category", "issues", "is_equivalent",
        "gold_sql", "extracted_sql", "raw_response_preview",
    ]
    with open(REPORT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in failures:
            writer.writerow(row)
    print(f"\n✅ Detailed report: {REPORT_CSV}  ({len(failures)} rows)")

    # ── Build summary tables ──────────────────────────────────────────────

    # Per (category, model) counts
    cat_model = Counter()
    cat_total = Counter()
    models = sorted(model_totals.keys())

    for f_rec in failures:
        cat = f_rec["error_category"]
        m = f_rec["model"]
        cat_model[(cat, m)] += 1
        cat_total[cat] += 1

    # All categories sorted by frequency
    categories = [c for c, _ in cat_total.most_common()]

    # Write summary CSV
    summary_fields = ["error_category", "total"] + models + ["example_query_id", "example_job_id", "example_extracted"]
    summary_rows = []
    # Find one example per category
    category_examples = {}
    for f_rec in failures:
        cat = f_rec["error_category"]
        if cat not in category_examples:
            category_examples[cat] = f_rec

    for cat in categories:
        row = {"error_category": cat, "total": cat_total[cat]}
        for m in models:
            row[m] = cat_model.get((cat, m), 0)
        ex = category_examples.get(cat, {})
        row["example_query_id"] = ex.get("query_id", "")
        row["example_job_id"] = ex.get("job_id", "")
        row["example_extracted"] = ex.get("extracted_sql", "")[:150]
        summary_rows.append(row)

    with open(SUMMARY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)
    print(f"✅ Summary table:   {SUMMARY_CSV}  ({len(categories)} categories)")

    # ── Write human-readable text report ──────────────────────────────────

    lines = []
    lines.append("=" * 80)
    lines.append("SQL PARSE FAILURE REPORT")
    lines.append(f"Dataset: {EVAL_PATH}")
    lines.append(f"Date: 2026-03-12")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Total records scanned:  {total_records:,}")
    lines.append(f"Total parse failures:   {len(failures):,}")
    lines.append(f"Overall pass rate:      {(total_records - len(failures)) / total_records * 100:.1f}%")
    lines.append("")

    # Per-model overview
    lines.append("-" * 80)
    lines.append("PER-MODEL OVERVIEW")
    lines.append("-" * 80)
    lines.append(f"{'Model':<45s} {'Pass':>7s} {'Fail':>7s} {'Total':>7s}  {'Rate':>6s}")
    for m in models:
        m_fail = sum(1 for f_rec in failures if f_rec["model"] == m)
        m_total = model_totals[m]
        m_pass = m_total - m_fail
        rate = m_pass / m_total * 100 if m_total else 0
        lines.append(f"{m:<45s} {m_pass:>7,d} {m_fail:>7,d} {m_total:>7,d}  {rate:>5.1f}%")
    lines.append("")

    # Per-category breakdown
    lines.append("-" * 80)
    lines.append("ERROR CATEGORIES (sorted by frequency)")
    lines.append("-" * 80)

    for cat in categories:
        lines.append("")
        lines.append(f"  [{cat_total[cat]:,}] {cat}")
        lines.append(f"  {'':4s}{'Model':<45s} {'Count':>6s}  {'% of model failures':>18s}")
        for m in models:
            cnt = cat_model.get((cat, m), 0)
            m_fail_total = sum(1 for f_rec in failures if f_rec["model"] == m)
            pct = cnt / m_fail_total * 100 if m_fail_total else 0
            if cnt > 0:
                lines.append(f"  {'':4s}{m:<45s} {cnt:>6,d}  {pct:>17.1f}%")

        # Example
        ex = category_examples.get(cat, {})
        if ex:
            lines.append(f"  {'':4s}Example:")
            lines.append(f"  {'':6s}job_id:    {ex.get('job_id', '')}")
            lines.append(f"  {'':6s}query_id:  {ex.get('query_id', '')}")
            lines.append(f"  {'':6s}model:     {ex.get('model', '')}")
            lines.append(f"  {'':6s}schema:    {ex.get('schema', '')}")
            lines.append(f"  {'':6s}complexity: {ex.get('complexity', '')}")
            lines.append(f"  {'':6s}issues:    {ex.get('issues', '')}")
            extracted_preview = ex.get("extracted_sql", "")[:200].replace("\n", "\n" + " " * 17)
            lines.append(f"  {'':6s}extracted: {extracted_preview}")
            gold_preview = ex.get("gold_sql", "")[:150].replace("\n", "\n" + " " * 17)
            lines.append(f"  {'':6s}gold_sql:  {gold_preview}")

    # Per-schema breakdown
    lines.append("")
    lines.append("-" * 80)
    lines.append("PER-SCHEMA FAILURE RATES")
    lines.append("-" * 80)
    schema_model_fail = Counter()
    for f_rec in failures:
        schema_model_fail[(f_rec["schema"], f_rec["model"])] += 1

    schemas = sorted({f_rec["schema"] for f_rec in failures})
    lines.append(f"{'Schema':<20s}" + "".join(f"{m:>18s}" for m in models))
    for schema in schemas:
        parts = [f"{schema:<20s}"]
        for m in models:
            fail = schema_model_fail.get((schema, m), 0)
            total = model_schema_totals.get((m, schema), 0)
            if total:
                rate = (total - fail) / total * 100
                parts.append(f"{rate:>7.1f}% ({fail:>4d})")
            else:
                parts.append(f"{'N/A':>18s}")
        lines.append("".join(parts))

    # Top 20 query_ids with most failures
    lines.append("")
    lines.append("-" * 80)
    lines.append("TOP 20 QUERY IDs WITH MOST PARSE FAILURES (across all models)")
    lines.append("-" * 80)
    qid_counts = Counter()
    qid_examples = {}
    for f_rec in failures:
        qkey = (f_rec["schema"], f_rec["query_id"])
        qid_counts[qkey] += 1
        if qkey not in qid_examples:
            qid_examples[qkey] = f_rec

    lines.append(f"{'Schema':<18s} {'QID':>5s}  {'Fails':>5s}  {'Category':<30s}  Gold SQL")
    for (schema, qid), cnt in qid_counts.most_common(20):
        ex = qid_examples[(schema, qid)]
        gold = ex.get("gold_sql", "")[:60].replace("\n", " ")
        lines.append(f"{schema:<18s} {str(qid):>5s}  {cnt:>5d}  {ex['error_category']:<30s}  {gold}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("END OF REPORT")
    lines.append("=" * 80)

    report_text = "\n".join(lines)
    with open(REPORT_TXT, "w") as f:
        f.write(report_text)
    print(f"✅ Text report:     {REPORT_TXT}")
    print()
    print(report_text)


if __name__ == "__main__":
    main()
