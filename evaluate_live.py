"""
Lightweight concurrent evaluator — runs alongside run_experiments.py.

Scans for completed model result files, evaluates SQL equivalence
per-schema, and appends results to a shared evaluated JSONL file.
Resume-safe: re-run anytime to pick up new results.

No plot generation — use analyze_results.py for that after all
evaluations finish.

Usage:
  python evaluate_live.py experiment_workspace/runs/20260311_190551/outputs \\
      --db-dir dataset/dbs_to_test --parallel --workers 4
"""
import os
import sys
import json
import glob
import hashlib
import shutil
import argparse
import multiprocessing
from pathlib import Path
from collections import defaultdict

import pandas as pd
from tqdm.auto import tqdm

sys.path.insert(0, os.path.abspath('.'))

from src.utils.sql_utils import extract_sql
from src.equivalence.equivalence_engine import SQLEquivalenceEngine
from src.equivalence.config import EquivalenceConfig, EquivalenceResult, EquivalenceCheckResult
from src.core.schema_loader import load_schema


# ---------------------------------------------------------------------------
# Record key (for resume)
# ---------------------------------------------------------------------------
def _record_key(record: dict) -> str:
    prompt_hash = hashlib.md5(
        record.get('input_prompt', '').encode()
    ).hexdigest()[:8]
    return '|'.join([
        record.get('model_name', ''),
        str(record.get('query_id', '')),
        record.get('perturbation_source', ''),
        record.get('perturbation_type', ''),
        prompt_hash,
    ])


def load_existing_keys(eval_path: str) -> set:
    keys = set()
    if not os.path.exists(eval_path):
        return keys
    with open(eval_path, 'r') as f:
        for line in f:
            if line.strip():
                try:
                    keys.add(_record_key(json.loads(line)))
                except json.JSONDecodeError:
                    continue
    return keys


# ---------------------------------------------------------------------------
# Schema discovery — maps schema_name -> schema file path
# ---------------------------------------------------------------------------
SUPPORTED_EXTENSIONS = {'.yaml', '.yml', '.sqlite', '.db', '.sqlite3'}


def discover_schema_map(db_dir: str) -> dict:
    """Return {schema_name: schema_file_path} for all databases in db_dir."""
    schema_map = {}
    db_dir = Path(db_dir)
    for p in sorted(db_dir.iterdir()):
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS and not p.name.endswith('_dictionary.yaml'):
            cfg = load_schema(str(p))
            schema_map[cfg.schema_name] = str(p)
    return schema_map


# ---------------------------------------------------------------------------
# Equivalence engine per schema
# ---------------------------------------------------------------------------
def setup_engine(schema_path: str, workspace: str) -> SQLEquivalenceEngine:
    """Create a seeded equivalence engine for one schema."""
    os.makedirs(workspace, exist_ok=True)
    schema_config = load_schema(schema_path)

    dest_db_dir = os.path.join(workspace, 'test_dbs')
    os.makedirs(dest_db_dir, exist_ok=True)

    from src.equivalence.schema_adapter import create_database_from_schema
    from src.equivalence.seed_database import seed_database
    base_db = os.path.join(dest_db_dir, 'base.sqlite')
    create_database_from_schema(base_db, schema_config=schema_config, overwrite=True)
    seed_database(base_db, schema_config=schema_config)

    config = EquivalenceConfig(
        base_db_path=base_db,
        test_suite_dir=dest_db_dir,
        schema_config=schema_config,
    )
    return SQLEquivalenceEngine(config)


# ---------------------------------------------------------------------------
# DQL cache (same as analyze_results.py)
# ---------------------------------------------------------------------------
_last_gold_sql = None
_last_test_suite = None


def _is_dql(sql: str) -> bool:
    return sql.strip().upper().startswith(('SELECT', 'WITH'))


def check_equivalence_cached(engine, gold_sql, candidate_sql):
    if not _is_dql(gold_sql):
        return engine.check_equivalence(gold_sql, candidate_sql)

    checker = engine.dql_checker
    gold_type = engine._detect_query_type(gold_sql)
    candidate_type = engine._detect_query_type(candidate_sql)
    if gold_type != candidate_type:
        return EquivalenceCheckResult(
            is_equivalent=False,
            result_type=EquivalenceResult.NOT_EQUIVALENT,
            details=f"Query type mismatch: gold={gold_type}, candidate={candidate_type}",
            gold_sql=gold_sql, candidate_sql=candidate_sql,
            query_type=f"{gold_type}/{candidate_type}",
        )

    global _last_gold_sql, _last_test_suite
    if _last_gold_sql == gold_sql and _last_test_suite is not None:
        test_databases = _last_test_suite
    else:
        try:
            test_databases = checker.testsuite_gen.generate_test_suite(gold_sql)
            _last_gold_sql = gold_sql
            _last_test_suite = test_databases
        except Exception as e:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Test suite generation failed: {e}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
            )

    for db_path in test_databases:
        gold_status, gold_result = checker._execute_query(db_path, gold_sql)
        if gold_status == "error":
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Gold exec failed: {gold_result}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
            )
        cand_status, cand_result = checker._execute_query(db_path, candidate_sql)
        if cand_status == "error":
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Candidate exec failed: {cand_result}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
            )
        if not checker._compare_denotations(gold_result, cand_result):
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.NOT_EQUIVALENT,
                details=f"Different denotations on {os.path.basename(db_path)}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
            )

    return EquivalenceCheckResult(
        is_equivalent=True,
        result_type=EquivalenceResult.EQUIVALENT,
        details=f"Equivalent across {len(test_databases)} test databases",
        gold_sql=gold_sql, candidate_sql=candidate_sql,
        query_type="SELECT",
        databases_tested=len(test_databases),
    )


# ---------------------------------------------------------------------------
# Parallel worker support
# ---------------------------------------------------------------------------
_subprocess_engine = None
_subprocess_pid = None


def _get_or_create_worker_engine(config_template):
    global _subprocess_engine, _subprocess_pid
    pid = os.getpid()
    if _subprocess_engine is not None and _subprocess_pid == pid:
        # Check if schema changed
        if _subprocess_engine._current_schema == config_template['schema_name']:
            return _subprocess_engine

    worker_workspace = f'./live_eval_worker_{pid}'
    dest_db_dir = os.path.join(worker_workspace, 'test_dbs')
    os.makedirs(dest_db_dir, exist_ok=True)

    from src.equivalence.schema_adapter import create_database_from_schema
    from src.equivalence.seed_database import seed_database
    base_db = os.path.join(dest_db_dir, 'base.sqlite')
    create_database_from_schema(
        base_db,
        schema_config=config_template['schema_config'],
        overwrite=True,
    )
    seed_database(base_db, schema_config=config_template['schema_config'])

    config = EquivalenceConfig(
        base_db_path=base_db,
        test_suite_dir=dest_db_dir,
        schema_config=config_template['schema_config'],
    )
    engine = SQLEquivalenceEngine(config)
    engine._current_schema = config_template['schema_name']
    _subprocess_engine = engine
    _subprocess_pid = pid

    global _last_gold_sql, _last_test_suite
    _last_gold_sql = None
    _last_test_suite = None

    return engine


def _evaluate_single(args):
    record, engine_config = args
    engine = _get_or_create_worker_engine(engine_config)
    generated_sql = record.get('generated_sql') or extract_sql(record.get('generated_response', ''))
    gold_sql = record.get('gold_sql', '')

    try:
        eq_result = check_equivalence_cached(engine, gold_sql, generated_sql)
        is_equivalent = eq_result.is_equivalent
        eq_details = eq_result.details
    except Exception as e:
        is_equivalent = False
        eq_details = f"Error: {e}"

    record['generated_sql'] = generated_sql
    record['is_equivalent'] = is_equivalent
    record['equivalence_details'] = eq_details
    return record


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Live evaluator — run alongside experiments to evaluate completed results.",
    )
    parser.add_argument('outputs_dir', help='Path to run outputs directory')
    parser.add_argument('--db-dir', required=True,
                        help='Directory with database schema files')
    parser.add_argument('--parallel', action='store_true')
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()

    outputs_dir = args.outputs_dir
    eval_path = os.path.join(outputs_dir, 'evaluated_results_aggregated.jsonl')

    # 1. Discover schemas
    schema_map = discover_schema_map(args.db_dir)
    print(f"Found {len(schema_map)} schemas: {list(schema_map.keys())}")

    # 2. Load existing evaluated keys for resume
    existing_keys = load_existing_keys(eval_path)
    if existing_keys:
        print(f"📋 {len(existing_keys):,} records already evaluated (will skip)")

    # 3. Load all available result files
    result_files = sorted(glob.glob(os.path.join(outputs_dir, 'results_*.jsonl')))
    if not result_files:
        print("No result files found yet.")
        return

    all_records = []
    for fpath in result_files:
        with open(fpath, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        all_records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    print(f"📋 {len(all_records):,} total records across {len(result_files)} files")

    # 3b. Backfill missing complexity from baseline records
    #     (systematic perturbations may have complexity="unknown" if generated
    #      before the fix in 03_generate_systematic_perturbations.py)
    baseline_complexity = {}
    for r in all_records:
        if r.get('perturbation_source') == 'baseline' and r.get('complexity', 'unknown') != 'unknown':
            baseline_complexity[(r['schema_name'], r['query_id'])] = {
                'complexity': r['complexity'],
                'tables': r.get('tables'),
            }
    patched = 0
    for r in all_records:
        if r.get('complexity', 'unknown') == 'unknown':
            lookup = baseline_complexity.get((r.get('schema_name'), r.get('query_id')))
            if lookup:
                r['complexity'] = lookup['complexity']
                if lookup['tables'] is not None and not r.get('tables'):
                    r['tables'] = lookup['tables']
                patched += 1
    if patched:
        print(f"🔧 Backfilled complexity for {patched:,} records from baseline data")

    # 4. Filter to pending only
    pending = [r for r in all_records if _record_key(r) not in existing_keys]
    if not pending:
        print("✅ All records already evaluated — nothing to do.")
        _repair_complexity(eval_path)
        return

    print(f"⏳ {len(pending):,} new records to evaluate")

    # 5. Group by schema for correct engine setup
    by_schema = defaultdict(list)
    for r in pending:
        by_schema[r.get('schema_name', 'unknown')].append(r)

    # 6. Evaluate per schema
    total_new = 0
    for schema_name, records in by_schema.items():
        schema_path = schema_map.get(schema_name)
        if not schema_path:
            print(f"⚠  Schema '{schema_name}' not found in {args.db_dir} — skipping {len(records)} records")
            continue

        cfg = load_schema(schema_path)

        print(f"\n{'─' * 50}")
        print(f"Schema: {schema_name}  ({len(records):,} pending)")
        print(f"{'─' * 50}")

        # Sort by gold_sql for cache affinity
        records.sort(key=lambda r: r.get('gold_sql', ''))

        if args.parallel and len(records) > 50:
            engine_config = {
                'schema_name': schema_name,
                'schema_config': cfg,
            }
            tasks = [(r, engine_config) for r in records]

            with open(eval_path, 'a') as out_f:
                with multiprocessing.Pool(processes=args.workers, maxtasksperchild=100) as pool:
                    for result in tqdm(
                        pool.imap_unordered(_evaluate_single, tasks, chunksize=10),
                        total=len(tasks),
                        desc=f"  {schema_name} (parallel)",
                    ):
                        out_f.write(json.dumps(result) + '\n')
                        out_f.flush()
                        total_new += 1

            # Cleanup worker workspaces
            for ws in glob.glob('./live_eval_worker_*'):
                if os.path.isdir(ws):
                    shutil.rmtree(ws, ignore_errors=True)
        else:
            # Sequential
            workspace = f'./live_eval_workspace_{schema_name}'
            engine = setup_engine(schema_path, workspace)

            global _last_gold_sql, _last_test_suite
            _last_gold_sql = None
            _last_test_suite = None

            with open(eval_path, 'a') as out_f:
                for r in tqdm(records, desc=f"  {schema_name}"):
                    generated_sql = r.get('generated_sql') or extract_sql(r.get('generated_response', ''))
                    gold_sql = r.get('gold_sql', '')
                    try:
                        eq_result = check_equivalence_cached(engine, gold_sql, generated_sql)
                        is_equivalent = eq_result.is_equivalent
                        eq_details = eq_result.details
                    except Exception as e:
                        is_equivalent = False
                        eq_details = f"Error: {e}"

                    r['generated_sql'] = generated_sql
                    r['is_equivalent'] = is_equivalent
                    r['equivalence_details'] = eq_details
                    out_f.write(json.dumps(r) + '\n')
                    out_f.flush()
                    total_new += 1

            shutil.rmtree(workspace, ignore_errors=True)

    print(f"\n✅ {total_new:,} new records evaluated → {eval_path}")
    print(f"   Total evaluated: {len(existing_keys) + total_new:,}")

    # 7. Repair any "unknown" complexity in the output file using baseline lookup
    _repair_complexity(eval_path)


def _repair_complexity(eval_path: str):
    """Patch complexity='unknown' records in the evaluated output file.

    Builds a lookup from baseline records (which always have correct
    complexity) and rewrites the file only if patches are needed.
    """
    if not os.path.exists(eval_path):
        return

    records = []
    with open(eval_path) as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    # Build lookup from baseline records
    baseline_complexity = {}
    for r in records:
        if r.get('perturbation_source') == 'baseline' and r.get('complexity', 'unknown') != 'unknown':
            baseline_complexity[(r['schema_name'], r['query_id'])] = {
                'complexity': r['complexity'],
                'tables': r.get('tables'),
            }

    if not baseline_complexity:
        return  # no baseline data to use as reference

    patched = 0
    for r in records:
        if r.get('complexity', 'unknown') == 'unknown':
            lookup = baseline_complexity.get((r.get('schema_name'), r.get('query_id')))
            if lookup:
                r['complexity'] = lookup['complexity']
                if lookup['tables'] is not None and not r.get('tables'):
                    r['tables'] = lookup['tables']
                patched += 1

    if patched:
        with open(eval_path, 'w') as f:
            for r in records:
                f.write(json.dumps(r) + '\n')
        print(f"🔧 Repaired complexity for {patched:,} records in {eval_path}")


if __name__ == '__main__':
    main()
