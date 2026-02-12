"""
Script for Analyzing Experiment Results (Systematic & Baseline Only).
Handles EVALUATION (SQL Equivalence) and PLOTTING.
Aggregates all results from a given run directory, BUT filters out LLM perturbations.

Optimizations:
  1. Gold SQL test suite cache — reuses test DBs for same gold_sql (DQL only)
  2. Resume capability — skips already-evaluated records
  5. Parallel evaluation — uses ProcessPoolExecutor with isolated DB workspaces
  6. Streaming writes — appends each record immediately to JSONL
"""
import os
import sys
import json
import glob
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm.auto import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.sql_utils import extract_sql
from src.equivalence.equivalence_engine import SQLEquivalenceEngine
from src.equivalence.config import EquivalenceConfig, EquivalenceResult, EquivalenceCheckResult

# Configuration Defaults
LOCAL_BASE = os.getenv('LOCAL_BASE', './')
REPO_PATH = os.getenv('REPO_PATH', f'{LOCAL_BASE}/')

# ---------------------------------------------------------------------------
# Opt 1: Gold SQL Test Suite Cache (DQL only)
# ---------------------------------------------------------------------------
_test_suite_cache = {}


def _normalize_sql_for_cache(sql: str) -> str:
    """Normalize SQL for use as a cache key."""
    return ' '.join(sql.strip().split()).upper()


def _is_dql(sql: str) -> bool:
    """Check if SQL is a SELECT/DQL query."""
    normalized = sql.strip().upper()
    return normalized.startswith(('SELECT', 'WITH'))


def check_equivalence_cached(engine, gold_sql, candidate_sql):
    """
    Equivalence check with gold SQL test suite caching for DQL queries.
    DML queries delegate directly to engine.check_equivalence().
    """
    if not _is_dql(gold_sql):
        return engine.check_equivalence(gold_sql, candidate_sql)

    cache_key = _normalize_sql_for_cache(gold_sql)
    checker = engine.dql_checker

    gold_type = engine._detect_query_type(gold_sql)
    candidate_type = engine._detect_query_type(candidate_sql)
    if gold_type != candidate_type:
        return EquivalenceCheckResult(
            is_equivalent=False,
            result_type=EquivalenceResult.NOT_EQUIVALENT,
            details=f"Query type mismatch: gold is {gold_type}, candidate is {candidate_type}",
            gold_sql=gold_sql, candidate_sql=candidate_sql,
            query_type=f"{gold_type}/{candidate_type}"
        )

    if cache_key not in _test_suite_cache:
        try:
            test_databases = checker.testsuite_gen.generate_test_suite(gold_sql)
            _test_suite_cache[cache_key] = test_databases
        except Exception as e:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Failed to generate test suite: {str(e)}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT"
            )

    test_databases = _test_suite_cache[cache_key]

    for db_path in test_databases:
        gold_status, gold_result = checker._execute_query(db_path, gold_sql)
        if gold_status == "error":
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Gold query execution failed: {gold_result}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
                databases_tested=test_databases.index(db_path) + 1,
                gold_result=str(gold_result)
            )

        cand_status, cand_result = checker._execute_query(db_path, candidate_sql)
        if cand_status == "error":
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Candidate query execution failed: {cand_result}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
                databases_tested=test_databases.index(db_path) + 1,
                candidate_result=str(cand_result)
            )

        if not checker._compare_denotations(gold_result, cand_result):
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.NOT_EQUIVALENT,
                details=f"Different denotations on database: {os.path.basename(db_path)}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT",
                databases_tested=test_databases.index(db_path) + 1,
                gold_result=str(gold_result[0][:5]) + "..." if len(gold_result[0]) > 5 else str(gold_result[0]),
                candidate_result=str(cand_result[0][:5]) + "..." if len(cand_result[0]) > 5 else str(cand_result[0])
            )

    return EquivalenceCheckResult(
        is_equivalent=True,
        result_type=EquivalenceResult.EQUIVALENT,
        details=f"Equivalent across {len(test_databases)} test databases",
        gold_sql=gold_sql, candidate_sql=candidate_sql,
        query_type="SELECT",
        databases_tested=len(test_databases)
    )


# ---------------------------------------------------------------------------
# Opt 2: Resume — build set of already-evaluated record keys
# ---------------------------------------------------------------------------
def _record_key(record: dict) -> str:
    """Generate a unique key for a record to detect duplicates."""
    parts = [
        record.get('model_name', ''),
        str(record.get('query_id', '')),
        record.get('perturbation_source', ''),
        record.get('perturbation_name', ''),
        str(record.get('perturbation_id', '')),
    ]
    return '|'.join(parts)


def load_existing_evaluated(eval_path: str) -> set:
    """Load keys of already-evaluated records from existing JSONL file."""
    existing_keys = set()
    if not os.path.exists(eval_path):
        return existing_keys

    with open(eval_path, 'r') as f:
        for line in f:
            if line.strip():
                try:
                    record = json.loads(line)
                    existing_keys.add(_record_key(record))
                except json.JSONDecodeError:
                    continue

    print(f"📋 Found {len(existing_keys):,} previously evaluated records (will skip)")
    return existing_keys


# ---------------------------------------------------------------------------
# Core Functions
# ---------------------------------------------------------------------------
def setup_equivalence_engine(local_workspace='./local_eval_workspace'):
    """Copy DBs locally and init engine."""
    if os.path.exists(local_workspace):
        shutil.rmtree(local_workspace)
    os.makedirs(local_workspace, exist_ok=True)

    print("📂 Copying test databases to local VM...")
    src_db_dir = f'{REPO_PATH}/test_dbs'
    dest_db_dir = f'{local_workspace}/test_dbs'

    shutil.copytree(src_db_dir, dest_db_dir)
    print("✅ Database files copied.")

    config = EquivalenceConfig(
        base_db_path=f'{dest_db_dir}/base.sqlite',
        test_suite_dir=dest_db_dir
    )
    return SQLEquivalenceEngine(config)


def aggregate_results(run_outputs_dir: str = None, file_list: list = None) -> pd.DataFrame:
    """Load results from formatted directory OR specific file list."""
    all_records = []

    if file_list:
        files = file_list
        print(f"📋 processing {len(files)} manually specified files.")
    elif run_outputs_dir:
        pattern = f'{run_outputs_dir}/results_*.jsonl'
        files = glob.glob(pattern)
        print(f"📋 Found {len(files)} result files in {run_outputs_dir}")
    else:
        print("⚠️ No directory or files provided.")
        return pd.DataFrame()

    if not files:
        print(f"⚠️ No result files found.")
        return pd.DataFrame()

    for fpath in files:
        if not os.path.exists(fpath):
            print(f"⚠️ File not found: {fpath}")
            continue

        with open(fpath, 'r') as f:
            for line in f:
                if line.strip():
                    all_records.append(json.loads(line))

    df = pd.DataFrame(all_records)
    print(f"✅ Aggregated {len(df):,} total records.")
    return df


# ---------------------------------------------------------------------------
# Opt 5: Worker function for parallel evaluation (runs in subprocess)
# ---------------------------------------------------------------------------
def _evaluate_single(args):
    """
    Evaluate a single record in a subprocess.
    """
    record, engine_config_template, worker_id = args
    engine = _get_or_create_worker_engine(engine_config_template, worker_id)

    generated_sql = extract_sql(record.get('generated_response', ''))
    gold_sql = record.get('gold_sql', '')

    try:
        eq_result = check_equivalence_cached(engine, gold_sql, generated_sql)
        is_equivalent = eq_result.is_equivalent
        eq_details = eq_result.details
    except Exception as e:
        is_equivalent = False
        eq_details = f"Error: {str(e)}"

    record['generated_sql'] = generated_sql
    record['is_equivalent'] = is_equivalent
    record['equivalence_details'] = eq_details
    return record


_subprocess_engine = None
_subprocess_worker_id = None


def _get_or_create_worker_engine(config_template, worker_id):
    """
    Get or create a cached engine for this subprocess.
    """
    global _subprocess_engine, _subprocess_worker_id

    if _subprocess_engine is not None and _subprocess_worker_id == worker_id:
        return _subprocess_engine

    worker_workspace = f'./local_eval_workspace_worker_{worker_id}'
    if not os.path.exists(worker_workspace):
        os.makedirs(worker_workspace, exist_ok=True)
        src_db_dir = config_template['source_db_dir']
        dest_db_dir = f'{worker_workspace}/test_dbs'
        shutil.copytree(src_db_dir, dest_db_dir)

    dest_db_dir = f'{worker_workspace}/test_dbs'
    config = EquivalenceConfig(
        base_db_path=f'{dest_db_dir}/base.sqlite',
        test_suite_dir=dest_db_dir,
        max_fuzz_iterations=config_template.get('max_fuzz_iterations', 100),
        max_distilled_dbs=config_template.get('max_distilled_dbs', 10),
        order_matters=config_template.get('order_matters', False),
    )

    _subprocess_engine = SQLEquivalenceEngine(config)
    _subprocess_worker_id = worker_id

    global _test_suite_cache
    _test_suite_cache = {}

    return _subprocess_engine


def evaluate_dataframe_sequential(
    df: pd.DataFrame,
    engine: SQLEquivalenceEngine,
    eval_path: str,
    existing_keys: set
) -> int:
    """
    Evaluate records sequentially with streaming writes and resume.
    """
    records = df.to_dict('records')
    new_count = 0
    skipped = 0

    with open(eval_path, 'a') as out_f:
        for r in tqdm(records, desc="Evaluating SQL (Systematic)"):
            key = _record_key(r)
            if key in existing_keys:
                skipped += 1
                continue

            generated_sql = extract_sql(r.get('generated_response', ''))
            gold_sql = r.get('gold_sql', '')

            try:
                eq_result = check_equivalence_cached(engine, gold_sql, generated_sql)
                is_equivalent = eq_result.is_equivalent
                eq_details = eq_result.details
            except Exception as e:
                is_equivalent = False
                eq_details = f"Error: {str(e)}"

            r['generated_sql'] = generated_sql
            r['is_equivalent'] = is_equivalent
            r['equivalence_details'] = eq_details

            out_f.write(json.dumps(r) + '\n')
            out_f.flush()
            new_count += 1

    if skipped:
        print(f"⏭️  Skipped {skipped:,} already-evaluated records")
    return new_count


def evaluate_dataframe_parallel(
    df: pd.DataFrame,
    engine: SQLEquivalenceEngine,
    eval_path: str,
    existing_keys: set,
    max_workers: int = 4
) -> int:
    """
    Evaluate records in parallel with streaming writes and resume.
    """
    records = df.to_dict('records')
    pending = [r for r in records if _record_key(r) not in existing_keys]

    if not pending:
        print("✅ All records already evaluated — nothing to do.")
        return 0

    skipped = len(records) - len(pending)
    if skipped:
        print(f"⏭️  Skipped {skipped:,} already-evaluated records")

    print(f"⏳ Evaluating {len(pending):,} new records with {max_workers} workers...")
    pending.sort(key=lambda r: r.get('gold_sql', ''))

    for i, r in enumerate(pending):
        r['_worker_id'] = i % max_workers

    engine_config_template = {
        'source_db_dir': f'{REPO_PATH}/test_dbs',
        'max_fuzz_iterations': engine.config.max_fuzz_iterations,
        'max_distilled_dbs': engine.config.max_distilled_dbs,
        'order_matters': engine.config.order_matters,
    }

    new_count = 0

    with open(eval_path, 'a') as out_f:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _evaluate_single,
                    (r, engine_config_template, r.pop('_worker_id'))
                ): r
                for r in pending
            }

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc="Evaluating SQL (Systematic Parallel)"
            ):
                try:
                    result = future.result()
                    out_f.write(json.dumps(result) + '\n')
                    out_f.flush()
                    new_count += 1
                except Exception as e:
                    original = futures[future]
                    original['generated_sql'] = ''
                    original['is_equivalent'] = False
                    original['equivalence_details'] = f"Worker error: {str(e)}"
                    out_f.write(json.dumps(original) + '\n')
                    out_f.flush()
                    new_count += 1

    for wid in range(max_workers):
        workspace = f'./local_eval_workspace_worker_{wid}'
        if os.path.exists(workspace):
            shutil.rmtree(workspace, ignore_errors=True)

    return new_count


def generate_plots(df: pd.DataFrame, output_dir: str):
    """Generate plots - Source analysis is redundant as we filtered to baseline/systematic only, but keeping for consistency."""
    sns.set_theme(style='whitegrid')

    acc = df.groupby('model_name')['is_equivalent'].mean() * 100
    plt.figure(figsize=(10, 6))
    acc.plot(kind='barh')
    plt.title('Accuracy by Model (Baseline + Systematic Only)')
    plt.xlabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_model_systematic.png')
    # 2. Accuracy by Source
    acc_src = df.groupby('perturbation_source')['is_equivalent'].mean() * 100
    plt.figure(figsize=(8, 5))
    acc_src.plot(kind='bar')
    plt.title('Accuracy by Source')
    plt.ylabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_source_systematic.png')
    plt.close()

    # 3. Accuracy by Perturbation Type (Detailed)
    # Filter for systematic only to see specific types, or include all
    plt.figure(figsize=(12, 8))
    pert_acc = df.groupby('perturbation_type')['is_equivalent'].mean() * 100
    pert_acc = pert_acc.sort_values()
    pert_acc.plot(kind='barh')
    plt.title('Accuracy by Perturbation Type (Systematic)')
    plt.xlabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_perturbation_type.png')
    plt.close()

    # 4. Baseline vs Perturbation Accuracy by Model
    # Pivot data: Model | Baseline Acc | Systematic Acc
    pivot_data = df.groupby(['model_name', 'perturbation_source'])['is_equivalent'].mean().unstack() * 100
    # Reorder columns if possible
    if 'baseline' in pivot_data.columns and 'systematic' in pivot_data.columns:
        pivot_data = pivot_data[['baseline', 'systematic']]
    
    pivot_data.plot(kind='bar', figsize=(10, 6))
    plt.title('Baseline vs Systematic Perturbation Accuracy by Model')
    plt.ylabel('Accuracy (%)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/baseline_vs_perturbation_by_model.png')
    plt.close()

    print(f"📊 Plots saved to {output_dir}")


def main(run_outputs_dir=None, input_files=None, parallel=False, workers=4):
    engine = setup_equivalence_engine()

    df = aggregate_results(run_outputs_dir, input_files)
    if df.empty:
        return

    # --- FILTERING STEP ---
    # Keep only baseline and systematic
    initial_count = len(df)
    df = df[df['perturbation_source'].isin(['baseline', 'systematic'])]
    filtered_count = len(df)
    print(f"📉 Filtered records: {initial_count:,} -> {filtered_count:,} (Removed LLM perturbations)")
    
    if df.empty:
        print("⚠️ No baseline/systematic records found.")
        return

    if run_outputs_dir:
        output_dir = run_outputs_dir
    else:
        output_dir = os.path.dirname(input_files[0]) if input_files else '.'

    # Use a different output file to avoid conflict/append issues with the regular full run
    eval_path = f'{output_dir}/evaluated_results_systematic_only.jsonl'

    existing_keys = load_existing_evaluated(eval_path)

    if parallel:
        new_count = evaluate_dataframe_parallel(
            df, engine, eval_path, existing_keys, max_workers=workers
        )
    else:
        new_count = evaluate_dataframe_sequential(
            df, engine, eval_path, existing_keys
        )

    print(f"💾 {new_count:,} new records evaluated and appended to {eval_path}")

    # Reload for summary
    eval_records = []
    with open(eval_path, 'r') as f:
        for line in f:
            if line.strip():
                eval_records.append(json.loads(line))
    eval_df = pd.DataFrame(eval_records)

    print("=" * 40)
    print("SUMMARY (Systematic Only)")
    print("=" * 40)
    print(eval_df.groupby('model_name')['is_equivalent'].mean() * 100)

    generate_plots(eval_df, output_dir)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Evaluate experiment results (Systematic Only)."
    )
    parser.add_argument('output_dir', nargs='?', help='Directory containing results')
    parser.add_argument('--files', nargs='+', help='Specific result files to process')
    parser.add_argument('--parallel', action='store_true', help='Enable parallel evaluation')
    parser.add_argument('--workers', type=int, default=4,
                        help='Number of parallel workers (default: 4)')

    args = parser.parse_args()

    if args.files:
        main(input_files=args.files, parallel=args.parallel, workers=args.workers)
    elif args.output_dir:
        main(run_outputs_dir=args.output_dir, parallel=args.parallel, workers=args.workers)
    else:
        print("Usage: python analyze_results_systematic.py <output_dir> [--parallel] [--workers N]")
        sys.exit(1)
