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
import multiprocessing
from tqdm.auto import tqdm

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.sql_utils import extract_sql
from src.equivalence.equivalence_engine import SQLEquivalenceEngine
from src.equivalence.config import EquivalenceConfig, EquivalenceResult, EquivalenceCheckResult
from src.core.schema_loader import load_from_yaml

# Configuration Defaults
LOCAL_BASE = os.getenv('LOCAL_BASE', './')
REPO_PATH = os.getenv('REPO_PATH', f'{LOCAL_BASE}/')

# ---------------------------------------------------------------------------
# Opt 1: Gold SQL Test Suite Single-Slot Cache (DQL only)
# ---------------------------------------------------------------------------
# We use a single-slot cache because the test suite generator overwrites
# the same file names (test_db_0, etc.) for each new query.
# Keeping old cache entries would point to corrupted/overwritten files.
_last_gold_sql = None
_last_test_suite = None


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

    global _last_gold_sql, _last_test_suite

    # Check valid cache hit
    if _last_gold_sql == gold_sql and _last_test_suite is not None:
        test_databases = _last_test_suite
    else:
        # Cache miss or new query -> Generate new suite
        try:
            test_databases = checker.testsuite_gen.generate_test_suite(gold_sql)
            _last_gold_sql = gold_sql
            _last_test_suite = test_databases
        except Exception as e:
            return EquivalenceCheckResult(
                is_equivalent=False,
                result_type=EquivalenceResult.EXECUTION_ERROR,
                details=f"Failed to generate test suite: {str(e)}",
                gold_sql=gold_sql, candidate_sql=candidate_sql,
                query_type="SELECT"
            )

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
    """Generate a unique key for a record to detect duplicates.
    
    Uses fields that actually exist in the data:
    model_name, query_id, perturbation_source, perturbation_type, input_prompt (hash).
    The input_prompt hash ensures uniqueness even if perturbation_type has variants.
    """
    import hashlib
    prompt_hash = hashlib.md5(
        record.get('input_prompt', '').encode()
    ).hexdigest()[:8]
    parts = [
        record.get('model_name', ''),
        str(record.get('query_id', '')),
        record.get('perturbation_source', ''),
        record.get('perturbation_type', ''),
        prompt_hash,
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
def setup_equivalence_engine(local_workspace='./local_eval_workspace',
                             schema_path='schemas/social_media.yaml'):
    """Copy DBs locally and init engine."""
    if os.path.exists(local_workspace):
        shutil.rmtree(local_workspace)
    os.makedirs(local_workspace, exist_ok=True)

    # Load schema from YAML
    schema_cfg = load_from_yaml(schema_path)
    schema = schema_cfg.get_legacy_schema()
    foreign_keys = schema_cfg.get_fk_pairs()
    schema_name = schema_cfg.schema_name

    print(f"📂 Setting up evaluation workspace (schema: {schema_name})...")
    dest_db_dir = f'{local_workspace}/test_dbs'
    os.makedirs(dest_db_dir, exist_ok=True)

    # Create & seed base database from schema
    from src.equivalence.schema_adapter import create_database_from_schema
    from src.equivalence.seed_database import seed_database
    base_db = f'{dest_db_dir}/base.sqlite'
    create_database_from_schema(base_db, schema, foreign_keys, overwrite=True)
    seed_database(base_db, schema, foreign_keys)
    print("✅ Base database created and seeded.")

    config = EquivalenceConfig(
        base_db_path=base_db,
        test_suite_dir=dest_db_dir,
        schema=schema,
        foreign_keys=foreign_keys,
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
    Each subprocess uses PID-based identity for its isolated DB workspace.
    """
    record, engine_config_template = args
    engine = _get_or_create_worker_engine(engine_config_template)

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
_subprocess_pid = None


def _get_or_create_worker_engine(config_template):
    """
    Get or create a cached engine for this subprocess.
    Uses os.getpid() as identity so each process creates its engine
    exactly once, regardless of which tasks it receives.
    """
    global _subprocess_engine, _subprocess_pid

    pid = os.getpid()
    if _subprocess_engine is not None and _subprocess_pid == pid:
        return _subprocess_engine

    # Create isolated workspace for this process
    worker_workspace = f'./local_eval_workspace_worker_{pid}'
    dest_db_dir = f'{worker_workspace}/test_dbs'
    if not os.path.exists(worker_workspace):
        os.makedirs(dest_db_dir, exist_ok=True)
        # Create & seed base database from schema in worker workspace
        from src.equivalence.schema_adapter import create_database_from_schema
        from src.equivalence.seed_database import seed_database
        base_db = f'{dest_db_dir}/base.sqlite'
        schema = config_template.get('schema')
        fks = config_template.get('foreign_keys')
        if schema and fks:
            create_database_from_schema(base_db, schema, fks, overwrite=True)
            seed_database(base_db, schema, fks)
        else:
            # Fallback: copy existing test_dbs
            src_db_dir = config_template['source_db_dir']
            shutil.copytree(src_db_dir, dest_db_dir, dirs_exist_ok=True)

    config = EquivalenceConfig(
        base_db_path=f'{dest_db_dir}/base.sqlite',
        test_suite_dir=dest_db_dir,
        max_fuzz_iterations=config_template.get('max_fuzz_iterations', 100),
        max_distilled_dbs=config_template.get('max_distilled_dbs', 10),
        order_matters=config_template.get('order_matters', False),
        schema=config_template.get('schema'),
        foreign_keys=config_template.get('foreign_keys'),
    )

    _subprocess_engine = SQLEquivalenceEngine(config)
    _subprocess_pid = pid

    global _last_gold_sql, _last_test_suite
    _last_gold_sql = None
    _last_test_suite = None

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

    # Sort by gold_sql for cache affinity: consecutive records with the same
    # gold_sql will be sent to the same worker as a batch (via chunksize).
    pending.sort(key=lambda r: r.get('gold_sql', ''))

    engine_config_template = {
        'source_db_dir': f'{REPO_PATH}/test_dbs',
        'max_fuzz_iterations': engine.config.max_fuzz_iterations,
        'max_distilled_dbs': engine.config.max_distilled_dbs,
        'order_matters': engine.config.order_matters,
        'schema': engine.config.schema,
        'foreign_keys': engine.config.foreign_keys,
    }

    new_count = 0

    # Determine optimal chunksize: group size of perturbations per gold_sql
    # Typically ~5 perturbations per query, so chunksize=10 covers 2 queries.
    chunksize = 10

    # Prepare tasks (no worker_id needed — PID-based identity handles it)
    tasks = [(r, engine_config_template) for r in pending]

    with open(eval_path, 'a') as out_f:
        with multiprocessing.Pool(processes=max_workers, maxtasksperchild=100) as pool:
            try:
                for result in tqdm(
                    pool.imap_unordered(_evaluate_single, tasks, chunksize=chunksize),
                    total=len(tasks),
                    desc="Evaluating SQL (Systematic Parallel)"
                ):
                    try:
                        out_f.write(json.dumps(result) + '\n')
                        out_f.flush()
                        new_count += 1
                    except Exception as e:
                        print(f"⚠️ Error writing result: {e}")
            
            except Exception as e:
                print(f"\n❌ CRITICAL POOL ERROR: {e}")
                print(f"⚠️  Analysis crashed. Progress saved to {eval_path}.")
                print(f"👉 To resume, re-run the script.")

    # Cleanup worker workspaces (PID-based, so glob for them)
    import glob as glob_mod
    for workspace in glob_mod.glob('./local_eval_workspace_worker_*'):
        if os.path.isdir(workspace):
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

    # 5. Heatmap: Accuracy by Perturbation Type × Model (all models, one image)
    pivot = (
        df.groupby(['perturbation_type', 'model_name'])['is_equivalent']
        .mean()
        .mul(100)
        .unstack(level='model_name')
    )
    # Shorten model names for readability (strip org prefix)
    pivot.columns = [c.split('/')[-1] for c in pivot.columns]
    # Sort rows by mean accuracy ascending so hardest types are at the top
    pivot = pivot.loc[pivot.mean(axis=1).sort_values().index]

    fig, ax = plt.subplots(figsize=(max(10, len(pivot.columns) * 1.8), max(8, len(pivot) * 0.7)))
    sns.heatmap(
        pivot,
        annot=True,
        fmt='.1f',
        cmap='RdYlGn',
        linewidths=0.5,
        linecolor='#cccccc',
        vmin=0,
        vmax=100,
        ax=ax,
        cbar_kws={'label': 'Accuracy (%)'},
    )
    ax.set_title('Accuracy (%) by Perturbation Type × Model', fontsize=14, fontweight='bold', pad=12)
    ax.set_xlabel('Model', fontsize=11)
    ax.set_ylabel('Perturbation Type', fontsize=11)
    ax.tick_params(axis='x', rotation=30)
    ax.tick_params(axis='y', rotation=0)
    plt.tight_layout()
    heatmap_path = f'{output_dir}/accuracy_heatmap_perttype_x_model.png'
    plt.savefig(heatmap_path, dpi=150)
    plt.close()
    print(f"🗺️  Heatmap saved → {heatmap_path}")

    # 6. Heatmap: Accuracy by Complexity Type × Model (BASELINE only)
    baseline_df = df[df['perturbation_source'] == 'baseline'].copy()
    if not baseline_df.empty and 'complexity' in baseline_df.columns:
        cplx_pivot = (
            baseline_df.groupby(['complexity', 'model_name'])['is_equivalent']
            .mean()
            .mul(100)
            .unstack(level='model_name')
        )
        cplx_pivot.columns = [c.split('/')[-1] for c in cplx_pivot.columns]
        # Sort rows by mean accuracy ascending (hardest at top)
        cplx_pivot = cplx_pivot.loc[cplx_pivot.mean(axis=1).sort_values().index]

        fig, ax = plt.subplots(figsize=(max(10, len(cplx_pivot.columns) * 1.8), max(6, len(cplx_pivot) * 0.8)))
        sns.heatmap(
            cplx_pivot,
            annot=True,
            fmt='.1f',
            cmap='RdYlGn',
            linewidths=0.5,
            linecolor='#cccccc',
            vmin=0,
            vmax=100,
            ax=ax,
            cbar_kws={'label': 'Accuracy (%)'},
        )
        ax.set_title('Accuracy (%) by Query Complexity × Model — Baseline Only', fontsize=14, fontweight='bold', pad=12)
        ax.set_xlabel('Model', fontsize=11)
        ax.set_ylabel('Complexity Type', fontsize=11)
        ax.tick_params(axis='x', rotation=30)
        ax.tick_params(axis='y', rotation=0)
        plt.tight_layout()
        cplx_heatmap_path = f'{output_dir}/accuracy_heatmap_complexity_x_model_baseline.png'
        plt.savefig(cplx_heatmap_path, dpi=150)
        plt.close()
        print(f"🗺️  Complexity heatmap (baseline) saved → {cplx_heatmap_path}")
    else:
        print("⚠️  No baseline records with complexity field — skipping complexity heatmap.")

    print(f"📊 Plots saved to {output_dir}")


def main(run_outputs_dir=None, input_files=None, parallel=False, workers=4,
         schema_path='schemas/social_media.yaml'):
    engine = setup_equivalence_engine(schema_path=schema_path)

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
    parser.add_argument('--schema', default='schemas/social_media.yaml',
                        help='Path to schema YAML file (default: schemas/social_media.yaml)')
    parser.add_argument('--parallel', action='store_true', help='Enable parallel evaluation')
    import psutil
    
    # Smart Default for Workers
    # If explicit --workers is passed, use it.
    # Otherwise, calculate safe default based on RAM.
    
    # 1. Total RAM in GB
    total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
    cpu_count = os.cpu_count() or 4
    
    # Heuristic: Each worker needs ~2-3GB (Engine + DBs + spaCy)
    # Safe count = RAM / 3GB, capped at CPU count, capped at 8 (user pref)
    safe_workers_by_ram = int(total_ram_gb / 3)
    if safe_workers_by_ram < 1:
        safe_workers_by_ram = 1
        
    # Default is the minimum of (8, cpu_count, safe_by_ram)
    # But ensuring at least 1 or 2
    default_workers = min(8, cpu_count, safe_workers_by_ram)
    # Ensure reasonable floor if RAM is huge but CPUs are few
    default_workers = max(1, default_workers)

    parser.add_argument('--workers', type=int, default=default_workers,
                        help=f'Number of parallel workers (default: {default_workers} based on {total_ram_gb:.1f}GB RAM)')

    args = parser.parse_args()

    # Warn if user manually requests unsafe count
    if args.workers > safe_workers_by_ram:
        print(f"\n⚠️  WARNING: {args.workers} workers requested, but system has {total_ram_gb:.1f}GB RAM.")
        print(f"   Safe estimated max is {safe_workers_by_ram} workers.")
        print(f"   If script crashes with 'Worker error', reduce --workers.\n")


    if args.files:
        main(input_files=args.files, parallel=args.parallel, workers=args.workers,
             schema_path=args.schema)
    elif args.output_dir:
        main(run_outputs_dir=args.output_dir, parallel=args.parallel, workers=args.workers,
             schema_path=args.schema)
    else:
        print("Usage: python analyze_results_systematic.py <output_dir> [--parallel] [--workers N]")
        sys.exit(1)
