"""
Script for Analyzing Experiment Results.
Handles EVALUATION (SQL Equivalence) and PLOTTING.
Aggregates all results from a given run directory.

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
    # DML queries: delegate directly — DML checker has its own test suite logic
    if not _is_dql(gold_sql):
        return engine.check_equivalence(gold_sql, candidate_sql)

    # --- DQL path: use test suite cache ---
    cache_key = _normalize_sql_for_cache(gold_sql)
    checker = engine.dql_checker

    # Detect type mismatch first
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

    # Compare denotations using cached test DBs
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

    # Get or create engine using PID-based identity (created once per process)
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


# Per-subprocess engine cache (avoids re-creating engine for every record)
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

    # Also reset the test suite cache for this subprocess
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
    Returns count of newly evaluated records.
    """
    records = df.to_dict('records')
    new_count = 0
    skipped = 0

    # Opt 6: Open file in append mode for streaming writes
    with open(eval_path, 'a') as out_f:
        for r in tqdm(records, desc="Evaluating SQL"):
            key = _record_key(r)
            if key in existing_keys:
                skipped += 1
                continue  # Opt 2: Skip already evaluated

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

            # Opt 6: Write immediately
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
    Each worker gets its own isolated DB workspace to prevent race conditions.
    Records are sorted by gold_sql to maximize cache hits within each worker.
    Returns count of newly evaluated records.
    """
    records = df.to_dict('records')

    # Filter out already-evaluated records (Opt 2)
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
    chunksize = 10

    # Prepare tasks (no worker_id needed — PID-based identity handles it)
    tasks = [(r, engine_config_template) for r in pending]

    with open(eval_path, 'a') as out_f:
        with multiprocessing.Pool(processes=max_workers, maxtasksperchild=100) as pool:
            try:
                for result in tqdm(
                    pool.imap_unordered(_evaluate_single, tasks, chunksize=chunksize),
                    total=len(tasks),
                    desc="Evaluating SQL (parallel)"
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
    """Generate comprehensive plots covering all analysis dimensions:
       Model × Complexity × Perturbation Type × Source.
    """
    import numpy as np

    sns.set_theme(style='whitegrid')
    # Shorten model names helper
    def short_name(name):
        return name.split('/')[-1]

    # =========================================================================
    # 1. Accuracy by Model
    # =========================================================================
    acc = df.groupby('model_name')['is_equivalent'].mean() * 100
    acc.index = [short_name(m) for m in acc.index]
    acc = acc.sort_values()
    plt.figure(figsize=(10, 6))
    bars = acc.plot(kind='barh', color=sns.color_palette('viridis', len(acc)))
    for i, v in enumerate(acc):
        plt.text(v + 0.5, i, f'{v:.1f}%', va='center', fontsize=9)
    plt.title('Overall Accuracy by Model', fontsize=14, fontweight='bold')
    plt.xlabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_model.png', dpi=150)
    plt.close()

    # =========================================================================
    # 2. Accuracy by Source × Model (grouped bar)
    # =========================================================================
    pivot_src = df.groupby(['model_name', 'perturbation_source'])['is_equivalent'].mean().unstack() * 100
    pivot_src.index = [short_name(m) for m in pivot_src.index]
    # Reorder columns sensibly
    col_order = [c for c in ['baseline', 'systematic', 'llm'] if c in pivot_src.columns]
    if col_order:
        pivot_src = pivot_src[col_order]

    pivot_src.plot(kind='bar', figsize=(12, 6), width=0.8)
    plt.title('Accuracy by Source × Model', fontsize=14, fontweight='bold')
    plt.ylabel('Accuracy (%)')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Source')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_source_x_model.png', dpi=150)
    plt.close()


    # =========================================================================
    # 5. Heatmap: Perturbation Type × Model
    # =========================================================================
    pivot_pt = (
        df.groupby(['perturbation_type', 'model_name'])['is_equivalent']
        .mean().mul(100).unstack(level='model_name')
    )
    pivot_pt.columns = [short_name(c) for c in pivot_pt.columns]
    pivot_pt = pivot_pt.loc[pivot_pt.mean(axis=1).sort_values().index]

    fig, ax = plt.subplots(figsize=(max(10, len(pivot_pt.columns) * 1.8),
                                    max(8, len(pivot_pt) * 0.7)))
    sns.heatmap(
        pivot_pt, annot=True, fmt='.1f', cmap='RdYlGn',
        linewidths=0.5, linecolor='#cccccc', vmin=0, vmax=100,
        ax=ax, cbar_kws={'label': 'Accuracy (%)'},
    )
    ax.set_title('Accuracy (%) — Perturbation Type × Model',
                 fontsize=14, fontweight='bold', pad=12)
    ax.set_xlabel('Model', fontsize=11)
    ax.set_ylabel('Perturbation Type', fontsize=11)
    ax.tick_params(axis='x', rotation=30)
    ax.tick_params(axis='y', rotation=0)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/heatmap_perttype_x_model.png', dpi=150)
    plt.close()
    print(f"🗺️  Heatmap saved → heatmap_perttype_x_model.png")

    # =========================================================================
    # 6. Heatmap: Complexity × Model (baseline only)
    # =========================================================================
    baseline_df = df[df['perturbation_source'] == 'baseline'].copy()
    if not baseline_df.empty and 'complexity' in baseline_df.columns:
        cplx_pivot = (
            baseline_df.groupby(['complexity', 'model_name'])['is_equivalent']
            .mean().mul(100).unstack(level='model_name')
        )
        cplx_pivot.columns = [short_name(c) for c in cplx_pivot.columns]
        cplx_pivot = cplx_pivot.loc[cplx_pivot.mean(axis=1).sort_values().index]

        fig, ax = plt.subplots(figsize=(max(10, len(cplx_pivot.columns) * 1.8),
                                        max(6, len(cplx_pivot) * 0.8)))
        sns.heatmap(
            cplx_pivot, annot=True, fmt='.1f', cmap='RdYlGn',
            linewidths=0.5, linecolor='#cccccc', vmin=0, vmax=100,
            ax=ax, cbar_kws={'label': 'Accuracy (%)'},
        )
        ax.set_title('Accuracy (%) — Query Complexity × Model (Baseline)',
                     fontsize=14, fontweight='bold', pad=12)
        ax.set_xlabel('Model', fontsize=11)
        ax.set_ylabel('Complexity', fontsize=11)
        ax.tick_params(axis='x', rotation=30)
        ax.tick_params(axis='y', rotation=0)
        plt.tight_layout()
        plt.savefig(f'{output_dir}/heatmap_complexity_x_model_baseline.png', dpi=150)
        plt.close()
        print(f"🗺️  Complexity heatmap saved → heatmap_complexity_x_model_baseline.png")
    else:
        print("⚠️  No baseline records with complexity — skipping complexity heatmap.")

    # =========================================================================
    # 7. Accuracy Drop from Baseline (Δ chart per model × source)
    # =========================================================================
    if not baseline_df.empty:
        baseline_acc = baseline_df.groupby('model_name')['is_equivalent'].mean() * 100
        baseline_acc.index = [short_name(m) for m in baseline_acc.index]

        non_baseline = df[df['perturbation_source'] != 'baseline']
        sources = non_baseline['perturbation_source'].unique()
        if len(sources) > 0:
            delta_data = {}
            for src in sources:
                src_acc = (non_baseline[non_baseline['perturbation_source'] == src]
                           .groupby('model_name')['is_equivalent'].mean() * 100)
                src_acc.index = [short_name(m) for m in src_acc.index]
                delta_data[src] = src_acc - baseline_acc

            delta_df = pd.DataFrame(delta_data).dropna()
            if not delta_df.empty:
                delta_df = delta_df.loc[delta_df.mean(axis=1).sort_values().index]
                fig, ax = plt.subplots(figsize=(12, 6))
                delta_df.plot(kind='bar', ax=ax, width=0.7)
                ax.axhline(y=0, color='black', linewidth=0.8, linestyle='--')
                ax.set_title('Accuracy Drop from Baseline (Δ%)',
                             fontsize=14, fontweight='bold')
                ax.set_ylabel('Δ Accuracy (% points)')
                ax.set_xlabel('Model')
                plt.xticks(rotation=45, ha='right')
                plt.legend(title='Source')
                plt.tight_layout()
                plt.savefig(f'{output_dir}/accuracy_delta_from_baseline.png', dpi=150)
                plt.close()
                print(f"📉 Delta chart saved → accuracy_delta_from_baseline.png")

    # =========================================================================
    # 8. Heatmap: Complexity × Perturbation Type (aggregated across models)
    # =========================================================================
    if 'complexity' in df.columns:
        perturbed_df = df[df['perturbation_source'] != 'baseline']
        if not perturbed_df.empty:
            cpt_pivot = (
                perturbed_df.groupby(['complexity', 'perturbation_type'])['is_equivalent']
                .mean().mul(100).unstack(level='perturbation_type')
            )
            if not cpt_pivot.empty and cpt_pivot.shape[1] > 1:
                # Sort columns by mean accuracy
                cpt_pivot = cpt_pivot[cpt_pivot.mean().sort_values().index]
                fig, ax = plt.subplots(figsize=(max(12, cpt_pivot.shape[1] * 0.9),
                                                max(5, cpt_pivot.shape[0] * 1.2)))
                sns.heatmap(
                    cpt_pivot, annot=True, fmt='.1f', cmap='RdYlGn',
                    linewidths=0.5, linecolor='#cccccc', vmin=0, vmax=100,
                    ax=ax, cbar_kws={'label': 'Accuracy (%)'},
                )
                ax.set_title('Accuracy (%) — Complexity × Perturbation Type (All Models)',
                             fontsize=13, fontweight='bold', pad=12)
                ax.set_xlabel('Perturbation Type', fontsize=11)
                ax.set_ylabel('Complexity', fontsize=11)
                ax.tick_params(axis='x', rotation=45, labelsize=8)
                ax.tick_params(axis='y', rotation=0)
                plt.tight_layout()
                plt.savefig(f'{output_dir}/heatmap_complexity_x_perttype.png', dpi=150)
                plt.close()
                print(f"🗺️  Complexity×PertType heatmap saved → heatmap_complexity_x_perttype.png")

    # =========================================================================
    # 9. Per-Model Faceted Heatmaps: Complexity × Perturbation Type
    # =========================================================================
    if 'complexity' in df.columns:
        models = df['model_name'].unique()
        perturbed_df = df[df['perturbation_source'] != 'baseline']
        if not perturbed_df.empty and len(models) > 0:
            n_models = len(models)
            ncols = min(3, n_models)
            nrows = (n_models + ncols - 1) // ncols

            fig, axes = plt.subplots(nrows, ncols,
                                      figsize=(ncols * 7, nrows * 5),
                                      squeeze=False)
            for idx, model in enumerate(sorted(models)):
                row, col = divmod(idx, ncols)
                ax = axes[row][col]
                model_df = perturbed_df[perturbed_df['model_name'] == model]
                if model_df.empty:
                    ax.set_visible(False)
                    continue
                facet_pivot = (
                    model_df.groupby(['complexity', 'perturbation_type'])['is_equivalent']
                    .mean().mul(100).unstack(level='perturbation_type')
                )
                if facet_pivot.empty:
                    ax.set_visible(False)
                    continue
                sns.heatmap(
                    facet_pivot, annot=True, fmt='.0f', cmap='RdYlGn',
                    linewidths=0.3, vmin=0, vmax=100, ax=ax,
                    cbar=False, annot_kws={'fontsize': 7},
                )
                ax.set_title(short_name(model), fontsize=11, fontweight='bold')
                ax.set_xlabel('')
                ax.set_ylabel('')
                ax.tick_params(axis='x', rotation=45, labelsize=7)
                ax.tick_params(axis='y', rotation=0, labelsize=8)

            # Hide unused subplots
            for idx in range(n_models, nrows * ncols):
                row, col = divmod(idx, ncols)
                axes[row][col].set_visible(False)

            fig.suptitle('Accuracy (%) — Complexity × Perturbation Type per Model',
                         fontsize=15, fontweight='bold', y=1.01)
            plt.tight_layout()
            plt.savefig(f'{output_dir}/faceted_heatmaps_per_model.png',
                        dpi=150, bbox_inches='tight')
            plt.close()
            print(f"🗺️  Faceted per-model heatmaps saved → faceted_heatmaps_per_model.png")

    # =========================================================================
    # 10. Accuracy by Complexity × Source (grouped bar)
    # =========================================================================
    if 'complexity' in df.columns:
        cplx_src = df.groupby(['complexity', 'perturbation_source'])['is_equivalent'].mean().unstack() * 100
        col_order = [c for c in ['baseline', 'systematic', 'llm'] if c in cplx_src.columns]
        if col_order:
            cplx_src = cplx_src[col_order]
        cplx_src.plot(kind='bar', figsize=(10, 6), width=0.8)
        plt.title('Accuracy by Complexity × Source', fontsize=14, fontweight='bold')
        plt.ylabel('Accuracy (%)')
        plt.xlabel('Complexity')
        plt.xticks(rotation=0)
        plt.legend(title='Source')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/accuracy_complexity_x_source.png', dpi=150)
        plt.close()

    print(f"📊 All plots saved to {output_dir}")


def main(run_outputs_dir=None, input_files=None, parallel=False, workers=4,
         schema_path='schemas/social_media.yaml'):
    # 1. Init Engine
    engine = setup_equivalence_engine(schema_path=schema_path)

    # 2. Aggregate
    df = aggregate_results(run_outputs_dir, input_files)
    if df.empty:
        return

    # 3. Determine output path
    if run_outputs_dir:
        output_dir = run_outputs_dir
    else:
        output_dir = os.path.dirname(input_files[0]) if input_files else '.'

    eval_path = f'{output_dir}/evaluated_results_aggregated.jsonl'

    # Opt 2: Load already-evaluated records
    existing_keys = load_existing_evaluated(eval_path)

    # 4. Evaluate (with caching, resume, streaming)
    if parallel:
        new_count = evaluate_dataframe_parallel(
            df, engine, eval_path, existing_keys, max_workers=workers
        )
    else:
        new_count = evaluate_dataframe_sequential(
            df, engine, eval_path, existing_keys
        )

    print(f"💾 {new_count:,} new records evaluated and appended to {eval_path}")

    # 5. Summary Report (reload full evaluated file)
    eval_records = []
    with open(eval_path, 'r') as f:
        for line in f:
            if line.strip():
                eval_records.append(json.loads(line))
    eval_df = pd.DataFrame(eval_records)

    print("=" * 40)
    print("SUMMARY")
    print("=" * 40)
    print(eval_df.groupby('model_name')['is_equivalent'].mean() * 100)

    # 6. Plot
    generate_plots(eval_df, output_dir)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Evaluate experiment results with SQL equivalence checking."
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
        print("Usage: python analyze_results.py <output_dir> [--parallel] [--workers N]")
        print("       python analyze_results.py --files file1 file2 ... [--parallel]")
        sys.exit(1)
