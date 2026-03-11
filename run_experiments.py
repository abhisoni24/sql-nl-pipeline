"""
End-to-end SQL generation experiment pipeline.

  1. Discover databases (.yaml / .sqlite) in the input directory.
  2. For each database, generate a dictionary and run steps 01-03:
       - 01: Raw SQL queries
       - 02: Natural-language prompts (two-pass with dictionary)
       - 02b: Validate raw SQL + NL prompts via pipeline_tests/
       - 03: Systematic perturbations
       - 03b: Validate systematic perturbations via pipeline_tests/
  2b. Run step 04 (LLM perturbations) once across all databases
      so the model is loaded only once (optional).
  3. Flatten all tasks across databases into a single list with
     full provenance (schema_name, query_id, perturbation_source,
     perturbation_type) so every result can be traced to its gold SQL.
  4. Save the flattened list + experiment results in
     experiment_workspace/runs/<timestamp>/.
  5. Feed the flattened list through each target LLM model.
  6. Save results (JSONL) for later analysis.
  python run_experiments.py --db-dir dataset/dbs_to_test --skip-generation

  # Only specific models
  python run_experiments.py --db-dir dataset/dbs_to_test --models llama3.1-8b deepseek-coder-v2-lite

  # Skip LLM perturbation generation (step 04)
  python run_experiments.py --db-dir dataset/dbs_to_test --no-llm-perturbations
"""
import gc
import json
import os
import random
import subprocess
import sys
import yaml
from collections import Counter
from datetime import datetime
from pathlib import Path

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.core.schema_loader import load_schema
from src.utils.data_loader import (
    load_baseline_queries,
    load_systematic_perturbations,
    load_llm_perturbations,
)
from src.utils.storage_manager import StorageManager

# ── Configuration defaults (overridable via env vars) ────────────────────
IS_COLAB = os.path.exists('/content')

DEFAULT_LOCAL_BASE = '/content/experiment_workspace' if IS_COLAB else './experiment_workspace'
DEFAULT_REPO_PATH = f'{DEFAULT_LOCAL_BASE}/sql-nl' if IS_COLAB else '.'
DEFAULT_DRIVE_BASE = '/content/drive/MyDrive/ExpResults' if IS_COLAB else './drive_backup'

LOCAL_BASE = os.getenv('LOCAL_BASE', DEFAULT_LOCAL_BASE)
REPO_PATH = os.getenv('REPO_PATH', DEFAULT_REPO_PATH)
DRIVE_BASE = os.getenv('DRIVE_BASE', DEFAULT_DRIVE_BASE)
CONFIG_PATH = os.path.join(REPO_PATH, 'experiments.yaml')
DATASET_DIR = os.path.join(REPO_PATH, 'dataset')

SUPPORTED_DB_EXTENSIONS = {'.yaml', '.yml', '.sqlite', '.db', '.sqlite3'}


# ╔════════════════════════════════════════════════════════════════════════╗
# ║  Phase 1 — Discovery                                                 ║
# ╚════════════════════════════════════════════════════════════════════════╝

def discover_databases(db_dir: str) -> list:
    """Scan *db_dir* for database definition files (.yaml, .sqlite, etc.).

    Returns a sorted list of ``Path`` objects.
    """
    db_dir = Path(db_dir)
    if not db_dir.is_dir():
        raise FileNotFoundError(f"Database directory not found: {db_dir}")

    dbs = sorted(
        p for p in db_dir.iterdir()
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_DB_EXTENSIONS
        and not p.name.endswith('_dictionary.yaml')
    )
    if not dbs:
        raise FileNotFoundError(
            f"No database files ({', '.join(SUPPORTED_DB_EXTENSIONS)}) found in {db_dir}"
        )
    return dbs


# ╔════════════════════════════════════════════════════════════════════════╗
# ║  Phase 2 — Dataset Generation (steps 00–03)                          ║
# ╚════════════════════════════════════════════════════════════════════════╝

def _run_step(cmd: list, step_label: str, verbose: bool = False) -> bool:
    """Run a pipeline step as a subprocess. Returns ``True`` on success."""
    print(f"  ▸ {step_label}")
    if verbose:
        result = subprocess.run(cmd, cwd=REPO_PATH)
    else:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_PATH)
    if result.returncode != 0:
        if not verbose and result.stderr:
            print(f"    ✗ FAILED:\n{result.stderr.strip()[-500:]}")
        elif not verbose:
            stdout_tail = (result.stdout or "").strip()[-500:]
            print(f"    ✗ FAILED (rc={result.returncode}):\n{stdout_tail}")
        return False
    return True


def _run_validation(
    cmd: list,
    step_label: str,
    log_path: str,
    verbose: bool = False,
) -> bool:
    """Run a validation test script, save output to *log_path*.

    Returns ``True`` if the test passed (exit code 0).
    """
    print(f"  ▸ {step_label}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_PATH)
    combined = (result.stdout or "") + (result.stderr or "")

    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "w") as fh:
        fh.write(combined)

    if result.returncode != 0:
        # Show a compact summary on failure
        tail = combined.strip().splitlines()[-5:]
        print(f"    ✗ FAILED (log: {log_path})")
        for ln in tail:
            print(f"      {ln}")
        return False

    if verbose:
        for ln in combined.strip().splitlines():
            print(f"    {ln}")
    else:
        # Print just the last summary line
        lines = combined.strip().splitlines()
        if lines:
            print(f"    {lines[-1]}")
    return True


def generate_datasets(
    db_paths: list,
    num_per_complexity: int,
    *,
    two_pass: bool = True,
    skip_existing: bool = False,
    verbose: bool = False,
    test_log_dir: str | None = None,
) -> list:
    """Run the generation pipeline (dictionary + steps 01-03) for each database.

    Returns a list of *schema_name* strings for databases that completed
    successfully (through step 03).
    """
    py = sys.executable
    successful_schemas = []

    for db_path in db_paths:
        schema_cfg = load_schema(str(db_path))
        schema_name = schema_cfg.schema_name
        out_dir = os.path.join(DATASET_DIR, schema_name)
        schema_arg = str(db_path)

        print(f"\n{'─' * 60}")
        print(f"Database: {schema_name}  ({db_path.name})")
        print(f"{'─' * 60}")

        # Fast-path: all outputs already exist
        required_files = [
            f"{out_dir}/raw_queries.json",
            f"{out_dir}/nl_prompts.json",
            f"{out_dir}/systematic_perturbations.json",
        ]
        if skip_existing and all(os.path.exists(f) for f in required_files):
            print("  ✓ Datasets already exist — skipping generation")
            successful_schemas.append(schema_name)
            continue

        # Step 0 — Dictionary
        dict_path = os.path.join(
            str(db_path.parent), f"{schema_name}_dictionary.yaml"
        )
        if not os.path.exists(dict_path):
            if not _run_step(
                [py, "generate_dictionary.py",
                 "--schema", schema_arg,
                 "--outdir", str(db_path.parent)],
                "Step 00: Generating dictionary",
                verbose=verbose,
            ):
                print(f"  ✗ Dictionary generation failed — skipping {schema_name}")
                continue

        # Step 1 — Raw SQL queries
        raw_path = f"{out_dir}/raw_queries.json"
        if not _run_step(
            [py, "01_generate_sql_dataset.py",
             "--schema", schema_arg,
             "-n", str(num_per_complexity),
             "-o", raw_path],
            f"Step 01: Generating {num_per_complexity} queries/complexity",
            verbose=verbose,
        ):
            print(f"  ✗ SQL generation failed — skipping {schema_name}")
            continue

        # Step 2 — NL prompts
        nl_path = f"{out_dir}/nl_prompts.json"
        step2_cmd = [
            py, "02_generate_nl_prompts.py",
            "--schema", schema_arg,
            "-i", raw_path,
            "-o", nl_path,
        ]
        if two_pass:
            step2_cmd.append("--two-pass")
            if os.path.exists(dict_path):
                step2_cmd.extend(["--dictionary", dict_path])
        if not _run_step(step2_cmd, "Step 02: Generating NL prompts", verbose=verbose):
            print(f"  ✗ NL prompt generation failed — skipping {schema_name}")
            continue

        # Step 2b — Validate raw SQL + NL prompts
        if test_log_dir:
            db_test_dir = os.path.join(test_log_dir, schema_name)
            os.makedirs(db_test_dir, exist_ok=True)

            # SQL validation
            sql_test_cmd = [
                py, "pipeline_tests/generation_process/sql/test_sql_generation.py",
                "-i", raw_path,
                "--schema", schema_arg,
            ]
            _run_validation(
                sql_test_cmd,
                "Step 02b: Validating raw SQL queries",
                f"{db_test_dir}/test_sql_generation.txt",
                verbose=verbose,
            )

            # NL prompt validation
            nl_test_cmd = [
                py, "pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py",
                "-i", nl_path,
                "--schema", schema_arg,
            ]
            if os.path.exists(dict_path):
                nl_test_cmd.extend(["--dictionary", dict_path])
            _run_validation(
                nl_test_cmd,
                "Step 02b: Validating NL prompts",
                f"{db_test_dir}/test_nl_prompt.txt",
                verbose=verbose,
            )

        # Step 3 — Systematic perturbations
        sys_path = f"{out_dir}/systematic_perturbations.json"
        if not _run_step(
            [py, "03_generate_systematic_perturbations.py",
             "--schema", schema_arg,
             "-i", nl_path,
             "-o", sys_path],
            "Step 03: Generating systematic perturbations",
            verbose=verbose,
        ):
            print(f"  ✗ Systematic perturbation generation failed — skipping {schema_name}")
            continue

        # Step 3b — Validate systematic perturbations
        if test_log_dir:
            db_test_dir = os.path.join(test_log_dir, schema_name)
            sys_test_cmd = [
                py,
                "pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py",
                "-i", sys_path,
                "--schema", schema_arg,
                "--dictionary", dict_path,
            ]
            _run_validation(
                sys_test_cmd,
                "Step 03b: Validating systematic perturbations",
                f"{db_test_dir}/test_systematic_perturbations.txt",
                verbose=verbose,
            )

        print(f"  ✓ {schema_name} complete")
        successful_schemas.append(schema_name)

    return successful_schemas


def generate_llm_perturbations(
    db_paths: list,
    schema_names: list,
    llm_model: str,
    *,
    verbose: bool = False,
) -> None:
    """Run step 04 (LLM perturbation generation) for all databases.

    The vLLM model is loaded **once** and reused across all schemas.
    Only the system prompt (which embeds the schema context) is swapped
    between databases.  Failures are non-fatal.
    """
    import importlib

    # Import step-04 helpers (filename starts with a digit)
    step04 = importlib.import_module("04_generate_llm_nl_and_perturbations")

    schema_name_set = set(schema_names)

    print(f"\n{'━' * 60}")
    print(f"Step 04 — LLM perturbations (model: {llm_model})")
    print(f"{'━' * 60}")

    # Collect databases to process
    to_process = []
    for db_path in db_paths:
        schema_cfg = load_schema(str(db_path))
        schema_name = schema_cfg.schema_name
        if schema_name not in schema_name_set:
            continue

        out_dir = os.path.join(DATASET_DIR, schema_name)
        raw_path = f"{out_dir}/raw_queries.json"
        llm_path = f"{out_dir}/llm_perturbations.json"

        if not os.path.exists(raw_path):
            print(f"  ⚠ {schema_name}: raw_queries.json not found — skipping")
            continue
        to_process.append((db_path, schema_name, raw_path, llm_path))

    if not to_process:
        print("  Nothing to process.")
        print(f"{'━' * 60}")
        return

    # Build perturbation definitions once (shared across schemas)
    perturbation_text = step04._load_perturbation_text()

    # Create adapter ONCE using the first schema's context
    first_db_path = to_process[0][0]
    schema_ctx = step04._build_schema_context(str(first_db_path))
    system_prompt = step04._build_system_prompt(schema_ctx, perturbation_text)

    adapter, model_cfg = step04._create_adapter_from_config(
        llm_model,
        CONFIG_PATH,
        max_tokens=step04.DEFAULT_MAX_TOKENS,
        temperature=step04.DEFAULT_TEMPERATURE,
        system_prompt=system_prompt,
    )
    model_id = model_cfg.model_identifier
    print(f"  Model loaded: {model_id}")

    # Process each database — only swap the system prompt, not the model
    for db_path, schema_name, raw_path, llm_path in to_process:
        schema_ctx = step04._build_schema_context(str(db_path))
        system_prompt = step04._build_system_prompt(schema_ctx, perturbation_text)
        adapter._system_prompt = system_prompt

        print(f"  ▸ {schema_name}")
        try:
            step04.process_queries(
                input_file=raw_path,
                output_file=llm_path,
                adapter=adapter,
                model_id=model_id,
                schema_path=str(db_path),
            )
        except Exception as exc:
            print(f"    ⚠ {schema_name}: LLM perturbation generation failed: {exc}")

    # Free GPU memory
    del adapter
    gc.collect()
    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            print("  🧹 GPU memory cleared")
    except ImportError:
        pass

    print(f"{'━' * 60}")


# ╔════════════════════════════════════════════════════════════════════════╗
# ║  Phase 3 — Task Flattening                                           ║
# ╚════════════════════════════════════════════════════════════════════════╝

def flatten_all_tasks(dataset_dir: str, schema_names: list) -> list:
    """Load and flatten tasks from all schemas into a single shuffled list.

    Every task dict carries:
      - ``schema_name``         — which database it belongs to
      - ``job_id``              — globally unique (prefixed with schema_name)
      - ``query_id``            — original query id within the schema
      - ``perturbation_source`` — baseline / systematic / llm
      - ``perturbation_type``   — original, typos, verbosity_variation, …
      - ``input_prompt``        — the NL prompt to send to the LLM under test
      - ``gold_sql``            — expected SQL
      - ``complexity``, ``tables``
    """
    all_tasks = []

    for schema_name in schema_names:
        base = os.path.join(dataset_dir, schema_name)

        # Baseline (from nl_prompts.json)
        nl_path = f"{base}/nl_prompts.json"
        if os.path.exists(nl_path):
            tasks = load_baseline_queries(nl_path)
            for t in tasks:
                t['schema_name'] = schema_name
                t['job_id'] = f"{schema_name}_{t['job_id']}"
            all_tasks.extend(tasks)

        # Systematic perturbations
        sys_path = f"{base}/systematic_perturbations.json"
        if os.path.exists(sys_path):
            tasks = load_systematic_perturbations(sys_path)
            for t in tasks:
                t['schema_name'] = schema_name
                t['job_id'] = f"{schema_name}_{t['job_id']}"
            all_tasks.extend(tasks)

        # LLM perturbations
        llm_path = f"{base}/llm_perturbations.json"
        if os.path.exists(llm_path):
            tasks = load_llm_perturbations(llm_path)
            for t in tasks:
                t['schema_name'] = schema_name
                t['job_id'] = f"{schema_name}_{t['job_id']}"
            all_tasks.extend(tasks)

    random.shuffle(all_tasks)
    return all_tasks


# ╔════════════════════════════════════════════════════════════════════════╗
# ║  Phase 4 — Experiment Execution                                      ║
# ╚════════════════════════════════════════════════════════════════════════╝

def setup_directories(run_timestamp: str):
    """Create local directories for this run."""
    local_run_dir = f'{LOCAL_BASE}/runs/{run_timestamp}'
    inputs_dir = f'{local_run_dir}/inputs'
    outputs_dir = f'{local_run_dir}/outputs'
    logs_dir = f'{local_run_dir}/logs'

    for d in [inputs_dir, outputs_dir, logs_dir]:
        os.makedirs(d, exist_ok=True)

    return local_run_dir, inputs_dir, outputs_dir


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="End-to-end SQL generation experiment pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s --db-dir dataset/dbs_to_test -n 50
  %(prog)s --db-dir dataset/dbs_to_test --skip-generation
  %(prog)s --db-dir dataset/dbs_to_test --models llama3.1-8b deepseek-coder-v2-lite
  %(prog)s --db-dir dataset/dbs_to_test --no-llm-perturbations
""",
    )

    # ── I/O ──
    parser.add_argument(
        '--db-dir', required=True,
        help='Directory containing database files (.yaml, .sqlite, etc.)',
    )
    parser.add_argument(
        '--num-per-complexity', '-n', type=int, default=50,
        help='SQL queries to generate per complexity type (default: 50)',
    )

    # ── Generation control ──
    gen = parser.add_argument_group('Generation control')
    gen.add_argument(
        '--skip-generation', action='store_true',
        help='Skip steps 01-04; use existing datasets in dataset/',
    )
    gen.add_argument(
        '--skip-existing', action='store_true',
        help='Skip generation for databases whose dataset files already exist',
    )
    gen.add_argument(
        '--no-llm-perturbations', action='store_true',
        help='Skip step 04 (LLM perturbation generation)',
    )
    gen.add_argument(
        '--perturbation-model', default='qwen3-14b',
        help='Model (from experiments.yaml) for step 04 (default: qwen3-14b)',
    )
    gen.add_argument(
        '--no-two-pass', action='store_true',
        help='Disable two-pass NL rendering in step 02',
    )
    gen.add_argument(
        '--verbose', '-v', action='store_true',
        help='Stream subprocess output for generation steps',
    )

    # ── Evaluation control ──
    ev = parser.add_argument_group('Evaluation control')
    ev.add_argument(
        '--models', nargs='*', default=None,
        help='Model names from experiments.yaml to evaluate (default: all vLLM models)',
    )
    ev.add_argument(
        '--generation-only', action='store_true',
        help='Run only the generation phase (steps 01-04), skip LLM evaluation',
    )

    cli_args = parser.parse_args()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Phase 1 — Discover databases & create run directory
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    db_paths = discover_databases(cli_args.db_dir)
    print(f"Found {len(db_paths)} database(s) in {cli_args.db_dir}:")
    for p in db_paths:
        print(f"  • {p.name}")

    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_run_dir, inputs_dir, outputs_dir = setup_directories(run_timestamp)
    logs_dir = os.path.join(local_run_dir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    print(f"\n📂 Run directory: {local_run_dir}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Phase 2 — Generate datasets (with validation tests)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if cli_args.skip_generation:
        schema_names = []
        for p in db_paths:
            cfg = load_schema(str(p))
            sdir = os.path.join(DATASET_DIR, cfg.schema_name)
            if os.path.isdir(sdir):
                schema_names.append(cfg.schema_name)
            else:
                print(f"  ⚠ No dataset directory for '{cfg.schema_name}' — skipping")
        print(f"\nUsing existing datasets for: {schema_names}")
    else:
        schema_names = generate_datasets(
            db_paths,
            num_per_complexity=cli_args.num_per_complexity,
            two_pass=not cli_args.no_two_pass,
            skip_existing=cli_args.skip_existing,
            verbose=cli_args.verbose,
            test_log_dir=logs_dir,
        )
        print(f"\n✓ Dataset generation complete for {len(schema_names)} schema(s)")

        # Step 04 — LLM perturbations (separate pass to avoid repeated model loading)
        if not cli_args.no_llm_perturbations and schema_names:
            generate_llm_perturbations(
                db_paths, schema_names,
                llm_model=cli_args.perturbation_model,
                verbose=cli_args.verbose,
            )

    if not schema_names:
        print("No schemas available. Exiting.")
        return

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Phase 3 — Flatten all tasks
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    all_tasks = flatten_all_tasks(DATASET_DIR, schema_names)
    print(f"\n📋 Flattened task list: {len(all_tasks):,} total prompts")

    schema_counts = Counter(t['schema_name'] for t in all_tasks)
    source_counts = Counter(t['perturbation_source'] for t in all_tasks)
    for s, c in sorted(schema_counts.items()):
        print(f"  {s}: {c:,}")
    print(f"  Sources: {dict(sorted(source_counts.items()))}")

    if not all_tasks:
        print("No tasks loaded. Exiting.")
        return

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Save the flattened task list to the run directory
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    flat_tasks_path = f'{inputs_dir}/flat_tasks.jsonl'
    with open(flat_tasks_path, 'w') as f:
        for t in all_tasks:
            f.write(json.dumps(t) + '\n')
    print(f"\n   📄 Flat tasks saved: {flat_tasks_path}  ({len(all_tasks):,} tasks)")

    if cli_args.generation_only:
        print(f"\n✓ Generation-only mode — {len(all_tasks):,} tasks ready.")
        return

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Phase 4 — Experiment execution
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    import torch
    from src.harness.llm_worker import LLMWorker, _build_system_prompt, _build_schema_context
    from src.harness.experiment_runner import ExperimentRunner

    storage_mgr = StorageManager()

    print(f"\n🚀 Starting Experiment Run: {run_timestamp}")
    print(f"   📂 Output Dir: {outputs_dir}")
    print(f"   💾 Free Disk Space: {storage_mgr.get_free_space_gb():.1f}GB")

    # Load experiment config
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)

    # Determine models to evaluate
    if cli_args.models is not None:
        models_to_run = cli_args.models
    else:
        models_to_run = [
            # 'gemini-2.5-flash-lite',
            # # 'gpt-4o',
            # # 'claude-4.5',
            'local-qwen3-coder-30b-a3b',
            'gpt-oss-20b',
            'llama-3-sqlcoder-8b',
            'sqlcoder-34b-alpha',
            'llama3.1-8b',
            'deepseek-coder-v2-lite',
            # 'gpt-oss-120b'
        ]

    print(f"   🎯 Target Models: {models_to_run}")

    # Pre-load schema contexts for all databases
    schema_contexts = {}
    for p in db_paths:
        cfg = load_schema(str(p))
        if cfg.schema_name in schema_names:
            schema_contexts[cfg.schema_name] = {
                'schema': cfg.get_legacy_schema(),
                'foreign_keys': cfg.get_fk_pairs(),
                'dialect': cfg.dialect,
            }

    # ── Execution loop ───────────────────────────────────────────────
    for model_name in models_to_run:
        print(f"\n{'=' * 60}")
        print(f"🚀 STARTING EVALUATION: {model_name}")
        print('=' * 60)

        results_path = f'{outputs_dir}/results_{model_name}_{run_timestamp}.jsonl'
        runner = ExperimentRunner(results_path)

        model_config = next(
            (m for m in config['models'] if m['name'] == model_name), None
        )
        if not model_config:
            print(f"  ⚠ SKIPPING {model_name}: config not found in experiments.yaml")
            continue

        # Storage check for local models
        if model_config.get('adapter_type') == 'vllm':
            model_id = model_config.get('model_identifier', '')
            if not storage_mgr.ensure_capacity(model_id, min_free_gb=20.0):
                print(f"  ✗ SKIPPING {model_name}: insufficient disk space")
                continue

        try:
            # Create the worker once per model (first schema's context)
            first_sname = schema_names[0]
            first_ctx = schema_contexts[first_sname]

            worker_args = model_config.copy()
            worker_args.pop('name', None)

            worker = LLMWorker(
                adapter_type=worker_args.pop('adapter_type'),
                model_identifier=worker_args.pop('model_identifier'),
                rate_limit=worker_args.pop('rate_limit', None),
                schema=first_ctx['schema'],
                foreign_keys=first_ctx['foreign_keys'],
                dialect=first_ctx['dialect'],
                **worker_args,
            )

            # Run tasks per schema, updating the prompt context each time
            # (avoids reloading the model for each schema)
            for sname in schema_names:
                sctx = schema_contexts[sname]
                worker._system_prompt = _build_system_prompt(sctx['dialect'])
                worker._schema_context = _build_schema_context(
                    sctx['schema'], sctx['foreign_keys']
                )

                schema_tasks = [t for t in all_tasks if t['schema_name'] == sname]
                pending = runner.get_pending_tasks(schema_tasks, worker.model_name)
                if not pending:
                    continue

                print(f"  ▸ Schema: {sname}  ({len(pending):,} pending tasks)")
                runner.run(pending, worker)

            del worker
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                print("  🧹 GPU memory cleared")

        except Exception as e:
            print(f"  ✗ ERROR running {model_name}: {e}")

    print(f"\n✅ Run complete! Results saved in {outputs_dir}")

if __name__ == "__main__":
    main()
