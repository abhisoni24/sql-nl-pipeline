"""
Main script for running SQL Generation Experiments.
Handles strictly the GENERATION phase (LLM -> SQL).
"""
import os
import sys
import yaml
import gc
import json
import torch
import random
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.harness.llm_worker import LLMWorker
from src.harness.experiment_runner import ExperimentRunner
from src.core.schema_loader import load_from_yaml
from src.utils.data_loader import (
    load_baseline_queries,
    load_systematic_perturbations,
    load_llm_perturbations
)

from src.utils.storage_manager import StorageManager

# Configuration Defaults (Can be overridden by env vars)
IS_COLAB = os.path.exists('/content')

DEFAULT_LOCAL_BASE = '/content/experiment_workspace' if IS_COLAB else './experiment_workspace'
DEFAULT_REPO_PATH = f'{DEFAULT_LOCAL_BASE}/sql-nl' if IS_COLAB else '.'
DEFAULT_DRIVE_BASE = '/content/drive/MyDrive/ExpResults' if IS_COLAB else './drive_backup'

LOCAL_BASE = os.getenv('LOCAL_BASE', DEFAULT_LOCAL_BASE)
REPO_PATH = os.getenv('REPO_PATH', DEFAULT_REPO_PATH)
DRIVE_BASE = os.getenv('DRIVE_BASE', DEFAULT_DRIVE_BASE)
CONFIG_PATH = os.path.join(REPO_PATH, 'experiments.yaml')
DATASET_DIR = os.path.join(REPO_PATH, 'dataset')

def setup_directories(run_timestamp: str):
    """Create local and drive directories for this run."""
    # Local dirs
    local_run_dir = f'{LOCAL_BASE}/runs/{run_timestamp}'
    inputs_dir = f'{local_run_dir}/inputs'
    outputs_dir = f'{local_run_dir}/outputs'
    logs_dir = f'{local_run_dir}/logs'
    
    for d in [inputs_dir, outputs_dir, logs_dir]:
        os.makedirs(d, exist_ok=True)
        
    return local_run_dir, inputs_dir, outputs_dir

def load_all_tasks(dataset_dir: str):
    """Load and merge all task types."""
    print("⏳ Loading datasets...")
    baseline = load_baseline_queries(f'{dataset_dir}/social_media/nl_prompts.json')
    systematic = load_systematic_perturbations(f'{dataset_dir}/social_media/systematic_perturbations.json')
    llm = load_llm_perturbations(f'{dataset_dir}/social_media/llm_perturbations.json')
    
    all_tasks = baseline + systematic + llm
    random.shuffle(all_tasks)
    
    print(f"✅ Loaded {len(all_tasks):,} total tasks")
    return all_tasks

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run SQL generation experiments.")
    parser.add_argument('--schema', default='schemas/social_media.yaml',
                        help='Path to schema YAML (default: schemas/social_media.yaml)')
    cli_args = parser.parse_args()

    # Load schema config
    schema_cfg = load_from_yaml(cli_args.schema)
    schema = schema_cfg.get_legacy_schema()
    foreign_keys = schema_cfg.get_fk_pairs()
    dialect = schema_cfg.dialect
    schema_name = schema_cfg.schema_name

    # 1. Setup
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_run_dir, inputs_dir, outputs_dir = setup_directories(run_timestamp)
    
    # Initialize Storage Manager
    storage_mgr = StorageManager()
    
    print(f"🚀 Starting Experiment Run: {run_timestamp} (schema: {schema_name})")
    print(f"   📂 Output Dir: {outputs_dir}")
    print(f"   💾 Free Disk Space: {storage_mgr.get_free_space_gb():.1f}GB")
    
    # 2. Load Config
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
        
    # 3. Load Data
    tasks = load_all_tasks(DATASET_DIR)
    
    # Save flat tasks for reference
    with open(f'{inputs_dir}/flat_tasks.jsonl', 'w') as f:
        for t in tasks:
            f.write(json.dumps(t) + '\n')
            
    # 4. Determine Models to Run
    # Default list, can be modified via script args in future
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
    
    print(f"🎯 Targeted Models: {models_to_run}")
    
    # 5. Execution Loop
    for model_name in models_to_run:
        print(f"\n" + "="*60)
        print(f"🚀 STARTING GENERATION FOR: {model_name}")
        print("="*60)
        
        # Determine unique output file for this model
        # Format: results_{model}_{timestamp}.jsonl
        results_path = f'{outputs_dir}/results_{model_name}_{run_timestamp}.jsonl'
        runner = ExperimentRunner(results_path)
        
        # Find config
        model_config = next((m for m in config['models'] if m['name'] == model_name), None)
        if not model_config:
            print(f"⚠️ SKIPPING {model_name}: Config not found")
            continue
            
        # Storage Check: Ensure space exists BEFORE potentially downloading
        if model_config.get('adapter_type') == 'vllm':
            model_id = model_config.get('model_identifier', '')
            if not storage_mgr.ensure_capacity(model_id, min_free_gb=20.0):
                print(f"❌ SKIPPING {model_name}: Insufficient disk space and pruning failed.")
                continue

        try:
            # Init Worker
            # Pass all model config items as kwargs (excludes keys handled explicitly if we want, but **model_config is easiest)
            # We filter out keys that map to explicit args to avoid dupes or handle them cleanly
            worker_args = model_config.copy()
            worker_args.pop('name', None) # Remove internal name
            
            worker = LLMWorker(
                adapter_type=worker_args.pop('adapter_type'),
                model_identifier=worker_args.pop('model_identifier'),
                rate_limit=worker_args.pop('rate_limit', None),
                schema=schema,
                foreign_keys=foreign_keys,
                dialect=dialect,
                **worker_args # Pass remaining config (max_model_len, quantization, etc.)
            )
            
            # Run
            runner.run(tasks, worker)
            
            # Cleanup
            del worker
            gc.collect()
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                print("🧹 GPU memory cleared")
                
        except Exception as e:
            print(f"❌ ERROR running {model_name}: {str(e)}")

    print(f"\n✅ Run Complete! Results saved in {outputs_dir}")
    # Note: Orchestrator will handle the backup to Drive

if __name__ == "__main__":
    main()
