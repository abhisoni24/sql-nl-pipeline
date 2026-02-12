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
from src.utils.data_loader import (
    load_baseline_queries,
    load_systematic_perturbations,
    load_llm_perturbations
)

# Configuration Defaults (Can be overridden by env vars)
LOCAL_BASE = os.getenv('LOCAL_BASE', '/content/experiment_workspace')
REPO_PATH = os.getenv('REPO_PATH', f'{LOCAL_BASE}/sql-nl')
DRIVE_BASE = os.getenv('DRIVE_BASE', '/content/drive/MyDrive/ExpResults')
CONFIG_PATH = f'{REPO_PATH}/experiments.yaml'
DATASET_DIR = f'{REPO_PATH}/dataset/current'

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
    baseline = load_baseline_queries(f'{dataset_dir}/nl_social_media_queries_20.json')
    systematic = load_systematic_perturbations(f'{dataset_dir}/nl_social_media_queries_systematic_20.json')
    llm = load_llm_perturbations(f'{dataset_dir}/nl_social_media_queries_llm_perturbed_20.json')
    
    all_tasks = baseline + systematic + llm
    random.shuffle(all_tasks)
    
    print(f"✅ Loaded {len(all_tasks):,} total tasks")
    return all_tasks

def main():
    # 1. Setup
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_run_dir, inputs_dir, outputs_dir = setup_directories(run_timestamp)
    
    print(f"🚀 Starting Experiment Run: {run_timestamp}")
    print(f"   📂 Output Dir: {outputs_dir}")
    
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
        'gemini-2.5-flash-lite',
        # 'gpt-4o',
        # 'claude-4.5',
        # 'local-qwen3-coder-30b-a3b',
        # 'llama3.1-8b',
        # 'deepseek-coder-v2-lite'
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
