"""
Script for Analyzing Experiment Results.
Handles EVALUATION (SQL Equivalence) and PLOTTING.
Aggregates all results from a given run directory.
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

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.sql_utils import extract_sql
from src.equivalence.equivalence_engine import SQLEquivalenceEngine
from src.equivalence.config import EquivalenceConfig

# Configuration Defaults
LOCAL_BASE = os.getenv('LOCAL_BASE', './')
REPO_PATH = os.getenv('REPO_PATH', f'{LOCAL_BASE}/')

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

def aggregate_results(run_outputs_dir: str) -> pd.DataFrame:
    """Load all results_*.jsonl files from the output directory."""
    all_records = []
    pattern = f'{run_outputs_dir}/results_*.jsonl'
    files = glob.glob(pattern)
    
    if not files:
        print(f"⚠️ No result files found in {run_outputs_dir}")
        return pd.DataFrame()
        
    print(f"📋 Found {len(files)} result files. Aggregating...")
    for fpath in files:
        with open(fpath, 'r') as f:
            for line in f:
                if line.strip():
                    all_records.append(json.loads(line))
                    
    df = pd.DataFrame(all_records)
    print(f"✅ Aggregated {len(df):,} total records.")
    return df

def evaluate_dataframe(df: pd.DataFrame, engine: SQLEquivalenceEngine) -> pd.DataFrame:
    """Run equivalence check on DataFrame."""
    print("running evaluation...")
    evaluated_records = []
    
    # Convert to list of dicts for iteration
    records = df.to_dict('records')
    
    for r in tqdm(records, desc="Evaluating SQL"):
        generated_sql = extract_sql(r.get('generated_response', ''))
        gold_sql = r.get('gold_sql', '')
        
        try:
            eq_result = engine.check_equivalence(gold_sql, generated_sql)
            is_equivalent = eq_result.is_equivalent
            eq_details = eq_result.details
        except Exception as e:
            is_equivalent = False
            eq_details = f"Error: {str(e)}"
            
        r['generated_sql'] = generated_sql
        r['is_equivalent'] = is_equivalent
        r['equivalence_details'] = eq_details
        evaluated_records.append(r)
        
    return pd.DataFrame(evaluated_records)

def generate_plots(df: pd.DataFrame, output_dir: str):
    """Generate and save standard plots."""
    sns.set_theme(style='whitegrid')
    
    # 1. Accuracy by Model
    acc = df.groupby('model_name')['is_equivalent'].mean() * 100
    plt.figure(figsize=(10, 6))
    acc.plot(kind='barh')
    plt.title('Accuracy by Model')
    plt.xlabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_model.png')
    plt.close()
    
    # 2. Accuracy by Source
    acc_src = df.groupby('perturbation_source')['is_equivalent'].mean() * 100
    plt.figure(figsize=(8, 5))
    acc_src.plot(kind='bar')
    plt.title('Accuracy by Source')
    plt.ylabel('Accuracy (%)')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/accuracy_by_source.png')
    plt.close()
    
    print(f"📊 Plots saved to {output_dir}")

def main(run_outputs_dir):
    # 1. Init Engine
    engine = setup_equivalence_engine()
    
    # 2. Aggregate
    df = aggregate_results(run_outputs_dir)
    if df.empty:
        return
        
    # 3. Evaluate
    eval_df = evaluate_dataframe(df, engine)
    
    # 4. Save Evaluated Results (Single aggregated file)
    eval_path = f'{run_outputs_dir}/evaluated_results.jsonl'
    eval_df.to_json(eval_path, orient='records', lines=True)
    print(f"💾 Evaluated results saved to {eval_path}")
    
    # 5. Summary Report
    print("="*40)
    print("SUMMARY")
    print("="*40)
    print(eval_df.groupby('model_name')['is_equivalent'].mean() * 100)
    
    # 6. Plot
    generate_plots(eval_df, run_outputs_dir)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_results.py <outputs_dir>")
        sys.exit(1)
        
    outputs_dir = sys.argv[1]
    main(outputs_dir)
