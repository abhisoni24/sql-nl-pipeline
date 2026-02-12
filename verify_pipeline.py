#!/usr/bin/env python3
"""
Pipeline Verification / Regression Test Script.

This script executes the core components of the SQL->NL->Evaluate pipeline to ensure
functionality remains intact after code changes. It performs:
1. Syntax/Import checks on critical scripts.
2. End-to-end execution of the Data Generation pipeline (steps 01-04).
3. Execution of the Equivalence Test Suite.

Usage:
    python3 verify_pipeline.py
"""

import subprocess
import os
import sys
from typing import List, Tuple

def run_command(desc: str, cmd: str) -> bool:
    print(f"\n[VERIFY] {desc}...")
    try:
        # Run command and capture output
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        print("✅ Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed (Exit Code: {e.returncode})")
        print(f"STDOUT:\n{e.stdout}")
        print(f"STDERR:\n{e.stderr}")
        return False

def check_syntax(script_path: str) -> bool:
    try:
        with open(script_path, 'r') as f:
            compile(f.read(), script_path, 'exec')
        return True
    except Exception as e:
        print(f"❌ Syntax Error in {script_path}: {e}")
        return False

def main():
    print("🚀 Starting Pipeline Verification / Regression Test...")
    print(f"CWD: {os.getcwd()}")
    
    # 1. Check imports/syntax first (fast fail)
    print("\n[VERIFY] Checking Syntax for Core Scripts...")
    scripts_to_check = [
        "run_experiments.py", 
        "analyze_results.py",
        "generate_detailed_plots.py",
        "run_equivalence_test.py",
        "01_generate_sql_dataset.py",
        "02_generate_nl_prompts.py",
        "03_generate_systematic_perturbations.py",
        "04_generate_llm_perturbations_cached.py"
    ]
    
    failed_checks = 0
    for script in scripts_to_check:
        if not os.path.exists(script):
            print(f"❌ Missing File: {script}")
            failed_checks += 1
            continue
            
        if check_syntax(script):
            print(f"✅ Syntax OK: {script}")
        else:
            failed_checks += 1

    if failed_checks > 0:
        print(f"❌ Aborting: {failed_checks} critical scripts missing or broken.")
        sys.exit(1)

    # 2. Run Data Generation Pipeline (subset)
    # We rely on the fact that 01 generates a small file (_20.json) by default
    generation_steps: List[Tuple[str, str]] = [
        ("Step 1: SQL Generation", "python3 01_generate_sql_dataset.py"),
        ("Step 2: NL Prompt Generation", "python3 02_generate_nl_prompts.py"),
        ("Step 3: Systematic Perturbations", "python3 03_generate_systematic_perturbations.py"),
        ("Step 4: LLM Perturbations (Mock)", "python3 04_generate_llm_perturbations_cached.py --mock --limit 5")
    ]

    for desc, cmd in generation_steps:
        if not run_command(desc, cmd):
            print("❌ Pipeline Verification Failed at Generation Stage.")
            sys.exit(1)

    # 3. Equivalence Suite Check
    equiv_steps: List[Tuple[str, str]] = [
        ("Step 5: Generate Test Pairs", "python3 generate_sql_equivalence_pairs.py"),
        ("Step 6: Run Equivalence Tests", "python3 run_equivalence_test.py --input dataset/current/sql_equivalence_pairs.json --output dataset/current/test_results.json")
    ]
    
    for desc, cmd in equiv_steps:
        if not run_command(desc, cmd):
            # Note: run_equivalence_test.py returns non-zero if accuracy < 100%
            # We handle this leniently for now, but report failure if the script crashes
            print("⚠️ Equivalence tests completed with < 100% accuracy (Check logs).")
            # If we wantstrict fail on crash vs just failure on accuracy, we'd need to parse stderr.
            # Assuming for regression testing, any non-zero exit code is worth investigating.
            print("❌ Pipeline Verification Failed at Equivalence Stage.")
            sys.exit(1)

    print("\n🎉 ALL CHECKS PASSED: Pipeline is regression-tested and fully operational.")

if __name__ == "__main__":
    main()
