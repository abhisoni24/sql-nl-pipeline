#!/usr/bin/env python3
"""
Central Runner for Systematic Perturbation Test Suite
=====================================================
Discovers and executes all 13 test_*.py scripts in this directory
and aggregates their results into a unified summary report.

Usage:
  python3 run_all_perturbation_tests.py --input <path_to_json>
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

def run_script(script_path, input_file, schema_file=None, dictionary_file=None):
    """Runs a single test script and returns (total, passed, failed, duration)."""
    start_time = time.time()
    cmd = [sys.executable, str(script_path), "--input", input_file]
    if schema_file:
        cmd += ["--schema", schema_file]
    if dictionary_file:
        cmd += ["--dictionary", dictionary_file]
    
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        stdout, _ = process.communicate()
        duration = time.time() - start_time
        
        # Parse output for results
        total, passed, failed = 0, 0, 0
        for line in stdout.splitlines():
            if "Total checks" in line:
                total = int(line.split(":")[-1].strip())
            elif "Passed" in line:
                passed = int(line.split(":")[-1].strip())
            elif "Failed" in line:
                failed = int(line.split(":")[-1].strip())
        
        return total, passed, failed, duration, stdout
    except Exception as e:
        return 0, 0, 0, 0, f"Error running script: {e}"

def main():
    parser = argparse.ArgumentParser(description="Run all systematic perturbation tests.")
    parser.add_argument("--input", "-i", required=True, help="Path to the JSON dataset file to test.")
    parser.add_argument("--schema", "-s", required=True, help="Path to the schema YAML file.")
    parser.add_argument("--dictionary", "-d", required=True, help="Path to the dictionary YAML file.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show full output for each script.")
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"Error: Input file {input_path} not found.")
        sys.exit(1)

    current_dir = Path(__file__).parent
    test_scripts = sorted([f for f in current_dir.glob("test_*.py") if f.name != Path(__file__).name])

    if not test_scripts:
        print("No test scripts found (test_*.py).")
        sys.exit(0)

    print("=" * 100)
    print(f"{'SYSTEMATIC PERTURBATION TEST SUITE':^100}")
    print("=" * 100)
    print(f"Dataset: {input_path}")
    print(f"Found {len(test_scripts)} test scripts.")
    print("-" * 100)
    print(f"{'Perturbation Type':<50} | {'Total':>8} | {'Passed':>8} | {'Failed':>8} | {'Time':>6}")
    print("-" * 100)

    overall_total = 0
    overall_passed = 0
    overall_failed = 0
    overall_start_time = time.time()
    
    results = []

    for script in test_scripts:
        pname = script.stem.replace("test_", "")
        total, passed, failed, duration, output = run_script(script, str(input_path), args.schema, args.dictionary)
        
        results.append((pname, total, passed, failed, duration, output))
        
        overall_total += total
        overall_passed += passed
        overall_failed += failed
        
        status_color = "\033[92m" if failed == 0 else "\033[91m"
        reset_color = "\033[0m"
        
        print(f"{pname:<50} | {total:>8} | {passed:>8} | {status_color}{failed:>8}{reset_color} | {duration:>5.1f}s")

    overall_duration = time.time() - overall_start_time
    print("-" * 100)
    
    final_color = "\033[92m" if overall_failed == 0 else "\033[91m"
    print(f"{'OVERALL TOTAL':<50} | {overall_total:>8} | {overall_passed:>8} | {final_color}{overall_failed:>8}{reset_color} | {overall_duration:>5.1f}s")
    print("=" * 100)

    if args.verbose:
        for pname, total, passed, failed, duration, output in results:
            print(f"\n\n{'='*20} {pname.upper()} FULL OUTPUT {'='*20}")
            print(output)

    if overall_failed > 0:
        sys.exit(1)
    else:
        print("\nAll tests passed successfully! 🎉")
        sys.exit(0)

if __name__ == "__main__":
    main()
