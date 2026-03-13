#!/usr/bin/env bash
set -e

# Run from project root
cd "$(dirname "$0")"

# 1) Generation + model run (skip step 04 llm perturbations)
python3 run_experiments.py \
  --db-dir dataset/dbs_to_test \
  --num-per-complexity 50 \
  --no-llm-perturbations

# 2) Get latest run directory
RUN_DIR=$(ls -dt experiment_workspace/runs/* | head -n 1)
OUT_DIR="$RUN_DIR/outputs"

# 3) Equivalence evaluation
python3 evaluate_live.py "$OUT_DIR" \
  --db-dir dataset/dbs_to_test \
  --parallel --workers 4

# 4) Tabulate results
python3 tabulate_results.py \
  --results "$OUT_DIR/evaluated_results_aggregated.jsonl"

# 5) Generate plots
python3 plot_results.py \
  --table2 "$OUT_DIR/table2_accuracy_results.csv"

echo "Done. Outputs: $OUT_DIR"

# 6) Power down the machine
sudo poweroff
