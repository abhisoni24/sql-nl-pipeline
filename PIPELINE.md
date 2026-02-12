# SQL-NL Pipeline Documentation

This document outlines the active pipeline for the SQL -> NL -> SQL\* experiment.

## 1. Data Generation Phase

These scripts generate the training/evaluation data.

- `01_generate_sql_dataset.py`: **SQL Generator**. Creates synthetic SQL queries based on the schema.
  - Output: `dataset/current/raw_social_media_queries_20.json`
- `02_generate_nl_prompts.py`: **NL Generator**. Converts SQL to "vanilla" Natural Language prompts.
  - Output: `dataset/current/nl_social_media_queries_20.json`
- `03_generate_systematic_perturbations.py`: **Systematic Perturbator**. Applies rule-based NL variations (typos, synonyms).
  - Output: `dataset/current/nl_social_media_queries_systematic_20.json`
- `04_generate_llm_perturbations_cached.py`: **LLM Perturbator**. Uses Gemini to generate diverse/complex NL variations.
  - Output: `dataset/current/nl_social_media_queries_llm_perturbed_20.json`

## 2. Experiment Execution Phase

These scripts run the models to generate SQL from NL prompts.

- `run_experiments.py`: **Main Orchestrator**. Reads the datasets, loads models (via `src/harness`), and generates SQL.
  - Config: `experiments.yaml`
  - Output: `sample_exp_run/{run_id}/output/results_*.jsonl`
- `run_experiments_orchestrator.ipynb`: **Notebook Interface**. High-level notebook for running experiments on Colab/Local.

## 3. Analysis Phase

These scripts evaluate the generated SQL and produce reports.

- `analyze_results.py`: **Evaluator**. Runs the `SQLEquivalenceEngine` to compare Generated SQL vs Gold SQL.
  - Output: `evaluated_results_aggregated.jsonl`
- `generate_detailed_plots.py`: **Reporter**. Generates plots and the detailed markdown report.
  - Output: `detailed_report.md`, `analysis.md`, `*.png` plots.

## 4. Test Suite (Equivalence Engine)

Tools for validating the equivalence logic itself.

- `run_equivalence_test.py`: Runs a test suite on the equivalence engine.
- `generate_sql_equivalence_pairs.py`: Generates test pairs for the engine.
- `test_dbs/`: Directory containing SQLite databases for testing.

## 5. Core Libraries (`src/`)

- `src/core/`: Generator, Schema, NL Renderer.
- `src/harness/`: MP/THREADing harness for LLM inference (Adapters, Workers).
- `src/equivalence/`: The SQL Equivalence Engine logic.
- `src/utils/`: Helper functions (SQL extraction, etc.).

## 6. Configuration

- `experiments.yaml`: Defines active models, rate limits, and parameters.
- `cached_info.py`: System prompt context for LLM perturbations.

## 7. Regression Testing

Use this script to verify pipeline integrity after any code changes.

- `verify_pipeline.py`: Runs a subset of all generation steps and the equivalence test suite to ensure no regressions.
  - Usage: `python3 verify_pipeline.py`
