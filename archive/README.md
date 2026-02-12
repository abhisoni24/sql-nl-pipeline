# Archived Scripts

This directory contains scripts that were identified as obsolete or not directly used in the active `SQL -> NL -> SQL*` pipeline as of Feb 2025.

## Files

- `05_run_experiment.py`: Old CLI entry point. Replaced by `run_experiments.py`.
- `llm_sql_experiment.py`: Monolithic legacy script containing generation/execution/eval in one file. Replaced by modular pipeline.
- `old_run_experiments.ipynb`: Old notebook.
- `normalization.py`: Unused normalization logic. The active pipeline uses `src/utils/sql_utils.py` and `src/equivalence/`.
- `fix_eval.py`: Temporary script for fixing a specific evaluation run.
- `reproduce_extraction.py`: One-off reproduction script.
- `temp.py`: Temporary scratch file.
- `inspect_errors.py`: Custom debug script for inspecting errors. Valid but not core pipeline.
- `extract_report_examples.py`: Helper script used to generate examples for a specific report.
- `verify_pronouns.py`: One-off verification script.

These files are kept for reference but should not be used for new experiments.
