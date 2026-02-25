# Updated SQL-NL Pipeline Report

This document reflects the comprehensive latest state of the SQL -> NL -> SQL\* pipeline, incorporating modern systematic and LLM-based perturbations, experiment execution, and robust equivalence-based evaluation.

## 1. Data Generation Phase

The dataset preparation process is broken down into four distinct steps, producing baseline queries and their systematically/LLM-perturbed counter-parts.

### 1.1 SQL Generation (`01_generate_sql_dataset.py`)

- **Process:** Utilizes the `SQLQueryGenerator` along with the defined schema (`SCHEMA`, `FOREIGN_KEYS`) to synthetically generate valid DQL (SELECT) and DML (INSERT, UPDATE, DELETE) SQL statements. It generates a specified number of queries per complexity level (e.g., 50 per complexity).
- **Core Engine:** `src.core.generator`
- **Output:** `dataset/current/raw_social_media_queries_20.json`

### 1.2 Natural Language Prompts (`02_generate_nl_prompts.py`)

- **Process:** Parses the raw SQL dataset (using `sqlglot`) and translates them into "vanilla" natural language requests using the `SQLToNLRenderer` setup with zero active perturbations. This provides a clean baseline of SQL-to-NL translations.
- **Core Engine:** `src.core.nl_renderer` (Syntax-Directed Translation Framework)
- **Output:** `dataset/current/nl_social_media_queries_20.json`

### 1.3 Systematic Perturbations (`03_generate_systematic_perturbations.py`)

- **Process:** Iterates through the vanilla NL prompts and checks which of the 14 defined perturbation rules apply (e.g., typos, synonym substitution, operator variation, ambiguous pronouns). It evaluates the AST to determine applicability and deterministically generates rule-based "single perturbations" for each query using the SDT template engine.
- **Core Engine:** `src.core.nl_renderer` (evaluates `PerturbationType` constraints).
- **Output:** `dataset/current/nl_social_media_queries_systematic_20.json`

### 1.4 LLM Perturbations (`04_generate_llm_perturbations_cached.py`)

- **Process:** Uses the Gemini `gemini-2.5-flash-lite` API to generate realistic LLM perturbations loosely guided by the definitions in `cached_info.py`. For each query, it produces 14 single-perturbation variants and 1 compound-perturbation variant. Uses context caching and local file tracking to resume interrupted runs and respect rate limits (RPM-tracking).
- **Output:** `dataset/current/nl_social_media_queries_llm_perturbed_20.json`

### 1.5 Generation Validation (`pipeline_tests/generation_process/`)

- **Process:** Dedicated test suites rigorously evaluate the outputs of the generation phases.
  - **SQL Generation Tests (`sql/test_sql_generation.py`):** Validates structural integrity, complexity-specific constraints (e.g., verifying JOIN uses correct foreign keys, ensuring UNION legs match in column count), and schema compliance.
  - **NL Prompt Tests (`nl_prompt/test_nl_prompt.py`):** Checks baseline prompt fidelity, ensuring no SQL keywords leak, columns and tables are accurately referenced matching the AST, and filters/order/limit clauses are semantically preserved.
  - **Perturbation Tests (`systematic_perturbations/`):** Validates that each deterministic perturbation adheres to its specific contract (e.g., `test_typos.py` enforces character edit distances, ensures object/number preservation, and verifies the prompt remains readable while introducing realistic errors).

---

## 2. Experiment Execution Phase

The execution stage runs the generated NL datasets through various language models to evaluate their SQL-generation capabilites.

### Experiment Orchestrator (`run_experiments.py`)

- **Process:**
  1. Loads and merges the three datasets: baseline vanilla, systematic perturbations, and LLM perturbations into a single task pool.
  2. Reads configurations from `experiments.yaml` for active models (e.g., `gemini`, `local-qwen3-coder`, `llama-3-sqlcoder`, `deepseek-coder`).
  3. Uses a multiprocessing/threading harness (`LLMWorker` and `ExperimentRunner`) from `src/harness` to dispatch prompts to LLMs and record the generated SQL output.
- **Output Directory:** `sample_exp_run/{timestamp}/outputs/results_{model_name}_{timestamp}.jsonl`

---

## 3. Evaluation and Analysis Phase

Unlike direct string comparison, this pipeline dynamically evaluates whether the model-generated SQL actually produces identical behavior as the gold SQL.

### Analysis & Reporting (`analyze_results.py`)

- **Process:**
  1. **Aggregation:** Combines output records from all tested models.
  2. **Equivalence Checking:** Streams records to the `SQLEquivalenceEngine`.
     - For **DQL (SELECT)**: Generates a suite of randomly seeded temporary SQLite databases (via `dql_checker`). Both the Gold SQL and the Generated SQL are executed against these databases to compare table denotations/results. If they match on all generated DBs, they are declared equivalents.
     - For **DML (INSERT, UPDATE, DELETE)**: Utilizes a "State Delta" approach (via `dml_checker`). It creates identical twin databases (Trial A and Trial B). Gold SQL executes on A, Generated SQL on B. The full post-execution state of affected tables is compared. Statements are equivalent iff the table states match perfectly across all fuzzed databases.
     - Utilizes multiprocessing (isolated local test DBs per worker) and query caching to immensely speed up identical gold_sql verification.
  3. **Visual Analytics:** Once evaluations append to `evaluated_results_aggregated.jsonl`, the script uses seaborn/matplotlib to render extensive analytical charts, such as Accuracy by Model, Accuracy Drop from Baseline, and Faceted Heatmaps covering Complexity × Perturbation Type.
- **Key Modules:** `src.equivalence.equivalence_engine`
- **Output Files:**
  - `evaluated_results_aggregated.jsonl`
  - Visual Plots: `accuracy_by_model.png`, `heatmap_complexity_x_perttype.png`, `accuracy_delta_from_baseline.png`, etc.
