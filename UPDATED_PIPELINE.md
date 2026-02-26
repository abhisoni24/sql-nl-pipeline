# Updated SQL-NL Pipeline Report

This document reflects the comprehensive latest state of the SQL → NL → SQL\* pipeline: a **schema-agnostic** framework supporting any relational schema via YAML definitions and auto-generated linguistic dictionaries.

---

## Architecture Overview

The pipeline is fully driven by two YAML files per schema:

| File | Purpose | Example |
|------|---------|---------|
| `schemas/<name>.yaml` | Tables, columns, types, PKs, foreign keys, dialect | `schemas/social_media.yaml` |
| `schemas/<name>_dictionary.yaml` | Table & column synonym dictionaries | `schemas/social_media_dictionary.yaml` |

Dictionaries are auto-generated via `generate_dictionary.py` (WordNet-based) and can be manually curated.

Validated schemas: `social_media`, `bank`, `hospital`, `university_system`.

---

## 1. Data Generation Phase

### 1.1 SQL Generation (`01_generate_sql_dataset.py`)

```bash
python 01_generate_sql_dataset.py --schema schemas/<name>.yaml [-n NUM_PER_COMPLEXITY] [-o OUTPUT]
```

- **Process:** Reads the YAML schema, derives type sets (numeric/text/date/boolean) and composite primary keys, then generates valid DQL and DML SQL statements across 7 complexity types (simple, join, advanced, union, insert, update, delete).
- **Key features:**
  - Schema-aware type classification: `real`, `float`, `decimal` → numeric; prevents invalid placeholder values.
  - Composite PK inference: tables without an `id` column whose FK columns form the PK are automatically excluded from UPDATE SET clauses.
  - DELETE statements always include a WHERE clause (retry logic).
- **Core Engine:** `src.core.generator.SQLQueryGenerator`
- **Output:** `dataset/<name>/raw_queries.json` (metadata-wrapped JSON)

### 1.2 Natural Language Prompts (`02_generate_nl_prompts.py`)

```bash
python 02_generate_nl_prompts.py --schema schemas/<name>.yaml [-i INPUT] [-o OUTPUT]
```

- **Process:** Parses raw SQL (via `sqlglot`) and translates into baseline natural language using the `SQLToNLRenderer` with zero active perturbations. Uses the schema's table categories for natural phrasing.
- **Core Engine:** `src.core.nl_renderer` (Syntax-Directed Translation Framework)
- **Output:** `dataset/<name>/nl_prompts.json`

### 1.3 Systematic Perturbations (`03_generate_systematic_perturbations.py`)

```bash
python 03_generate_systematic_perturbations.py --schema schemas/<name>.yaml [-i INPUT] [-o OUTPUT]
```

- **Process:** Iterates through baseline NL prompts and applies 13 registered perturbation strategies. Each strategy:
  1. Checks `is_applicable()` against the SQL AST
  2. If applicable, calls `apply()` to generate the perturbed NL
  3. Calls `was_applied()` to verify the perturbation actually took effect
  4. Stores `was_applied` and `was_applied_reason` metadata per perturbation
- **Strategies (13):** anchored_pronoun_references, comment_annotations, incomplete_join_spec, mixed_sql_nl, omit_obvious_operation_markers, operator_aggregate_variation, phrasal_and_idiomatic_action_substitution, punctuation_variation, table_column_synonyms, temporal_expression_variation, typos, urgency_qualifiers, verbosity_variation
- **Core Engine:** `src.perturbations/` (strategy pattern with `PerturbationStrategy` ABC)
- **Output:** `dataset/<name>/systematic_perturbations.json`

### 1.4 LLM Perturbations (`04_generate_llm_perturbations_cached.py`)

```bash
python 04_generate_llm_perturbations_cached.py
```

- **Process:** Uses the Gemini API to generate realistic LLM perturbations guided by definitions in `cached_info.py`. Produces 14 single-perturbation variants and 1 compound-perturbation variant per query. Uses context caching and local file tracking for resumption.
- **Output:** `dataset/<name>/llm_perturbations.json`

---

## 2. Generation Validation (`pipeline_tests/generation_process/`)

Dedicated test suites validate each generation phase. All accept `--schema` and `--dictionary` flags for schema-agnostic validation.

### 2.1 SQL Generation Tests

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py \
  --input dataset/<name>/raw_queries.json \
  --schema schemas/<name>.yaml [-v]
```

- Validates structural integrity, complexity-specific constraints, schema compliance, type-correct INSERT values, composite PK exclusion in UPDATEs, and WHERE clause presence in DELETEs.

### 2.2 NL Prompt Tests

```bash
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py \
  --input dataset/<name>/nl_prompts.json \
  --schema schemas/<name>.yaml \
  --dictionary schemas/<name>_dictionary.yaml [-v]
```

- Checks that tables/columns appear in the NL (using dictionary synonyms), no raw SQL keywords leak, filter/order/limit clauses are preserved, and DML intent verbs are correct.

### 2.3 Perturbation Tests

```bash
python pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py \
  --input dataset/<name>/systematic_perturbations.json \
  --schema schemas/<name>.yaml \
  --dictionary schemas/<name>_dictionary.yaml [-v]
```

- Validates each perturbation strategy's contract (edit distances, synonym fidelity, SQL preservation, `was_applied` consistency, etc.) across 13 individual test modules.

---

## 3. Experiment Execution Phase

### Experiment Orchestrator (`run_experiments.py`)

- **Process:**
  1. Loads and merges the three datasets: baseline vanilla, systematic perturbations, and LLM perturbations into a single task pool.
  2. Reads configurations from `experiments.yaml` for active models (e.g., `gemini`, `local-qwen3-coder`, `llama-3-sqlcoder`, `deepseek-coder`).
  3. Uses a multiprocessing/threading harness (`LLMWorker` and `ExperimentRunner`) from `src/harness` to dispatch prompts to LLMs and record the generated SQL output.
- **Output Directory:** `sample_exp_run/{timestamp}/outputs/results_{model_name}_{timestamp}.jsonl`

---

## 4. Evaluation and Analysis Phase

### Analysis & Reporting (`analyze_results.py`)

- **Process:**
  1. **Aggregation:** Combines output records from all tested models.
  2. **Equivalence Checking:** Streams records to the `SQLEquivalenceEngine`.
     - For **DQL (SELECT)**: Generates a suite of randomly seeded temporary SQLite databases. Both Gold SQL and Generated SQL are executed against these databases to compare table denotations. If they match on all generated DBs, they are declared equivalent.
     - For **DML (INSERT, UPDATE, DELETE)**: Uses a "State Delta" approach. Creates identical twin databases; Gold SQL executes on A, Generated SQL on B. Full post-execution table state is compared across all fuzzed databases.
     - Utilizes multiprocessing (isolated local test DBs per worker) and query caching for performance.
  3. **Visual Analytics:** Renders analytical charts — Accuracy by Model, Accuracy Drop from Baseline, Faceted Heatmaps (Complexity × Perturbation Type).
- **Key Modules:** `src.equivalence.equivalence_engine`
- **Output Files:** `evaluated_results_aggregated.jsonl`, visual plots

---

## 5. Dataset Format

All pipeline outputs use a metadata-wrapped JSON envelope:

```json
{
  "metadata": {
    "schema_name": "university_system",
    "dialect": "sqlite",
    "schema_source": "schemas/university_system.yaml",
    "generated_at": "2025-...",
    "num_records": 140,
    "pipeline_step": "01_generate_sql_dataset"
  },
  "records": [ ... ]
}
```

---

## 6. Adding a New Schema

1. Create `schemas/<name>.yaml` with tables, columns (with types and `is_pk`), and foreign keys.
2. Generate dictionary: `python generate_dictionary.py --schema schemas/<name>.yaml --outdir schemas/`
3. Run the pipeline: steps 01 → 02 → 03
4. Validate: run all three test suites with `--schema` and `--dictionary` flags.

---

## Validation Results (Phase 9)

| Schema | SQL Checks | NL Checks | Perturbation Checks | Total | Failures |
|--------|-----------|-----------|---------------------|-------|----------|
| social_media | 2,392 | 2,072 | 16,576 | 21,040 | 0 |
| university_system | 2,467 | 2,140 | 15,846 | 20,453 | 0 |
| **Total** | **4,859** | **4,212** | **32,422** | **41,493** | **0** |
