# SQL-NL Pipeline

> **Last updated:** March 4, 2026 — after legacy code removal and full refactoring.

A **schema-agnostic** framework for generating SQL queries, rendering them as natural language prompts, applying systematic perturbations, and evaluating LLM-generated SQL via execution-based equivalence checking. Supports any relational schema via YAML definitions or direct SQLite introspection.

---

## Architecture Overview

```
schemas/<name>.yaml ─┐
           OR        ├─► generate_dictionary.py ─► schemas/<name>_dictionary.yaml
<database>.sqlite ───┘
          │
          ▼
  01_generate_sql_dataset.py    ─► dataset/<name>/raw_queries.json
          │
          ▼
  02_generate_nl_prompts.py     ─► dataset/<name>/nl_prompts.json
          │
          ▼
  03_generate_systematic_perturbations.py ─► dataset/<name>/systematic_perturbations.json
          │
          ▼
  04_generate_llm_nl_and_perturbations.py ─► dataset/<name>/llm_perturbations.json
          │
          ▼
  run_experiments.py            ─► LLM-generated SQL outputs
          │
          ▼
  analyze_results.py            ─► Equivalence verdicts + visual analytics
```

### Schema Input

The pipeline accepts schemas in two forms — **both are first-class citizens**:

| Input | How | Example |
|-------|-----|---------|
| YAML definition | `--schema schemas/<name>.yaml` | `schemas/social_media.yaml` |
| SQLite database | `--schema path/to/database.sqlite` | `european_football_1.sqlite` |

SQLite schemas are introspected automatically via `src/core/schema_loader.py` (`load_from_sqlite()`), which reads `CREATE TABLE` DDL, infers column types, detects foreign keys from `PRAGMA foreign_key_list`, and builds the same `SchemaConfig` object as the YAML loader.

Dictionaries (table/column synonym mappings for NL rendering) are auto-generated via `generate_dictionary.py` using WordNet expansion:

```bash
python generate_dictionary.py --schema schemas/<name>.yaml --outdir schemas/
# or from SQLite:
python generate_dictionary.py --schema path/to/database.sqlite --outdir schemas/
```

### Core Modules

| Module | Purpose |
|--------|---------|
| `src/core/schema_config.py` | `SchemaConfig`, `TableDef`, `ColumnDef`, `ForeignKeyDef` dataclasses |
| `src/core/schema_loader.py` | `load_schema()` — dispatches to `load_from_yaml()` or `load_from_sqlite()` by file extension |
| `src/core/generator.py` | `SQLQueryGenerator` — generates SQL ASTs via complexity handlers |
| `src/core/nl_renderer.py` | `SQLToNLRenderer` — syntax-directed translation from SQL AST to NL |
| `src/core/template_resolver.py` | `TemplateResolver` — two-pass IR token resolution using `LinguisticDictionary` |
| `src/core/linguistic_dictionary.py` | `LinguisticDictionary` — schema-specific synonym/category lookups |
| `src/core/dictionary_builder.py` | `build_dictionary()` — WordNet-based synonym expansion |
| `src/complexity/` | 7 `ComplexityHandler` subclasses (simple, join, advanced, union, insert, update, delete) + registry |
| `src/perturbations/` | 13 `PerturbationStrategy` subclasses + registry with auto-discovery |
| `src/equivalence/` | `SQLEquivalenceEngine` — execution-based DQL/DML equivalence checking |
| `src/harness/` | `LLMWorker`, `ExperimentRunner`, adapter layer (Gemini, OpenAI, Anthropic, vLLM) |

---

## 1. Data Generation Phase

All generation scripts require `--schema` (YAML or SQLite path). No legacy fallbacks exist.

### 1.1 SQL Generation (`01_generate_sql_dataset.py`)

```bash
python 01_generate_sql_dataset.py --schema schemas/<name>.yaml [-n NUM] [-o OUTPUT]
python 01_generate_sql_dataset.py --schema path/to/database.sqlite -n 20
```

| Flag | Default | Description |
|------|---------|-------------|
| `--schema`, `-s` | *required* | YAML or SQLite schema file |
| `--num-per-complexity`, `-n` | 50 | Queries per complexity type (7 types → 350 total) |
| `--output`, `-o` | `dataset/<name>/raw_queries.json` | Output path |

- Reads the schema, derives type sets (numeric/text/date/boolean) and composite primary keys, then generates valid SQL statements across 7 complexity types via the `src/complexity/` handler registry.
- **Composite PK inference:** Tables without an `id` column whose FK columns form the PK are automatically excluded from UPDATE SET clauses.
- **DELETE safety:** Retry logic ensures every DELETE includes a WHERE clause.
- **Output:** Metadata-wrapped JSON with `records` array.

### 1.2 Natural Language Prompts (`02_generate_nl_prompts.py`)

```bash
python 02_generate_nl_prompts.py --schema schemas/<name>.yaml --two-pass \
    --dictionary schemas/<name>_dictionary.yaml [-i INPUT] [-o OUTPUT]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--schema`, `-s` | none | YAML or SQLite schema (provides dialect + schema name) |
| `--two-pass` | off | Use IR template → dictionary resolution (recommended) |
| `--dictionary`, `-d` | none | Dictionary YAML for two-pass resolution |
| `--input`, `-i` | `dataset/<name>/raw_queries.json` | Input SQL dataset |
| `--output`, `-o` | `dataset/<name>/nl_prompts.json` | Output path |

- Parses each SQL statement via `sqlglot` and translates it into a baseline natural language prompt using the `SQLToNLRenderer`.
- **Two-pass mode** (recommended): Pass 1 emits IR tokens (`[TABLE:x]`, `[COL:x]`, etc.), Pass 2 resolves them via `TemplateResolver` against the `LinguisticDictionary`.
- **Output:** Each record gains `nl_text` (and optionally `ir_template`) fields.

### 1.3 Systematic Perturbations (`03_generate_systematic_perturbations.py`)

```bash
python 03_generate_systematic_perturbations.py --schema schemas/<name>.yaml [-i INPUT] [-o OUTPUT]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--schema`, `-s` | none | Schema file (provides dialect + FK info) |
| `--input`, `-i` | `dataset/<name>/nl_prompts.json` | Input NL dataset |
| `--output`, `-o` | `dataset/<name>/systematic_perturbations.json` | Output path |

- Applies 13 registered perturbation strategies to each baseline NL prompt. Each strategy:
  1. `is_applicable()` — pre-generation gate (checks SQL AST)
  2. `apply()` — generates the perturbed NL text
  3. `was_applied()` — post-generation validation (confirms the perturbation took effect)
- Results include `was_applied` and `was_applied_reason` metadata per perturbation.

**Registered strategies (13):**
`anchored_pronoun_references`, `comment_annotations`, `incomplete_join_spec`, `mixed_sql_nl`, `omit_obvious_operation_markers`, `operator_aggregate_variation`, `phrasal_and_idiomatic_action_substitution`, `punctuation_variation`, `table_column_synonyms`, `temporal_expression_variation`, `typos`, `urgency_qualifiers`, `verbosity_variation`

### 1.4 LLM Perturbations (`04_generate_llm_nl_and_perturbations.py`)

```bash
python 04_generate_llm_nl_and_perturbations.py --schema schemas/<name>.yaml \
    --model gemini [-i INPUT] [-o OUTPUT]
```

- Uses an LLM (via the `src/harness/adapters/` layer) to generate realistic NL perturbations. Supports any adapter configured in `experiments.yaml` (Gemini, OpenAI, Anthropic, vLLM).
- Uses context caching and local file tracking for resumption.
- **Output:** `dataset/<name>/llm_perturbations.json`

---

## 2. Generation Validation

Dedicated test suites in `pipeline_tests/generation_process/` validate each generation phase. All require `--schema` (YAML or SQLite).

### 2.1 SQL Generation Tests

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py \
    --input dataset/<name>/raw_queries.json \
    --schema schemas/<name>.yaml [-v]
```

Validates: structural integrity, complexity-specific contracts (30+ checks), schema compliance, type-correct INSERT values, composite PK exclusion in UPDATEs, WHERE clause presence in DELETEs.

### 2.2 NL Prompt Tests

```bash
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py \
    --input dataset/<name>/nl_prompts.json \
    --schema schemas/<name>.yaml \
    --dictionary schemas/<name>_dictionary.yaml [-v]
```

Validates: table/column mention coverage (using dictionary synonyms), no raw SQL keyword leakage, filter/order/limit clause preservation, DML intent verb correctness.

### 2.3 Perturbation Tests

```bash
python pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py \
    --input dataset/<name>/systematic_perturbations.json \
    --schema schemas/<name>.yaml \
    --dictionary schemas/<name>_dictionary.yaml [-v]
```

Runs 13 individual test modules (one per strategy), validating each strategy's contract: edit distances, synonym fidelity, SQL preservation, `was_applied` consistency, and more.

### 2.4 Batch Robustness Test

```bash
python test_dbs/spider/robustness_test.py
```

End-to-end automated test that runs the full pipeline (dictionary generation → SQL generation → NL rendering → perturbation generation → all three test suites) across **20 Spider databases**. Outputs per-database results to `test_dbs/spider/spider_batch_results.csv`.

---

## 3. Experiment Execution Phase

### Experiment Orchestrator (`run_experiments.py`)

```bash
python run_experiments.py --schema schemas/<name>.yaml
```

1. Loads and merges three datasets (baseline NL, systematic perturbations, LLM perturbations) into a single task pool using `load_all_tasks(dataset_dir, schema_name)`.
2. Reads model configurations from `experiments.yaml` (e.g., `gemini`, `local-qwen3-coder`, `llama-3-sqlcoder`, `deepseek-coder`).
3. Uses the `LLMWorker` + `ExperimentRunner` harness from `src/harness/` to dispatch prompts and record generated SQL output.
4. `LLMWorker` receives `schema`, `foreign_keys`, `dialect` dynamically — no hardcoded schema assumptions.

---

## 4. Evaluation and Analysis Phase

### Analysis & Reporting (`analyze_results.py`)

```bash
python analyze_results.py --schema schemas/<name>.yaml [--input RESULTS.jsonl]
```

1. **Aggregation:** Combines output records from all tested models.
2. **Equivalence Checking** via `SQLEquivalenceEngine`:
   - **DQL (SELECT):** Generates randomly seeded temporary SQLite databases. Both Gold SQL and Generated SQL are executed; table denotations are compared. Match on all seed DBs → equivalent.
   - **DML (INSERT, UPDATE, DELETE):** "State Delta" approach — creates identical twin databases; Gold on A, Generated on B; full post-execution table state compared across all fuzzed databases.
   - Multiprocessing with isolated local test DBs per worker + query caching.
3. **Visual Analytics:** Accuracy by Model, Accuracy Drop from Baseline, Faceted Heatmaps (Complexity × Perturbation Type).

---

## 5. Dataset Format

All pipeline outputs use a metadata-wrapped JSON envelope:

```json
{
  "metadata": {
    "schema_name": "european_football_1",
    "dialect": "sqlite",
    "schema_source": "dataset/train/train_databases/european_football_1/european_football_1.sqlite",
    "generated_at": "2026-03-04T...",
    "num_records": 140,
    "pipeline_step": "01_generate_sql_dataset"
  },
  "records": [ ... ]
}
```

---

## 6. Adding a New Schema

### From a YAML definition

1. Create `schemas/<name>.yaml` with tables, columns (with types and `is_pk`), and foreign keys.
2. Generate dictionary: `python generate_dictionary.py --schema schemas/<name>.yaml --outdir schemas/`
3. Run the pipeline:
   ```bash
   python 01_generate_sql_dataset.py -s schemas/<name>.yaml
   python 02_generate_nl_prompts.py -s schemas/<name>.yaml --two-pass -d schemas/<name>_dictionary.yaml
   python 03_generate_systematic_perturbations.py -s schemas/<name>.yaml
   ```
4. Validate:
   ```bash
   python pipeline_tests/generation_process/sql/test_sql_generation.py \
       --input dataset/<name>/raw_queries.json --schema schemas/<name>.yaml
   python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py \
       --input dataset/<name>/nl_prompts.json --schema schemas/<name>.yaml \
       --dictionary schemas/<name>_dictionary.yaml
   python pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py \
       --input dataset/<name>/systematic_perturbations.json --schema schemas/<name>.yaml \
       --dictionary schemas/<name>_dictionary.yaml
   ```

### From a SQLite database (e.g., Spider)

1. Generate dictionary: `python generate_dictionary.py --schema path/to/database.sqlite --outdir schemas/`
2. Run the pipeline with the `.sqlite` file as the schema:
   ```bash
   python 01_generate_sql_dataset.py -s path/to/database.sqlite
   python 02_generate_nl_prompts.py -s path/to/database.sqlite --two-pass -d schemas/<name>_dictionary.yaml
   python 03_generate_systematic_perturbations.py -s path/to/database.sqlite
   ```
3. Validate with the same `--schema` pointing to the `.sqlite` file.

---

## 7. Project Structure

```
sql-nl/
├── 01_generate_sql_dataset.py           # Step 1: SQL generation
├── 02_generate_nl_prompts.py            # Step 2: NL rendering
├── 03_generate_systematic_perturbations.py  # Step 3: Deterministic perturbations
├── 04_generate_llm_nl_and_perturbations.py  # Step 4: LLM-based perturbations
├── generate_dictionary.py               # Dictionary generation (WordNet)
├── run_experiments.py                   # LLM experiment orchestrator
├── analyze_results.py                   # Equivalence evaluation + analytics
├── analyze_results_systematic.py        # Systematic perturbation analysis
├── run_equivalence_test.py              # Standalone equivalence checker
├── cross_schema_test.py                 # Quick schema-agnostic smoke test
├── experiments.yaml                     # Model configurations
│
├── src/
│   ├── core/
│   │   ├── schema_config.py             # SchemaConfig, TableDef, ColumnDef, ForeignKeyDef
│   │   ├── schema_loader.py             # load_schema(), load_from_yaml(), load_from_sqlite()
│   │   ├── generator.py                 # SQLQueryGenerator
│   │   ├── nl_renderer.py               # SQLToNLRenderer (syntax-directed translation)
│   │   ├── template_resolver.py         # TemplateResolver (two-pass IR resolution)
│   │   ├── linguistic_dictionary.py     # LinguisticDictionary
│   │   └── dictionary_builder.py        # build_dictionary(), save/load
│   ├── complexity/                      # 7 ComplexityHandler subclasses + registry
│   ├── perturbations/                   # 13 PerturbationStrategy subclasses + registry
│   ├── equivalence/                     # SQLEquivalenceEngine, DQL/DML checkers
│   ├── harness/                         # LLMWorker, ExperimentRunner, adapters/
│   └── utils/
│
├── schemas/                             # YAML schemas + dictionaries
├── dataset/                             # Generated datasets per schema
├── pipeline_tests/generation_process/   # Test suites (sql/, nl_prompt/, systematic_perturbations/)
└── test_dbs/spider/                     # Batch robustness testing infrastructure
    ├── robustness_test.py               # 20-database end-to-end test runner
    └── spider_batch_results.csv         # Latest results
```

---

## 8. Validation Results

### Batch Robustness Test (20 Spider Databases)

Run: `python test_dbs/spider/robustness_test.py` — generates data and runs all tests per database.

| Database | SQL Checks | NL Checks | Pert Checks | Failures |
|----------|-----------|-----------|-------------|----------|
| european_football_1 | 2,454 | 2,115 | 15,238 | 0 |
| sales_in_weather | 2,522 | 2,140 | 15,711 | 0 |
| craftbeer | 2,482 | 2,108 | 15,687 | 0 |
| soccer_2016 | 2,393 | 2,061 | 15,449 | 0 |
| restaurant | 2,396 | 2,082 | 15,225 | 0 |
| movie | 2,609 | 2,180 | 15,417 | 0 |
| olympics | 2,318 | 2,024 | 15,783 | 0 |
| language_corpus | 2,392 | 2,078 | 15,510 | 0 |
| app_store | 2,527 | 2,144 | 14,999 | 0 |
| sales | 2,405 | 2,085 | 15,269 | 0 |
| video_games | 2,319 | 2,032 | 15,776 | 0 |
| image_and_language | 2,363 | 2,051 | 15,386 | 0 |
| software_company | 2,558 | 2,160 | 15,634 | 0 |
| authors | 2,429 | 2,113 | 15,719 | 0 |
| movies_4 | 2,324 | 2,026 | 15,251 | 0 |
| social_media | 2,586 | 2,176 | 15,170 | 0 |
| human_resources | 2,492 | 2,114 | 15,143 | 0 |
| regional_sales | 2,325 | 2,015 | 15,157 | 0 |
| computer_student | 2,340 | 2,038 | 15,456 | 0 |
| works_cycles | 2,486 | 2,129 | 15,597 | 0 |
| **Total** | **48,720** | **41,871** | **308,577** | **0** |

**20/20 databases — 399,168 checks — 0 failures — 100.000% pass rate**
