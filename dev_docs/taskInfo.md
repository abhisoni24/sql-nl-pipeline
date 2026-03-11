# SQL-NL Pipeline: Agent Task Reference

> **Purpose:** This document is the onboarding reference for any AI agent assigned to work on the SQL-NL pipeline overhaul. Read this **first** before touching any code. Then check [completed_tasks.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/completed_tasks.md) to see what's already done, and [implementation_action_plan.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/implementation_action_plan.md) for the detailed step-by-step instructions for each task.

---

## 1. Project Overview

This project implements a **SQL → Natural Language → SQL\*** evaluation pipeline. It:

1. **Generates** synthetic SQL queries across 7 complexity types (simple, join, advanced, union, insert, update, delete).
2. **Renders** each SQL query into a baseline ("vanilla") natural language prompt using a deterministic, AST-based Syntax-Directed Translation (SDT) engine.
3. **Perturbs** each NL prompt via 13 systematic perturbation strategies (typos, synonym substitution, verbosity injection, etc.) to create diverse prompt variants.
4. **Executes** these NL prompts against multiple LLMs (Gemini, Deepseek, Qwen, etc.) to generate candidate SQL.
5. **Evaluates** candidate SQL against the original gold SQL using a semantic equivalence checker (test-suite-based execution comparison, not string matching).

### The Problem We Are Solving

The entire pipeline is currently **hardcoded to a single social media database schema** (5 tables: `users`, `posts`, `comments`, `likes`, `follows`). Table names, column names, foreign key relationships, synonym banks, and test assertions all reference this specific schema throughout the codebase. **Our goal is to make this pipeline schema-agnostic** — capable of running on any SQLite schema (healthcare, government, finance, etc.) with minimal or zero manual configuration.

---

## 2. Project Root and Key Directories

**Project root:** `/Users/obby/Documents/experiment/random/sql-nl/`

| Directory/File                            | Purpose                                                                                                                                                                                                |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `src/core/schema.py`                      | **CURRENT:** Hardcoded social media schema dicts (`SCHEMA`, `FOREIGN_KEYS`). **TARGET:** Replaced by `SchemaConfig` + loaders.                                                                         |
| `src/core/generator.py`                   | SQL query generator. Contains `SQLQueryGenerator` with monolithic `generate_query()` if/elif dispatch.                                                                                                 |
| `src/core/nl_renderer.py`                 | NL rendering engine. Contains `SQLToNLRenderer` (~1165 lines). Handles both baseline rendering and perturbation application. Hardcoded synonym banks in `__init__`.                                    |
| `src/equivalence/`                        | SQL equivalence checking engine. `equivalence_engine.py` routes to `dql_equivalence.py` (SELECT) or `dml_equivalence.py` (INSERT/UPDATE/DELETE). Currently imports `SCHEMA` directly from `schema.py`. |
| `src/harness/`                            | LLM experiment execution harness (`LLMWorker`, `ExperimentRunner`). Not in scope for this overhaul.                                                                                                    |
| `pipeline_tests/generation_process/`      | Test suites for generation quality: `sql/` (SQL structure tests), `nl_prompt/` (NL fidelity tests), `systematic_perturbations/` (per-perturbation contract tests).                                     |
| `01_generate_sql_dataset.py`              | Pipeline Step 1: Generate raw SQL queries.                                                                                                                                                             |
| `02_generate_nl_prompts.py`               | Pipeline Step 2: Render SQL → baseline NL.                                                                                                                                                             |
| `03_generate_systematic_perturbations.py` | Pipeline Step 3: Apply deterministic perturbations.                                                                                                                                                    |
| `04_generate_llm_perturbations_cached.py` | Pipeline Step 4: Apply LLM-based perturbations (Gemini).                                                                                                                                               |
| `run_experiments.py`                      | Runs NL prompts through LLMs to generate candidate SQL.                                                                                                                                                |
| `analyze_results.py`                      | Evaluates generated SQL via equivalence checker + generates plots.                                                                                                                                     |
| `schemas/`                                | **NEW:** Schema definition files (YAML).                                                                                                                                                               |
| `dev_docs/`                               | **NEW:** Development documentation and action plan.                                                                                                                                                    |

---

## 3. Current Architecture (BEFORE Overhaul)

```
                    ┌─────────────────────────┐
                    │   src/core/schema.py     │  ← Hardcoded social media dicts
                    │   SCHEMA, FOREIGN_KEYS   │
                    └──────────┬──────────────┘
                               │ imported directly by
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                     ▼
   generator.py          nl_renderer.py       equivalence_engine.py
   ├─ generate_query()   ├─ render()          ├─ _ensure_base_database()
   │  (250-line if/elif   │  (isinstance chain  │  imports SCHEMA directly
   │   over strings)      │   + scattered        │
   │                      │   if is_active()     │
   └─ generate_dataset()  │   perturbation       │
      (hardcoded list)    │   blocks)            │
                          └─ schema_synonyms     │
                             (hardcoded dict)    │
```

### Key Coupling Points

1. **`schema.py`** — Every module imports `SCHEMA` and `FOREIGN_KEYS` directly.
2. **`nl_renderer.py` L100–113** — `schema_synonyms` dict maps only social media table/column names to English synonyms.
3. **`generator.py` L275** — Complexity type list is a hardcoded Python list literal.
4. **`generator.py` L299–546** — `generate_query()` is a single 250-line if/elif chain.
5. **`nl_renderer.py` L224–308** — `render()` method dispatches on `isinstance(ast, ...)` and applies perturbations via scattered `if config.is_active(...)` blocks.
6. **`nl_renderer.py` L14–28** — `PerturbationType` is an Enum. Adding a perturbation requires editing the Enum, adding logic in `render()`, adding a test file, and updating `03_generate_systematic_perturbations.py`.
7. **`equivalence_engine.py` L68–71** — `_ensure_base_database()` imports `SCHEMA` via `from src.core.schema import SCHEMA, FOREIGN_KEYS`.
8. **`pipeline_tests/`** — Test scripts reference `users`, `posts`, `likes` tables directly.

---

## 4. Target Architecture (AFTER Overhaul)

```
schemas/social_media.yaml ─┐
schemas/healthcare.yaml  ──┤
any_database.sqlite  ──────┤
                            ▼
                  ┌──────────────────────┐
                  │  SchemaConfig        │   ← Universal schema object
                  │  (schema_config.py)  │
                  │  + SchemaLoader      │
                  │  (schema_loader.py)  │
                  └─────────┬────────────┘
                            │
              ┌─────────────┼──────────────────┐
              ▼             ▼                   ▼
    ┌──────────────┐  ┌───────────────┐  ┌──────────────┐
    │ Complexity   │  │ Dictionary    │  │ Equivalence  │
    │ Registry     │  │ Builder       │  │ Engine       │
    │              │  │ (WordNet/NLTK)│  │ (schema via  │
    │ simple.py    │  └───────┬───────┘  │  config)     │
    │ join.py      │          ▼          └──────────────┘
    │ advanced.py  │  ┌───────────────┐
    │ union.py     │  │ Linguistic    │
    │ insert.py    │  │ Dictionary    │
    │ update.py    │  └───────┬───────┘
    │ delete.py    │          │
    └──────┬───────┘          │
           │                  │
           ▼                  ▼
    ┌──────────────────────────────────┐
    │   Two-Pass NL Renderer           │
    │   Pass 1: AST → IR Template      │
    │   Pass 2: IR → NL (dictionary)   │
    └──────────────┬───────────────────┘
                   ▼
    ┌──────────────────────────────────┐
    │   Perturbation Strategy Registry │
    │   src/perturbations/             │
    │   ├── typos.py                   │
    │   ├── verbosity.py               │
    │   ├── synonym_sub.py             │
    │   └── ... (auto-discovered)      │
    └──────────────┬───────────────────┘
                   ▼
           Final Dataset JSON
           (schema-tagged)
```

### New Modules Being Created

| Module                                                                | Purpose                                                                  |
| --------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `src/core/schema_config.py`                                           | `SchemaConfig`, `TableDef`, `ColumnDef`, `ForeignKeyDef` dataclasses     |
| `src/core/schema_loader.py`                                           | `load_from_yaml()`, `load_from_sqlite()`, `load_from_legacy()`           |
| `src/core/linguistic_dictionary.py`                                   | `LinguisticDictionary` dataclass with synonym banks                      |
| `src/core/dictionary_builder.py`                                      | Automated synonym builder using WordNet/NLTK                             |
| `src/core/template_resolver.py`                                       | Pass 2 engine: resolves `[TABLE:x]`, `[COL:y]` tokens against dictionary |
| `src/complexity/base.py`                                              | `ComplexityHandler` ABC                                                  |
| `src/complexity/registry.py`                                          | Auto-registering complexity handler registry                             |
| `src/complexity/{simple,join,advanced,union,insert,update,delete}.py` | One handler per complexity type                                          |
| `src/perturbations/base.py`                                           | `PerturbationStrategy` ABC                                               |
| `src/perturbations/registry.py`                                       | Auto-discovering perturbation strategy registry                          |
| `src/perturbations/{typos,verbosity,...}.py`                          | One strategy per perturbation (13 files)                                 |

---

## 5. The 13 Perturbation Types

These are the deterministic NL prompt perturbation strategies currently implemented. Each will become a self-contained module in `src/perturbations/`.

| #   | Machine Name                                | What It Does                                           | Layer           |
| --- | ------------------------------------------- | ------------------------------------------------------ | --------------- |
| 1   | `omit_obvious_operation_markers`            | Removes explicit SQL clause keywords from NL           | template        |
| 2   | `phrasal_and_idiomatic_action_substitution` | Replaces query action verbs with synonyms              | dictionary      |
| 3   | `verbosity_variation`                       | Inserts conversational fillers and informal language   | post_processing |
| 4   | `operator_aggregate_variation`              | Varies how operators/aggregates are expressed          | dictionary      |
| 5   | `temporal_expression_variation`             | Uses relative temporal terms instead of absolute dates | dictionary      |
| 6   | `typos`                                     | Introduces realistic keyboard typos                    | post_processing |
| 7   | `comment_annotations`                       | Adds meta-comments like "-- for the audit"             | post_processing |
| 8   | `mixed_sql_nl`                              | Blends raw SQL keywords into NL                        | template        |
| 9   | `punctuation_variation`                     | Modifies sentence rhythm via punctuation changes       | post_processing |
| 10  | `urgency_qualifiers`                        | Prepends urgency markers like "URGENT:"                | post_processing |
| 11  | `table_column_synonyms`                     | Uses human-centric schema synonyms                     | dictionary      |
| 12  | `incomplete_join_spec`                      | Omits explicit JOIN/ON syntax in NL                    | template        |
| 13  | `anchored_pronoun_references`               | Replaces entity references with pronouns               | template        |

---

## 6. The 7 Complexity Types

| Complexity | SQL Feature                                        | Generator Method            | Renderer Method                          |
| ---------- | -------------------------------------------------- | --------------------------- | ---------------------------------------- |
| `simple`   | Single-table SELECT                                | `generate_query()` L321–335 | `render_select()`                        |
| `join`     | Two-table JOIN (INNER/LEFT/RIGHT/FULL)             | `generate_query()` L337–370 | `render_select()` + `_render_join()`     |
| `advanced` | Subqueries (WHERE IN, FROM, EXISTS) and self-joins | `generate_query()` L380–546 | `render_select()` + `_render_subquery()` |
| `union`    | UNION / UNION ALL                                  | `generate_union()`          | `render_union()`                         |
| `insert`   | INSERT INTO ... VALUES                             | `generate_insert()`         | `render_insert()`                        |
| `update`   | UPDATE ... SET ... WHERE                           | `generate_update()`         | `render_update()`                        |
| `delete`   | DELETE FROM ... WHERE                              | `generate_delete()`         | `render_delete()`                        |

---

## 7. How to Work on a Task

### Before starting:

1. **Read this file** for project context.
2. **Read [completed_tasks.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/completed_tasks.md)** to see what phases/steps are already done. Do NOT redo completed work.
3. **Read [implementation_action_plan.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/implementation_action_plan.md)** for the detailed step-by-step instructions for the specific task you've been assigned.

### While working:

- **Follow the action plan exactly.** Each step has specific file paths, code examples, and verification commands.
- **Run verification commands** at the end of each step to confirm correctness.
- **Do not modify files outside the scope** of your assigned task unless absolutely necessary for it to work.
- **Preserve backward compatibility** during migration. Use the `get_legacy_schema()` and `get_fk_pairs()` methods on `SchemaConfig` to keep existing consumers working until they are individually updated.

### After completing:

- Update [completed_tasks.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/completed_tasks.md) by moving the completed step from "Pending" to "Completed" with a brief note of what was done.

---

## 8. Important Design Decisions

1. **No LLM in the generation pipeline.** SQL generation, NL rendering, and systematic perturbations must remain fully deterministic. Only `04_generate_llm_perturbations_cached.py` uses an LLM, and it is separate from the core pipeline.

2. **AST-based approach is mandatory.** All SQL parsing uses `sqlglot`. All rendering walks the `sqlglot` AST. Do not introduce string-based SQL manipulation.

3. **Two-pass rendering.** Pass 1 produces an abstract template with `[TYPE:value]` placeholder tokens. Pass 2 resolves these tokens against the `LinguisticDictionary`. This separation is what makes the renderer schema-agnostic.

4. **Perturbations are self-contained modules.** Each perturbation strategy lives in its own file under `src/perturbations/` and is auto-discovered by the registry. Adding a perturbation = creating one file. Removing = deleting one file.

5. **Complexity types are pluggable.** Each complexity type lives in its own file under `src/complexity/` and registers itself. Adding a new SQL complexity type = creating one handler file.

6. **Schema input is flexible.** The pipeline accepts schemas via YAML files or SQLite reflection. The `SchemaConfig` dataclass is the universal internal representation.

7. **Linguistic dictionary is built programmatically.** WordNet/NLTK synsets are used to generate synonyms for table and column names. No LLM involvement. The generated dictionary can be manually reviewed and edited.

---

## 9. Testing Infrastructure

| Test Suite             | Location                                                                                   | What It Validates                                         |
| ---------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------- |
| SQL Generation         | `pipeline_tests/generation_process/sql/test_sql_generation.py`                             | Structural integrity of generated SQL (37 checks)         |
| NL Prompt Fidelity     | `pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py`                            | Baseline NL prompt quality (62 checks)                    |
| Perturbation Contracts | `pipeline_tests/generation_process/systematic_perturbations/test_*.py`                     | Per-perturbation quality (13 test files, ~14 checks each) |
| Perturbation Runner    | `pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py` | Orchestrates all 13 perturbation test suites              |

**Running all tests:**

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py
python pipeline_tests/generation_process/systematic_perturbations/run_all_perturbation_tests.py
```

---

## 10. Key Dependencies

| Package                  | Version | Used For                                          |
| ------------------------ | ------- | ------------------------------------------------- |
| `sqlglot`                | latest  | SQL parsing and AST manipulation                  |
| `pyyaml`                 | latest  | YAML schema file loading                          |
| `nltk`                   | latest  | WordNet synonym expansion (dictionary builder)    |
| `seaborn` / `matplotlib` | latest  | Analysis plots                                    |
| `google-generativeai`    | latest  | Gemini API (LLM perturbations + experiments only) |
