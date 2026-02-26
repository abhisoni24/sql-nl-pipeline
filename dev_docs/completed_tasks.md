# Completed Tasks

> **Instructions:** When you complete a task from the [implementation_action_plan.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/implementation_action_plan.md), move it here with a brief completion note. This file is the single source of truth for what has been done.

---

## Phase 1: Schema Abstraction Layer

| Step                                   | Status  | Notes                                                                                                             |
| -------------------------------------- | ------- | ----------------------------------------------------------------------------------------------------------------- |
| 1.1 Create `SchemaConfig` dataclass    | ✅ Done | `src/core/schema_config.py` — `SchemaConfig`, `TableDef`, `ColumnDef`, `ForeignKeyDef` with legacy compat methods |
| 1.2 Create schema loaders              | ✅ Done | `src/core/schema_loader.py` — `load_from_yaml()`, `load_from_sqlite()`, `load_from_legacy()`                      |
| 1.3 Create `social_media.yaml`         | ✅ Done | `schemas/social_media.yaml` — 5 tables, 6 FK definitions                                                          |
| 1.4 Verify parity with existing schema | ✅ Done | All 12 FK pairs match, 5 table schemas match, FK column marking verified on both sides                            |

## Phase 2: Linguistic Dictionary Builder

| Step                                        | Status  | Notes                                                                                       |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------------------------- |
| 2.1 Create `LinguisticDictionary` dataclass | ✅ Done | `src/core/linguistic_dictionary.py` — schema-specific + universal banks, lookup methods     |
| 2.2 Create automated dictionary builder     | ✅ Done | `src/core/dictionary_builder.py` — WordNet expansion, compound synonyms, category inference |
| 2.3 Save/load dictionary as YAML            | ✅ Done | `save_dictionary()` and `load_dictionary()` in dictionary_builder.py, round-trip verified   |
| 2.4 Verify dictionary quality               | ✅ Done | 100% coverage vs existing synonyms, categories correct, all lookups work                    |

## Phase 3: Complexity Type Registry

| Step                                      | Status  | Notes                                                                          |
| ----------------------------------------- | ------- | ------------------------------------------------------------------------------ |
| 3.1 Define `ComplexityHandler` ABC        | ✅ Done | `src/complexity/base.py` — ABC with `generate()`, `is_match()`                 |
| 3.2 Extract logic into handler classes    | ✅ Done | 7 handler files: simple, join_handler, advanced, union, insert, update, delete |
| 3.3 Build the registry                    | ✅ Done | `src/complexity/registry.py` — `get_handler()`, `all_handlers()`, `register()` |
| 3.4 Update `generator.py` to use registry | ✅ Done | Replaced if/elif chain and hardcoded list with registry-based dispatch         |
| 3.5 Verify generation parity              | ✅ Done | 350 queries generated, SQL test suite: **5975/5975 passed, 0 failures**        |

## Phase 4: Two-Pass NL Renderer

| Step                                    | Status  | Notes                                                                                                                        |
| --------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------------------------------- |
| 4.1 Define IR token format              | ✅ Done | 7 token types: `[TABLE:x]`, `[COL:x]`, `[OP:x]`, `[AGG:x]`, `[VAL:x]`, `[VERB:x]`, `[CONN:x]`                             |
| 4.2 Refactor renderer to emit IR tokens | ✅ Done | `render_template()` added to `SQLToNLRenderer`; `_render_table`, `_render_column`, `_render_expression`, `_choose_word` emit IR via `_emit_mode` flag |
| 4.3 Create `TemplateResolver` (Pass 2)  | ✅ Done | `src/core/template_resolver.py` — resolves IR tokens against `LinguisticDictionary` with seeded RNG                          |
| 4.4 Wire two-pass pipeline together     | ✅ Done | `02_generate_nl_prompts.py` supports `--two-pass` and `--schema` CLI flags; stores `ir_template` in JSON                     |
| 4.5 Verify NL quality                   | ✅ Done | 3150/3150 template checks passed (350 queries × 9 checks); 5172/5172 NL tests, 5969/5969 SQL tests, 40187/40187 perturbation tests — **0 regressions** |

## Phase 5: Modular Perturbation Framework

| Step                                                | Status     | Notes |
| --------------------------------------------------- | ---------- | ----- |
| 5.1 Define `PerturbationStrategy` ABC               | ⬜ Pending |       |
| 5.2 Extract perturbations into strategy files       | ⬜ Pending |       |
| 5.3 Build perturbation registry with auto-discovery | ⬜ Pending |       |
| 5.4 Update perturbation generation script           | ⬜ Pending |       |
| 5.5 Create unified test runner                      | ⬜ Pending |       |
| 5.6 Verify perturbation parity                      | ⬜ Pending |       |

## Phase 6: Pipeline Script Refactoring

| Step                                           | Status     | Notes |
| ---------------------------------------------- | ---------- | ----- |
| 6.1 Add `--schema` CLI argument to all scripts | ⬜ Pending |       |
| 6.2 Update output directory structure          | ⬜ Pending |       |
| 6.3 Standardize dataset JSON format            | ⬜ Pending |       |

## Phase 7: Equivalence Checker Generalization

| Step                                   | Status     | Notes |
| -------------------------------------- | ---------- | ----- |
| 7.1 Remove hardcoded schema import     | ⬜ Pending |       |
| 7.2 Update `EquivalenceConfig`         | ⬜ Pending |       |
| 7.3 Update `from_schema()` classmethod | ⬜ Pending |       |
| 7.4 Update `analyze_results.py`        | ⬜ Pending |       |
| 7.5 Verify equivalence checker         | ⬜ Pending |       |

## Phase 8: Test Suite Migration

| Step                                             | Status     | Notes                                                              |
| ------------------------------------------------ | ---------- | ------------------------------------------------------------------ |
| 8.1 Replace hardcoded references in test scripts | ✅ Done    | `test_sql_generation.py` & `test_nl_prompt.py` are schema-agnostic |
| 8.2 Make perturbation tests use strategy checks  | ⬜ Pending |                                                                    |
| 8.3 Add schema-parametric integration test       | ✅ Done    | Built and ran `cross_schema_test.py` against 3 different schemas   |

## Phase 9: End-to-End Validation

| Step                                    | Status     | Notes |
| --------------------------------------- | ---------- | ----- |
| 9.1 Full regression run on social media | ⬜ Pending |       |
| 9.2 Create healthcare test schema       | ⬜ Pending |       |
| 9.3 Full pipeline run on healthcare     | ⬜ Pending |       |
| 9.4 Update PIPELINE documentation       | ⬜ Pending |       |
