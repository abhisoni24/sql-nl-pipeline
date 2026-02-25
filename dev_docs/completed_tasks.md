# Completed Tasks

> **Instructions:** When you complete a task from the [implementation_action_plan.md](file:///Users/obby/Documents/experiment/random/sql-nl/dev_docs/implementation_action_plan.md), move it here with a brief completion note. This file is the single source of truth for what has been done.

---

## Phase 1: Schema Abstraction Layer

| Step                                   | Status     | Notes |
| -------------------------------------- | ---------- | ----- |
| 1.1 Create `SchemaConfig` dataclass    | ⬜ Pending |       |
| 1.2 Create schema loaders              | ⬜ Pending |       |
| 1.3 Create `social_media.yaml`         | ⬜ Pending |       |
| 1.4 Verify parity with existing schema | ⬜ Pending |       |

## Phase 2: Linguistic Dictionary Builder

| Step                                        | Status     | Notes |
| ------------------------------------------- | ---------- | ----- |
| 2.1 Create `LinguisticDictionary` dataclass | ⬜ Pending |       |
| 2.2 Create automated dictionary builder     | ⬜ Pending |       |
| 2.3 Save/load dictionary as YAML            | ⬜ Pending |       |
| 2.4 Verify dictionary quality               | ⬜ Pending |       |

## Phase 3: Complexity Type Registry

| Step                                      | Status     | Notes |
| ----------------------------------------- | ---------- | ----- |
| 3.1 Define `ComplexityHandler` ABC        | ⬜ Pending |       |
| 3.2 Extract logic into handler classes    | ⬜ Pending |       |
| 3.3 Build the registry                    | ⬜ Pending |       |
| 3.4 Update `generator.py` to use registry | ⬜ Pending |       |
| 3.5 Verify generation parity              | ⬜ Pending |       |

## Phase 4: Two-Pass NL Renderer

| Step                                    | Status     | Notes |
| --------------------------------------- | ---------- | ----- |
| 4.1 Define IR token format              | ⬜ Pending |       |
| 4.2 Refactor renderer to emit IR tokens | ⬜ Pending |       |
| 4.3 Create `TemplateResolver` (Pass 2)  | ⬜ Pending |       |
| 4.4 Wire two-pass pipeline together     | ⬜ Pending |       |
| 4.5 Verify NL quality                   | ⬜ Pending |       |

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

| Step                                             | Status     | Notes |
| ------------------------------------------------ | ---------- | ----- |
| 8.1 Replace hardcoded references in test scripts | ⬜ Pending |       |
| 8.2 Make perturbation tests use strategy checks  | ⬜ Pending |       |
| 8.3 Add schema-parametric integration test       | ⬜ Pending |       |

## Phase 9: End-to-End Validation

| Step                                    | Status     | Notes |
| --------------------------------------- | ---------- | ----- |
| 9.1 Full regression run on social media | ⬜ Pending |       |
| 9.2 Create healthcare test schema       | ⬜ Pending |       |
| 9.3 Full pipeline run on healthcare     | ⬜ Pending |       |
| 9.4 Update PIPELINE documentation       | ⬜ Pending |       |
