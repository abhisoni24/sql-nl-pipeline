# SQL → NL Pipeline: Progress Report
### Schema-Agnostic Overhaul — Final Status Presentation
**Date:** February 26, 2026

---

## Slide 1: Executive Summary

The SQL → NL → Perturbation → Evaluation pipeline has been transformed from a **single-schema, hardcoded prototype** into a **fully schema-agnostic, extensible framework**.

| Metric | Before | After |
|--------|--------|-------|
| Supported schemas | 1 (social_media only) | **Any** — 6 validated (social_media, bank, hospital, university_system, smart_city, authors) |
| Schema onboarding | Manual code changes in 20+ files | **1 file** (YAML schema or `.sqlite` DB + auto-generated dictionary) |
| Schema input formats | Hardcoded Python dict | **YAML** or **SQLite database** (auto-detected) |
| Perturbation strategies | Monolithic function with if/elif | **13 modular strategies** with auto-discovery registry |
| NL rendering | Single-pass with hardcoded synonyms | **Two-pass IR token architecture** with dictionary-driven resolution |
| Test coverage | Fragile, baseline-comparison heuristics | **Schema+dictionary-aware validation**, 0 failures across 6 schemas |
| Dataset organization | Flat files in `dataset/current/` | **`dataset/<schema>/`** structured directories |

---

## Slide 2: Previous Pipeline Architecture (Before)

The original pipeline was tightly coupled to the `social_media` schema at every layer.

```mermaid
flowchart TD
    A["src/core/schema.py</br>Hardcoded SCHEMA dict & FOREIGN_KEYS</br>(social_media tables only)"] --> B["01_generate_sql_dataset.py</br>No --schema flag</br>Hardcoded table/column references"]
    B --> C["dataset/current/</br>raw_social_media_queries.json</br>Flat directory, schema in filename"]
    C --> D["02_generate_nl_prompts.py</br>Hardcoded synonym mappings</br>Single-pass rendering"]
    D --> E["dataset/current/</br>nl_social_media_queries.json"]
    E --> F["03_generate_perturbations.py</br>Monolithic perturbation function</br>if/elif chain for 13 types"]
    F --> G["dataset/current/</br>nl_social_media_queries_systematic.json"]
    G --> H["Test Suite</br>Hardcoded file paths</br>Baseline-comparison heuristics"]

    style A fill:#ff6b6b,color:#fff
    style B fill:#ff9f9f,color:#000
    style C fill:#ffd93d,color:#000
    style D fill:#ff6b6b,color:#fff
    style E fill:#ffd93d,color:#000
    style F fill:#ff6b6b,color:#fff
    style G fill:#ffd93d,color:#000
    style H fill:#ff6b6b,color:#fff
```

### Key Pain Points

1. **Schema Lock-in:** Adding a new schema (e.g., bank) required editing 20+ source files — schema definitions, renderer synonym maps, generator logic, test paths, and evaluation scripts.
2. **Monolithic Code:** SQL generation used a single 800+ line generator with if/elif chains; perturbations were a single function dispatching 13 types.
3. **Brittle Tests:** Tests compared perturbed text against baseline text using string heuristics, causing false failures when synonyms or rendering varied across schemas.
4. **Flat Dataset Layout:** All files dumped into `dataset/current/` with schema name embedded in filename — unscalable and confusing.

---

## Slide 3: Previous Pipeline Data Flow (Before)

```mermaid
flowchart TD
    A["src/core/schema.py<br/>(Hardcoded social_media)"] --> B["01_generate_sql_dataset.py"]
    B --> C["dataset/current/<br/>raw_social_media_queries.json"]
    C --> D["02_generate_nl_prompts.py<br/>(Hardcoded synonyms)"]
    D --> E["dataset/current/<br/>nl_social_media_queries.json"]
    E --> F["03_generate_perturbations.py<br/>(Monolithic if/elif)"]
    F --> G["dataset/current/<br/>nl_social_media_queries_systematic.json"]
    G --> H["Test Suite<br/>(Hardcoded paths)"]

    style A fill:#ff6b6b,color:#fff
    style D fill:#ff6b6b,color:#fff
    style F fill:#ff6b6b,color:#fff
    style H fill:#ff6b6b,color:#fff
```

---

## Slide 4: Transformation — 9 Phases of Work

The overhaul was executed in 9 phases, each building on the previous:

| Phase | Name | What Changed |
|-------|------|-------------|
| **1** | Schema Abstraction Layer | `SchemaConfig` dataclass + YAML/SQLite loaders replace hardcoded dicts |
| **2** | Linguistic Dictionary Builder | WordNet-based synonym generation, per-schema YAML dictionaries </br> **(May need LLM here once)** |
| **3** | Complexity Type Registry | 7 handler classes (simple, join, advanced, union, insert, update, delete) with auto-registry |
| **4** | Two-Pass NL Renderer | IR token emission (Pass 1) → Dictionary-driven resolution (Pass 2) |
| **5** | Modular Perturbation Framework | 13 strategy classes with `PerturbationStrategy` ABC + auto-discovery registry |
| **6** | Pipeline Script Refactoring | `--schema` CLI flags, `dataset/<name>/` directories, metadata JSON envelope |
| **7** | Equivalence Checker Generalization | Schema-agnostic DB generation, dynamic system prompts |
| **8** | Test Suite Migration | Schema+dictionary-aware validators, unified test runner |
| **9** | End-to-End Validation | Full pipeline runs on 6 schemas with 0 failures |

---

## Slide 5: Current Architecture Overview (After)

```mermaid
flowchart TD
    subgraph Input["Schema Definition Layer"]
        YAML["schemas/name.yaml</br>Declarative schema definition"]
        DICT["schemas/name_dictionary.yaml</br>Auto-generated synonyms via WordNet"]
    end

    subgraph Core["Core Abstractions"]
        SC["SchemaConfig</br>TableDef · ColumnDef · ForeignKeyDef</br>get_type_sets() · get_fk_pairs()"]
        LD["LinguisticDictionary</br>table_synonyms · column_synonyms</br>table_categories (person/object/event)"]
    end

    subgraph Gen["Generation Pipeline"]
        G1["01 → SQL Gen</br>ComplexityRegistry → 7 handlers"]
        G2["02 → NL Render</br>Two-Pass: IR tokens → resolver"]
        G3["03 → Perturb</br>StrategyRegistry → 13 strategies"]
        G4["04 → LLM Perturb</br>Multi-adapter: Gemini / OpenAI / vLLM"]
    end

    subgraph Data["dataset/name/"]
        D1["raw_queries.json"]
        D2["nl_prompts.json"]
        D3["systematic_perturbations.json"]
        D4["llm_perturbations.json"]
    end

    subgraph Test["Validation Test Suites (schema+dictionary-aware)"]
        T1["SQL Generation Tests (structural, schema)"]
        T2["NL Prompt Tests (synonym-aware)"]
        T3["Perturbation Tests ×13 (contract validation)"]
    end

    subgraph Eval["Experiment Execution"]
        EX["LLMWorker (schema-agnostic prompts)"]
        EQ["SQLEquivalenceEngine (schema-driven DB gen)"]
        AN["analyze_results.py (visual analytics)"]
    end

    YAML --> SC
    YAML --> DICT
    DICT --> LD
    SC --> G1
    SC --> G2
    LD --> G2
    SC --> G3
    LD --> G3
    SC --> G4
    G1 --> D1
    G2 --> D2
    G3 --> D3
    G4 --> D4
    D1 --> T1
    D2 --> T2
    D3 --> T3
    D2 --> EX
    D3 --> EX
    D4 --> EX
    EX --> EQ
    EQ --> AN

    linkStyle default stroke:#8B0000,stroke-width:2px;
    style Input fill:#4ecdc4,color:#000
    style Core fill:#45b7d1,color:#000
    style Gen fill:#96ceb4,color:#000
    style Data fill:#ffeaa7,color:#000
    style Test fill:#dfe6e9,color:#000
    style Eval fill:#a29bfe,color:#000
```

---

## Slide 6: Current Data Flow (After — Detailed)

```mermaid
flowchart TD
    subgraph Input["Schema Definition (2 YAML files)"]
        YAML["schemas/&lt;name&gt;.yaml<br/>Tables, Columns, Types, PKs, FKs"]
        DICT["schemas/&lt;name&gt;_dictionary.yaml<br/>Table & Column Synonyms"]
    end

    subgraph Core["Core Abstractions"]
        SC["SchemaConfig<br/>(schema_config.py)"]
        LD["LinguisticDictionary<br/>(linguistic_dictionary.py)"]
    end

    subgraph Gen["Generation Pipeline"]
        G1["01: SQL Generation<br/>ComplexityRegistry<br/>7 handlers × N queries"]
        G2["02: NL Rendering<br/>Pass 1: IR Tokens<br/>Pass 2: Dictionary Resolve"]
        G3["03: Systematic Perturbations<br/>StrategyRegistry<br/>13 strategies × is_applicable"]
        G4["04: LLM Perturbations<br/>Multi-adapter harness<br/>14 single + 1 compound"]
    end

    subgraph Data["dataset/&lt;name&gt;/"]
        D1["raw_queries.json"]
        D2["nl_prompts.json"]
        D3["systematic_perturbations.json"]
        D4["llm_perturbations.json"]
    end

    subgraph Test["Validation Suites"]
        T1["SQL Tests<br/>Structural + Schema Compliance"]
        T2["NL Tests<br/>Synonym-aware Coverage"]
        T3["Perturbation Tests ×13<br/>Contract + was_applied"]
    end

    subgraph Eval["Experiment & Evaluation"]
        EX["run_experiments.py<br/>LLMWorker → models"]
        EQ["SQLEquivalenceEngine<br/>DQL: Denotation Match<br/>DML: State Delta"]
        AN["analyze_results.py<br/>Charts & Metrics"]
    end

    YAML --> SC
    DICT --> LD
    SC --> G1
    SC --> G2
    LD --> G2
    SC --> G3
    LD --> G3
    SC --> G4

    G1 --> D1
    D1 --> G2
    G2 --> D2
    D2 --> G3
    G3 --> D3
    D2 --> G4
    G4 --> D4

    D1 --> T1
    D2 --> T2
    D3 --> T3

    D2 --> EX
    D3 --> EX
    D4 --> EX
    EX --> EQ
    SC --> EQ
    EQ --> AN

    linkStyle default stroke:#8B0000,stroke-width:2px;
    style Input fill:#4ecdc4,color:#000
    style Core fill:#45b7d1,color:#000
    style Gen fill:#96ceb4,color:#000
    style Data fill:#ffeaa7,color:#000
    style Test fill:#dfe6e9,color:#000
    style Eval fill:#a29bfe,color:#000
```

---

## Slide 7: Key Architectural Innovations

### 7a. Schema Abstraction — From Hardcoded to Declarative

**Before:** A single Python file (`src/core/schema.py`) with hardcoded dictionaries for one schema.

**After:** Any schema is defined in a simple YAML file:

```yaml
# schemas/smart_city.yaml
name: smart_city
dialect: sqlite
tables:
  sensors:
    columns:
      id: { type: int, is_pk: true }
      asset_id: int
      sensor_type: varchar
      is_active: boolean
  # ... more tables
foreign_keys:
  - [assets, sensors, id, asset_id]
```

Loaded at runtime into a `SchemaConfig` object that provides type classification, FK resolution, and legacy-format compatibility.

### 7b. Two-Pass NL Rendering

**Before:** Single-pass renderer with hardcoded synonym maps mixed into rendering logic.

**After:**
- **Pass 1 (IR Emission):** Renderer emits structured tokens: `[TABLE:users]`, `[COL:email]`, `[OP:=]`, `[VERB:find]`
- **Pass 2 (Resolution):** `TemplateResolver` replaces tokens with natural language using the schema's `LinguisticDictionary`, with seeded RNG for reproducibility.

### 7c. Perturbation Strategy Pattern

**Before:** One monolithic function with 13 if/elif branches.

**After:** 13 independent strategy classes, each implementing:

```mermaid
classDiagram
    class PerturbationStrategy {
        <<ABC>>
        +name() str
        +is_applicable(sql_ast, context) bool
        +apply(nl_text, sql_ast, context) str
        +was_applied(baseline, perturbed, ctx) bool
    }
    class TypoStrategy { }
    class PronounStrategy { }
    class SynonymStrategy { }
    class PunctuationStrategy { }
    class VerbosityStrategy { }
    class MoreStrategies["... 8 more strategies"]

    PerturbationStrategy <|-- TypoStrategy
    PerturbationStrategy <|-- PronounStrategy
    PerturbationStrategy <|-- SynonymStrategy
    PerturbationStrategy <|-- PunctuationStrategy
    PerturbationStrategy <|-- VerbosityStrategy
    PerturbationStrategy <|-- MoreStrategies

    class StrategyRegistry {
        +get_strategy(name) PerturbationStrategy
        +all_strategies() List
        auto-discovers .py files
    }
    StrategyRegistry o-- PerturbationStrategy
```

Auto-discovered by `StrategyRegistry` — drop a new `.py` file and it's registered.

### 7d. `is_applicable` / `was_applied` Separation

Critical semantic fix: separated **"can this perturbation apply?"** (pre-generation) from **"did it actually change anything?"** (post-generation). This eliminated false positives in test validation and ensures accurate metadata in output datasets.

---

## Slide 8: Source Code Organization

```mermaid
graph LR
    subgraph src["src/"]
        subgraph core["core/"]
            SC["schema_config.py</br>SchemaConfig, TableDef,</br>ColumnDef, ForeignKeyDef"]
            SL["schema_loader.py</br>load_from_yaml()</br>load_from_sqlite()"]
            LDict["linguistic_dictionary.py</br>LinguisticDictionary</br>synonym banks"]
            DB["dictionary_builder.py</br>WordNet auto-generation"]
            GEN["generator.py</br>SQLQueryGenerator"]
            NLR["nl_renderer.py</br>SQLToNLRenderer</br>IR token emission"]
            TR["template_resolver.py</br>Pass 2: IR → NL"]
        end

        subgraph comp["complexity/ — 7 handlers"]
            CB["base.py — ComplexityHandler ABC"]
            CR["registry.py — auto-discovery"]
            CH["simple · join · advanced</br>union · insert · update · delete"]
        end

        subgraph pert["perturbations/ — 13 strategies"]
            PB["base.py — PerturbationStrategy ABC"]
            PR["registry.py — auto-discovery"]
            PS["typos · punctuation · urgency</br>synonyms · pronouns · ..."]
        end

        subgraph equiv["equivalence/"]
            EE["equivalence_engine.py</br>DQL denotation + DML state-delta"]
        end

        subgraph harness["harness/"]
            LW["llm_worker.py</br>Schema-agnostic LLM dispatch"]
        end

        subgraph utils["utils/"]
            GD["generate_dictionary.py</br>CLI dictionary generator"]
        end
    end

    style core fill:#45b7d1,color:#000
    style comp fill:#96ceb4,color:#000
    style pert fill:#ff9f9f,color:#000
    style equiv fill:#a29bfe,color:#000
    style harness fill:#ffeaa7,color:#000
    style utils fill:#dfe6e9,color:#000
```

---

## Slide 9: Dataset Directory Structure

```mermaid
graph LR
    subgraph Before["BEFORE: dataset/current/ (flat)"]
        direction TB
        F1["raw_social_media_queries.json"]
        F2["raw_social_media_queries_20.json"]
        F3["nl_social_media_queries.json"]
        F4["nl_social_media_queries_systematic_20.json"]
        F5["nl_social_media_queries_llm_perturbed_20.json"]
        F6["raw_bank_queries.json"]
        F7["nl_bank_queries.json"]
        F8["... all flat, schema in filename"]
    end

    subgraph After["AFTER: dataset/schema_name/ (organized)"]
        direction TB
        subgraph SM["social_media/"]
            S1["raw_queries.json"]
            S2["nl_prompts.json"]
            S3["systematic_perturbations.json"]
            S4["llm_perturbations.json"]
        end
        subgraph BK["bank/"]
            B1["raw_queries.json"]
            B2["nl_prompts.json"]
            B3["systematic_perturbations.json"]
        end
        subgraph HS["hospital/"]
            H1["raw_queries.json"]
            H2["nl_prompts.json"]
            H3["systematic_perturbations.json"]
        end
        subgraph US["university_system/"]
            U1["raw_queries.json"]
            U2["nl_prompts.json"]
            U3["systematic_perturbations.json"]
        end
        subgraph SC["smart_city/"]
            C1["raw_queries.json"]
            C2["nl_prompts.json"]
            C3["systematic_perturbations.json"]
        end
    end

    style Before fill:#ff6b6b22,stroke:#ff6b6b
    style After fill:#4ecdc422,stroke:#4ecdc4
    style SM fill:#96ceb4,color:#000
    style BK fill:#96ceb4,color:#000
    style HS fill:#96ceb4,color:#000
    style US fill:#96ceb4,color:#000
    style SC fill:#96ceb4,color:#000
```

---

## Slide 10: Adding a New Schema — 4 Steps, Zero Code Changes

```mermaid
flowchart LR
    A["1. Write YAML<br/>schemas/X.yaml"] --> B["2. Generate Dictionary<br/>python generate_dictionary.py<br/>--schema schemas/X.yaml"]
    B --> C["3. Run Pipeline<br/>python 01_... --schema schemas/X.yaml<br/>python 02_... --schema schemas/X.yaml<br/>python 03_... --schema schemas/X.yaml"]
    C --> D["4. Validate<br/>Run test suites with<br/>--schema & --dictionary"]

    style A fill:#4ecdc4,color:#000
    style B fill:#45b7d1,color:#000
    style C fill:#96ceb4,color:#000
    style D fill:#ffeaa7,color:#000
```

**Demonstrated live** with `smart_city` (8 tables, 53 columns, 7 FKs) — zero code changes, 51,178 checks, 0 failures.
Also demonstrated with **Spider benchmark** `authors.sqlite` — zero YAML writing, direct SQLite ingest, 50,997 checks, 0 failures.

---

## Slide 11: Validation Results — All 6 Schemas

### Per-Schema Test Results (Latest Run)

| Schema | Source | Tables | FKs | SQL Checks | NL Checks | Perturbation Checks | **Total** | **Failures** |
|--------|--------|--------|-----|-----------|-----------|---------------------|-----------|-------------|
| social_media | YAML | 5 | 6 | 5,958 | 5,170 | 41,260 | 52,388 | **0** |
| bank | YAML | 6 | 5 | 3,847 | 3,262 | 24,177 | 31,286 | **0** |
| hospital | YAML | 7 | 6 | 3,799 | 3,230 | 24,083 | 31,112 | **0** |
| university_system | YAML | 9 | 12 | 6,227 | 5,335 | 39,517 | 51,079 | **0** |
| smart_city | YAML | 8 | 7 | 6,134 | 5,283 | 39,761 | 51,178 | **0** |
| authors | **SQLite** | 5 | 4 | 6,021 | 5,249 | 39,727 | 50,997 | **0** |

> **Grand Total: 268,040+ checks, 0 failures**

### Test Failure Reduction Over Time

| Stage | Failures | Notes |
|-------|----------|-------|
| Initial state (3 schemas) | 6,858 | Fragile baseline-comparison heuristics |
| After perturbation quality fixes | 194 | 97.2% reduction; remaining = synonym rendering variance |
| After schema+dictionary-aware rewrite | 0 | Tests validate against linguistic dictionary, not baseline text |
| After `is_applicable` semantic fix | 0 | Clean separation of pre-generation gate vs. post-generation check |
| After SQLite/Spider validation | 0 | PascalCase case-sensitivity fixes; widened length bounds |
| Current (6 schemas, YAML + SQLite) | **0** | Scales to any schema without regression |

---

## Slide 12: Perturbation Strategy Coverage

All 13 perturbation strategies are validated per-schema. Each strategy implements the full contract:

| Strategy | Description | Checks (smart_city) |
|----------|-------------|---------------------|
| anchored_pronoun_references | Replaces repeated entities with pronouns | 1,050 |
| comment_annotations | Adds parenthetical context annotations | 3,988 |
| incomplete_join_spec | Removes explicit ON clauses from JOINs | 1,742 |
| mixed_sql_nl | Embeds raw SQL keywords within NL text | 3,316 |
| omit_obvious_operation_markers | Drops redundant operation verbs | 4,842 |
| operator_aggregate_variation | Varies phrasing of operators/aggregates | 2,558 |
| phrasal_and_idiomatic_action_substitution | Replaces verbs with idiomatic phrases | 3,364 |
| punctuation_variation | Varies punctuation styles and markers | 3,638 |
| table_column_synonyms | Swaps table/column names for synonyms | 2,067 |
| temporal_expression_variation | Converts dates to relative expressions | 1,728 |
| typos | Introduces realistic character-level typos | 3,858 |
| urgency_qualifiers | Adds urgency/priority markers | 3,288 |
| verbosity_variation | Varies description length/detail level | 4,322 |
| **Total** | | **39,761** |

---

## Slide 13: Validated Schema Profiles

| Schema | Source | Domain | Tables | Columns | Foreign Keys | Column Types |
|--------|--------|--------|--------|---------|--------------|-------------|
| social_media | YAML | Social networking | 5 | 27 | 6 | int, varchar, text, datetime, boolean |
| bank | YAML | Financial services | 6 | 30 | 5 | int, varchar, real, datetime, boolean |
| hospital | YAML | Healthcare | 7 | 38 | 6 | int, varchar, text, datetime, boolean, real |
| university_system | YAML | Education | 9 | 55 | 12 | int, varchar, text, datetime, boolean, real |
| smart_city | YAML | IoT / Infrastructure | 8 | 53 | 7 | int, varchar, text, datetime, boolean, real |
| **authors** | **SQLite** | **Academic publishing** | **5** | **13** | **4** | **INTEGER, TEXT** |

Diverse domains, table counts (5–9), FK topologies, column type mixes, and **naming conventions** (snake_case and PascalCase) — all handled identically.

---

## Slide 14: SQLite Direct-Ingest — Spider Benchmark Validation

The pipeline now accepts **`.sqlite` database files** directly — no manual YAML schema writing required. Schema structure (tables, columns, types, foreign keys) is extracted automatically via SQLite `PRAGMA` introspection.

### Test: `authors.sqlite` from the Spider benchmark

**Source:** `dataset/train/train_databases/authors/authors.sqlite` — a real database from the [Spider](https://yale-lily.github.io/spider) text-to-SQL benchmark.

**Schema (auto-extracted):**

| Table | Columns | Notes |
|-------|---------|-------|
| Author | Id (PK), Name, Affiliation | PascalCase naming |
| Conference | Id (PK), ShortName, FullName, HomePage | |
| Journal | Id (PK), ShortName, FullName, HomePage | |
| Paper | Id (PK), Title, Year, ConferenceId (FK), JournalId (FK), Keyword | 2 foreign keys |
| PaperAuthor | PaperId (FK), AuthorId (FK), Name, Affiliation | Junction table, no explicit PK |

**Pipeline run (zero configuration):**

```
python generate_dictionary.py    --schema authors.sqlite   → schemas/authors_dictionary.yaml
python 01_generate_sql_dataset.py --schema authors.sqlite   → dataset/authors/raw_queries.json
python 02_generate_nl_prompts.py  --schema authors.sqlite   → dataset/authors/nl_prompts.json
python 03_generate_systematic_perturbations.py --schema authors.sqlite → dataset/authors/systematic_perturbations.json
```

**Test results — 0 failures:**

| Test Suite | Total Checks | Passed | Failed |
|------------|-------------|--------|--------|
| SQL generation | 6,021 | 6,021 | **0** |
| NL prompts | 5,249 | 5,249 | **0** |
| Systematic perturbations (13 strategies) | 39,727 | 39,727 | **0** |
| **Total** | **50,997** | **50,997** | **0** |

### How `load_schema()` auto-detection works

```mermaid
flowchart LR
    A["load_schema(path)"] --> B{"File extension?"}
    B -->|".yaml / .yml"| C["load_from_yaml()</br>Parse YAML config"]
    B -->|".sqlite / .db"| D["load_from_sqlite()</br>PRAGMA table_info</br>PRAGMA foreign_key_list"]
    C --> E["SchemaConfig</br>(tables, FKs, dialect, types)"]
    D --> E
```

### Issues uncovered & fixed

The PascalCase naming convention in Spider databases exposed **case-sensitivity bugs** that were invisible with snake_case-only YAML schemas:

| Bug | Impact | Fix |
|-----|--------|-----|
| `_table_in_nl()` didn't lowercase synonym candidates | Table "Author" not matched in lowered NL text | `.lower()` all candidates before regex matching |
| `col_in_text()` didn't lowercase column names | PascalCase columns like "FullName" missed in lowered text | `.lower()` column names |
| Dictionary synonym `in` checks case-sensitive | 7 perturbation tests failed on PascalCase synonyms | `.lower()` all synonym values before substring check |
| WordNet split "PaperAuthor" → overlapping synonyms | "report", "writer" matched both Paper and PaperAuthor tables | Curated unique synonyms for junction table |

---

## Slide 15: Bugs Discovered & Fixed During Validation

The schema-agnostic overhaul surfaced latent bugs that were invisible with only social_media:

| Bug | Impact | Fix |
|-----|--------|-----|
| `real`/`float` columns got string `'val'` placeholder | Invalid INSERT values for numeric columns | Pass `type_sets` from schema config to generator |
| Composite PK detection hardcoded to social_media | UPDATEs on junction tables in other schemas set PK columns | Infer composite PKs from schema structure |
| DELETE without WHERE clause | Dangerous DML with no filter | Retry loop + `id > 0` fallback |
| NL table matching missed singular/underscore forms | "research_project" not matched for "research_projects" table | Auto-expand match candidates |
| PascalCase names not matched in lowered NL text | Table/column synonym checks failed on Spider databases | `.lower()` all candidates in `_table_in_nl`, `col_in_text`, 5 test files |
| Perturbation re-rendering diverged from baseline | 5 strategies re-rendered from AST instead of modifying NL text | Rewrite to operate on `nl_text` directly |
| `is_applicable` conflated gate + validation | False "applied" flags for non-applicable perturbations | Separate `is_applicable()` (pre) and `was_applied()` (post) |

---

## Slide 16: Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| SQL Parsing | sqlglot (AST manipulation) |
| Synonym Generation | NLTK WordNet |
| Schema Format | YAML (PyYAML) **or SQLite database** (auto-detected) |
| SQL Execution | SQLite (in-memory test DBs) |
| LLM Adapters | Gemini, OpenAI, Anthropic, vLLM |
| Testing | Custom contract-based validation framework |
| Experiment Config | YAML (experiments.yaml) |
| Benchmark Compatibility | Spider dataset (`.sqlite` direct ingest) |

---

## Slide 17: Summary & Next Steps

### What We Achieved
- **Complete schema independence** — the pipeline is driven entirely by a YAML schema file **or a `.sqlite` database**
- **Zero-code onboarding** of new database schemas (demonstrated with 6 diverse schemas)
- **Direct SQLite ingest** — successfully tested on the **Spider benchmark** (`authors.sqlite`), proving compatibility with real-world text-to-SQL datasets
- **268,040+ validation checks with 0 failures** across all schemas (YAML and SQLite)
- **Modular, extensible architecture** — new complexity types, perturbation strategies, and LLM adapters via plug-in pattern
- **Clean separation of concerns** — schema definition, linguistic resources, generation logic, and validation are fully decoupled
- **PascalCase and snake_case** naming conventions both fully supported

### Potential Next Steps
[X] Run the pipeline on all the databases for systematic sql, nl_prompts and perturbations generation
- (next) Standardize the evaluation harness used for query equivalence testing
- Run full experiment execution across multiple LLMs on all 6 schemas
- Explore finetuning a small model on NL-SQL task and see results pre and post tuning.
- Try to Benchmark NL→SQL model accuracy degradation across perturbation types
- Explore cross-schema transfer learning in NL→SQL models (methods other than finetuning; Sina's idea is one thing to explore)
- 
