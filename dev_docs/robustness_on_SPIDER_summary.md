# Spider Robustness Test — Summary Report

**Date:** February 26, 2026
**Pipeline:** SQL → NL → Perturbation (schema-agnostic)
**Test script:** `test_dbs/spider/robustness_test.py`
**Configuration:** 20 queries per complexity level (7 types) = 140 queries/database

---

## Final Results

| Metric | Value |
|---|---|
| **Databases tested** | 20 |
| **Databases passing** | **20 / 20 (100%)** |
| **SQL checks** | 48,569 — 0 failures (100.000%) |
| **NL checks** | 41,780 — 0 failures (100.000%) |
| **Perturbation checks** | 309,004 — 0 failures (100.000%) |
| **Grand total** | **399,353 checks — 0 failures (100.000%)** |

---

## Per-Database Breakdown

| # | Database | SQL Checks | NL Checks | Pert Checks | Total | Status |
|---|---|---|---|---|---|---|
| 1 | european_football_1 | 2,480 | 2,130 | 15,166 | 19,776 | ✅ |
| 2 | sales_in_weather | 2,517 | 2,118 | 15,666 | 20,301 | ✅ |
| 3 | craftbeer | 2,376 | 2,065 | 15,668 | 20,109 | ✅ |
| 4 | soccer_2016 | 2,381 | 2,071 | 15,398 | 19,850 | ✅ |
| 5 | restaurant | 2,397 | 2,069 | 15,182 | 19,648 | ✅ |
| 6 | movie | 2,586 | 2,141 | 15,260 | 19,987 | ✅ |
| 7 | olympics | 2,317 | 2,034 | 15,912 | 20,263 | ✅ |
| 8 | language_corpus | 2,367 | 2,061 | 15,386 | 19,814 | ✅ |
| 9 | app_store | 2,551 | 2,160 | 15,083 | 19,794 | ✅ |
| 10 | sales | 2,395 | 2,089 | 15,332 | 19,816 | ✅ |
| 11 | video_games | 2,304 | 2,011 | 16,011 | 20,326 | ✅ |
| 12 | image_and_language | 2,384 | 2,057 | 15,367 | 19,808 | ✅ |
| 13 | software_company | 2,467 | 2,108 | 15,788 | 20,363 | ✅ |
| 14 | authors | 2,411 | 2,079 | 15,626 | 20,116 | ✅ |
| 15 | movies_4 | 2,427 | 2,079 | 15,411 | 19,917 | ✅ |
| 16 | social_media | 2,490 | 2,122 | 15,271 | 19,883 | ✅ |
| 17 | human_resources | 2,514 | 2,143 | 15,177 | 19,834 | ✅ |
| 18 | regional_sales | 2,327 | 2,028 | 15,245 | 19,600 | ✅ |
| 19 | computer_student | 2,354 | 2,053 | 15,334 | 19,741 | ✅ |
| 20 | works_cycles | 2,524 | 2,162 | 15,721 | 20,407 | ✅ |

---

## Progress History

| Round | DBs Passing | SQL Fails | NL Fails | Pert Fails | Total Fails | Pass Rate |
|---|---|---|---|---|---|---|
| Initial (pre-fixes) | 6 / 20 | 70 | 203 | 45 | 318 | 99.918% |
| After Bug Fixes 1–8 | 9 / 20 | 0 | 16 | 47 | 63 | 99.984% |
| After RC1–RC4 Fixes | 19 / 20 | 0 | 2 | 0 | 2 | 99.999% |
| **Final** | **20 / 20** | **0** | **0** | **0** | **0** | **100.000%** |

---

## Bugs Fixed

### Phase 1 — Infrastructure bugs (8 fixes)

| # | Bug | File(s) | Impact |
|---|---|---|---|
| 1 | UPDATE/INSERT multi-word columns unquoted | `src/core/generator.py` | SQL generation errors |
| 2 | CLI arg `--num` instead of `-n` | `test_dbs/spider/robustness_test.py` | Batch runner crash |
| 3 | Hardcoded `FOREIGN_KEYS` in NL renderer + 8 perturbation strategies + 2 pipeline scripts | `src/core/nl_renderer.py`, perturbation strategies, pipeline scripts | Non-social_media schemas failed |
| 4 | Test validator stripping quotes incorrectly | `pipeline_tests/` | False test failures |
| 5 | `_render_column` missing `exp.Identifier` handling | `src/core/nl_renderer.py` | Rendering crashes |
| 6 | UPDATE SET regex overly strict | Test validator | False test failures |
| 7 | NULL FK target columns from SQLite PRAGMA | `src/core/schema_loader.py` | Schema loading crash for 11 DBs |
| 8 | None guard in test_sql_generation | `pipeline_tests/` | Crash on missing FK |

### Phase 2 — Root cause analysis of 63 remaining failures (4 fixes)

| # | Root Cause | Fails | File(s) | Fix |
|---|---|---|---|---|
| RC1 | `synonym_used`: alias-prefixed tokens (`a1.record`) defeated novel-word detection | 33 | `test_table_column_synonyms.py` | Added `_strip_alias()` helper + lowercase synonym matching |
| RC2 | `both_tables_present`: substring matching (`game` matched inside `game_platform`) | 11 | `test_incomplete_join_spec.py` | Word-boundary regex (`re.search(r'\b...\b')`) |
| RC3 | `simple_only_one_table`: column names collided with table names (`words` column vs `words` table) | 16 | `test_nl_prompt.py` | Column-name exclusion before table-name collision check |
| RC4 | Naive `table_nl.rstrip('s')` produced invalid singulars (`breweries`→`brewerie`, `Address`→`Addres`) | 3 | `src/core/nl_renderer.py` | Added `_singularize()` method with `-ies`, `-ses`, `-zes`, `-xes`, `-ches`, `-shes` rules |

### Phase 3 — Second-order fixes (3 fixes)

| # | Root Cause | Fails | File(s) | Fix |
|---|---|---|---|---|
| A | `_table_in_nl` expansion used naive `[:-1]` singularization (`OBJ_CLASSES`→`OBJ_CLASSE`) | 13 | `test_nl_prompt.py` | Added `_singularize_for_test()` mirroring renderer logic |
| B | Alias-qualified column references (`S1.Region`) matched as table names | 3 | `test_nl_prompt.py` | Dot-prefix rejection in `_table_in_nl` |
| C | WordNet table synonyms collided with other table names (`position` synonym for `location` vs `position` table) | 8 | `src/core/dictionary_builder.py` | Cross-table-name synonym filtering at dictionary generation time |

---

## Test Architecture

Each database goes through the full pipeline:

```
SQLite DB
  → generate_dictionary.py       (WordNet-based synonym dict)
  → 01_generate_sql_dataset.py   (140 SQL queries: 20 × 7 complexity types)
  → 02_generate_nl_prompts.py    (NL rendering with two-pass IR tokens)
  → 03_generate_systematic_perturbations.py  (13 perturbation strategies)
  → TEST: test_sql_generation.py        (~2,400 checks per DB)
  → TEST: test_nl_prompt.py             (~2,100 checks per DB)
  → TEST: run_all_perturbation_tests.py (~15,400 checks per DB)
```

### Test Suites

| Suite | Checks per DB | What It Validates |
|---|---|---|
| **SQL** | ~2,400 | Syntactic validity, table/column existence, FK consistency, quote correctness |
| **NL** | ~2,100 | Intent verb, table mention, column coverage, clause reflection, no SQL leakage, length sanity |
| **Perturbation** | ~15,400 | Per-strategy invariants across 13 perturbation types (synonym substitution, join removal, clause omission, etc.) |

---

## Database Diversity

The 20 Spider databases cover a broad range of schema characteristics:

| Characteristic | Examples |
|---|---|
| Multi-word table names | `Sales Team`, `Sales Orders`, `game_platform` |
| Underscore-heavy identifiers | `IMG_OBJ_ATT`, `ATT_CLASSES`, `OBJ_CLASSES` |
| Column-table name collisions | `words` column in `langs` table vs `words` table |
| Substring table overlaps | `game` table + `game_platform` table |
| Irregular plurals | `breweries`, `Address`, `countries` |
| CamelCase identifiers | `BusinessEntityAddress`, `SalesTeamID` |
| Space-in-name tables | `Sales Team`, `Sales Orders` |
| Deep FK chains | `works_cycles` (15 tables), `video_games` (8 tables) |

---

## Reproduction

```bash
conda activate sqlGen
cd /path/to/sql-nl

# Full batch test (all 20 databases)
python test_dbs/spider/robustness_test.py

# Results saved to:
#   test_dbs/spider/spider_batch_results.csv

# Summary stats:
python test_dbs/spider/_summary.py
```