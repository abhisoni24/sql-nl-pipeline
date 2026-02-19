# NL Prompt Test Suite — Walkthrough

## Overview

This comprehensive baseline test suite validates Natural Language (NL) rendering for SQL queries. It ensures that the generated prompts are human-readable, contextually accurate, and free of "leaked" technical SQL syntax.

**Location:** `pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py`

---

## Test Coverage (62 Named Checks)

The suite executes over 50,000 individual validations across 3,500 records, categorized by query complexity:

| Category | Checks | Validation Focus |
| --- | --- | --- |
| **Structural** (Global) | 1–8 | NL prompt presence, no Python `repr` leaks (e.g., `[None]`), no control chars, starts with intent verb, no raw SQL keywords. |
| **SELECT Fidelity** | 9–14 | `SELECT *` mapped to "all columns," specific columns present, `WHERE`/`ORDER BY`/`LIMIT` logic correctly reflected in NL. |
| **Simple** | 15–17 | Source table mentioned; no `JOIN`/`UNION` keyword leaks. |
| **Join** | 18–24 | Both tables present, INNER FK coupling phrases, LEFT/RIGHT/FULL join optionality signals preserved. |
| **Advanced / Subquery** | 25–35 | `IN` semantics, outer/inner tables present, inner `WHERE` conditions reflected, no `derived_table` or `inner_` aliases. |
| **Advanced / Self-Join** | 36–38 | Table mentioned, join structural signal present, no false `WHERE` flags. |
| **Advanced / Exists** | 39–42 | Existence/Negation language (e.g., "where there is no..."), correlated conditions reflected. |
| **Union** | 43–49 | "Combined with" connector, duplicate handling (ALL vs. Distinct), both halves of the query present in one sentence. |
| **DML (I/U/D)** | 50–62 | Action verbs (Insert/Update/Delete), target tables, column/value matching, `WHERE` presence for updates and deletes. |

---

## Key Design Decisions

* **Smart Synonym Matching:** Uses word-boundary and context matching to prevent false positives. For example, it won't flag the `users` table as "present" just because it found `user_id` or the string `'user'`.
* **Calibrated Filter Detection:** ON conditions in self-joins (like `equals` or `greater than`) are permitted, as only the literal SQL keyword `WHERE` is banned from the NL output.
* **Linguistic Flexibility:** Recognized that "from" is a valid English preposition; it is no longer flagged as a SQL keyword leak in simple queries.
* **DML Context:** Action words like **Delete**, **Update**, and **Insert** are permitted at the start of sentences for DML operations.

---

## Calibration Iterations

The suite was refined through several iterations to eliminate 2,286 initial false positives caused by overly-strict rules:

| Issue | Resolution |
| --- | --- |
| **Missing Intent Verbs** | Expanded the `SELECT_INTENT_VERBS` dictionary to include natural variations. |
| **Keyword Over-blocking** | Restricted keyword checks to structural elements (`FROM`, `WHERE`, `JOIN`) rather than common English words. |
| **Self-join False Positives** | Updated logic to distinguish between SQL `WHERE` clauses and natural language comparison indicators. |
| **Table/Column Collisions** | Added exclusions for underscores, quotes, and `LIKE` patterns to ensure table name matches are genuine. |

---

## Final Results

The suite is currently fully calibrated with a 100% pass rate on the baseline dataset:

* **Records Loaded:** 3,500
* **Total Checks:** 51,786
* **Passed:** 51,786
* **Failed:** 0

---

## Usage

```bash
# Run against the default dataset
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py

# Run against a custom file
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py --input path/to/file.json

# Verbose mode (logs failures immediately)
python pipeline_tests/generation_process/nl_prompt/test_nl_prompt.py -v

```

Would you like me to help you integrate these checks into a CI/CD pipeline configuration (like GitHub Actions)?