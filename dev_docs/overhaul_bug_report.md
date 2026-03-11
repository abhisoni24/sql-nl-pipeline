# Overhaul Bug Report: Schema-Agnostic Migration

This document provides a detailed technical analysis of the bugs discovered and resolved during the migration of the SQL→NL pipeline from a hardcoded social-media prototype to a schema-agnostic framework.

---

## 1. DML: Dangerous Unfiltered DELETE/UPDATE

### Failure
In the initial generator, `generate_where()` was probabilistic. If the random number generator didn't pick any filterable columns, it returned `None`.
*   **Resulting SQL:** `DELETE FROM posts;` or `UPDATE users SET name = 'val';`
*   **Impact:** Destructive DML operations that wipe or corrupt entire tables during equivalence testing.

### Fix
Implemented a robust fallback mechanism in `src/core/generator.py`.
*   **Retry Loop:** The generator now attempts to find a valid WHERE clause 10 times.
*   **Defensive Fallback:** If no filter is found, it defaults to a primary key range check (e.g., `WHERE id > 0`).

**Code Example:**
```python
# Before
where = self.generate_where(table)
delete_expr = exp.delete(table).where(where) # where could be None

# After
where = None
for _ in range(10):
    where = self.generate_where(table)
    if where: break

if where:
    delete_expr = delete_expr.where(where)
else:
    # Fallback to PK range to prevent "truncate" behavior
    delete_expr = delete_expr.where(exp.GT(this=exp.column('id'), 
                                           expression=exp.Literal.number(0)))
```

---

## 2. Type Safety: String Placeholders in Numeric Columns

### Failure
The original generator used a generic `'val'` string literal for any column it couldn't specifically identify.
*   **Resulting SQL:** `INSERT INTO loans (amount) VALUES ('val');`
*   **Impact:** `DataTypeMismatch` errors in SQLite.

### Fix
The `SchemaConfig` now exports `type_sets` (numeric, text, date, boolean). The generator uses these sets to pick the correct `sqlglot.exp` literal type.

**Example Fix:**
```python
# In generate_insert()
col_type = self._get_column_type(table, col)
if col_type in self.numeric_types:
    values.append(exp.Literal.number(random.randint(1, 1000)))
elif col_type in self.text_types:
    values.append(exp.Literal.string("sample_text"))
```

---

## 3. Schema Logic: Hardcoded Junction Tables

### Failure
The logic for identifying primary keys for UPDATE/DELETE was hardcoded to the `social_media` schema (specifically looking for `follows` and `likes`).
*   **Impact:** In a new schema like `bank`, a junction table between `customers` and `accounts` would not be recognized as having a composite primary key, leading to SQL generation errors.

### Fix
The `SchemaLoader` now dynamically identifies junction tables by looking for tables that lack an `id` column and treating their foreign key columns as the composite primary key.

---

## 4. Linguistic: Case-Sensitivity in Spider Databases

### Failure
The Spider benchmark (e.g., `authors.sqlite`) uses **PascalCase** for tables (`Author`, `PaperAuthor`) and columns (`FullName`). The validation logic used lowercase regex and string comparisons.
*   **Impact:** 1,137 test failures where the validator couldn't find "Author" in the generated NL text "Get the list of authors...".

### Fix
Standardized case-normalization across the entire validation stack.
1.  **Lowercasing Candidates:** `_table_in_nl()` now lowercases all synonym candidates before running the regex.
2.  **Lowercasing Schema:** Column name checks in `common.py` now apply `.lower()` to the schema data.

**Example:**
```python
# Before
if any(syn in nl_text for syn in synonyms): ...

# After
nl_text_l = nl_text.lower()
if any(syn.lower() in nl_text_l for syn in synonyms): ...
```

---

## 5. Perturbation Architecture: Re-rendering Divergence

### Failure
Some perturbation strategies (like `verbosity_variation`) were re-running the `SQLToNLRenderer` on the AST with new flags. 
*   **Impact:** This "re-rendering" reset the internal random state of other perturbations. If a query already had a `typo`, re-rendering the verbosity would produce a clean string, effectively deleting the previous perturbation.

### Fix
Refactored all "post-processing" layer perturbations to operate strictly on the **already rendered string**, rather than the AST. This allows perturbations to be stacked (e.g., an Urgent prompt with a typo and high verbosity) without destructive interference.

---

## 6. Tokenization: Singular/Plural Matching

### Failure
A table named `research_projects` would fail to match the word "research project" in the NL text because of the missing "s".
*   **Impact:** `TablePresenceTest` failures.

### Fix
The `LinguisticDictionary` now automatically generates singular versions of plural table names (and vice versa) using NLTK, ensuring that "projects" matches "project" and "research_projects" matches "research project".
