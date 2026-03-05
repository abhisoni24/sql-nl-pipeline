# Two-Pass Rendering Architecture (SQL→NL)

This document explains the technical implementation and design philosophy of the two-pass rendering system used to generate natural language (NL) prompts from SQL Abstract Syntax Trees (ASTs).

---

## 1. What is "IR"?

In this system, **IR** stands for **Intermediate Representation**. 

Instead of jumping directly from a SQL AST to a final sentence, the system first generates an **IR Template**. This template is a "structural skeleton" that contains placeholders (tokens) instead of final words. This decouples the **logical structure** of a query from its **linguistic expression**.

---

## 2. The Two-Pass Flow

The process is divided into two distinct phases to ensure the engine remains schema-agnostic while producing diverse output.

### Pass 1: IR Generation (The "Structural" Pass)
- **Module:** `SQLToNLRenderer.render_template(ast)`
- **Behavior:** The system traverses the SQL AST (parsed by `sqlglot`). It handles the complex logic of SQL—deciding the order of clauses, identifying JOIN relationships, and handling subqueries. It does not pick specific words; instead, it emits tokens.
- **Token Format:** `[TYPE:value]` (e.g., `[TABLE:users]`, `[VERB:get]`, `[OP:gt]`).
- **Knowledge:** Knows SQL structure but *doesn't* know schema-specific synonyms.

### Pass 2: Dictionary Resolution (The "Linguistic" Pass)
- **Module:** `TemplateResolver.resolve(template, dictionary)`
- **Behavior:** This pass takes the string of IR tokens and a `LinguisticDictionary` (loaded from a YAML like `bank_dictionary.yaml`). It performs a regex-based search for all `[TYPE:value]` patterns and replaces them with a random synonym from the dictionary.
- **Knowledge:** Knows synonyms but *doesn't* care about SQL logic.

---

## 3. Example Execution Flow

**Input SQL:** `SELECT email FROM users WHERE signup_date > '2024-01-01'`

| Stage | Data Representation | Responsible Module |
| :--- | :--- | :--- |
| **0. Input** | `SELECT email FROM users WHERE signup_date > '2024-01-01'` | `02_generate_nl_prompts.py` |
| **1. AST** | `(SELECT expressions=[email], from=[users], where=(GT ...))` | `sqlglot` |
| **2. IR Template** | `"[VERB:get] the [COL:email] [CONN:from] [TABLE:users] [CONN:where] [COL:signup_date] [OP:gt] [VAL:'2024-01-01']"` | `SQLToNLRenderer` |
| **3. Final NL** | `"Retrieve the contact address from symbols for which the join date is after '2024-01-01'"` | `TemplateResolver` |

---

## 4. Key Architectural Benefits

### 1. Zero-Code Schema Onboarding
Because the `SQLToNLRenderer` only emits abstract `[TABLE:name]` tokens, you can swap the dictionary (e.g., from `social_media` to `hospital`) without changing a single line of Python code. 

### 2. Combinatorial Diversity
A single IR template can generate thousands of unique NL prompts. If a query has 5 tokens and each token has 4 synonyms, the system can produce $4^5 = 1024$ unique variations of that single query's baseline NL.

### 3. Verification Precision
Our validation test suites use the IR template to ensure that a table or column was *actually* mentioned in the NL, even if it was replaced by a highly idiomatic synonym that a simple regex would miss.
