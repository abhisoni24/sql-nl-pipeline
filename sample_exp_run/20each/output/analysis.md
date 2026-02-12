# Experiment Analysis: SQL Generation Robustness

**Date:** February 4, 2026
**DataSet:** N=10,650 queries (across 6 models)
**Key Findings:** While top-tier models (GPT-4o, Claude 4.5) show high resilience (>90% accuracy), open weights models (Llama 3.1) and smaller distilled models (Gemini Flash Lite) exhibit sharp degradation in two specific areas: **implicit joins** and **complex temporal logic**.

## 1. Executive Summary

| Metric                            | Top Tier (GPT-4o, Claude) | Mid Tier (Gemini, Qwen, DeepSeek) | Low Tier (Llama 3.1)    |
| :-------------------------------- | :------------------------ | :-------------------------------- | :---------------------- |
| Metric                            | Top Tier (GPT-4o, Claude) | Mid Tier (Gemini, Qwen, DeepSeek) | Low Tier (Llama 3.1)    |
| :---                              | :---                      | :---                              | :---                    |
| **Overall Baseline Accuracy** [1] | >92%                      | ~85%                              | ~80%                    |
| **Join Accuracy** [2]             | ~85%                      | ~65-80%                           | **30% (Critical Fail)** |
| **Formatting Resilience** [3]     | High (>90%)               | High (>85%)                       | High (>80%)             |
| **Logic Resilience** [4]          | High                      | Moderate                          | Low                     |
| **Implicit Join Accuracy** [5]    | ~23%                      | ~18-26%                           | ~15%                    |

#### Metric Definitions

1.  **Overall Baseline Accuracy:** Mean accuracy across all 7 complexity categories (Simple, Advanced, Join, Union, Insert, Update, Delete) on the unperturbed dataset.
2.  **Join Accuracy:** Specific accuracy on the `join` complexity category (explicit multi-table queries).
3.  **Formatting Resilience:** Mean accuracy on `typos`, `punctuation_variation`, and `verbosity_variation` perturbations. High scores indicate robustness to noisy input.
4.  **Logic Resilience:** Qualitative assessment based on performance in `compound` complexity and `temporal_expression_variation`. Captures ability to handle semantic complexity.
5.  **Implicit Join Accuracy:** Accuracy on the `incomplete_join_spec` perturbation, where the relationship between tables is implied (e.g., "users and their posts") rather than explicit.

**The "Join Cliff":** Llama 3.1 fails catastrophically on vanilla joins (30% accuracy), often defaulting to single-table selects or invalid syntax. This indicates a fundamental lack of schema-reasoning capability in the base 8B model.

**The "Implicit" Barrier:** All models, regardless of size, failed (>75% failure rate) on `incomplete_join_spec` queries where the join condition was not explicitly stated (e.g., "users and their posts"). This suggests that current LLMs rely heavily on explicit "JOIN" keywords in the prompt.

---

## 2. Complexity Analysis

### Vanilla Performance by Complexity Type

accuracy (%) sorted by complexity.

| complexity   | Qwen     | Claude   | DeepSeek | Gemini   | GPT-4o   | Llama 3.1 |
| :----------- | :------- | :------- | :------- | :------- | :------- | :-------- |
| **delete**   | 100.0    | 100.0    | 85.0     | 100.0    | 95.0     | 100.0     |
| **insert**   | 95.0     | 100.0    | 100.0    | 70.0     | 95.0     | 100.0     |
| **update**   | 95.0     | 90.0     | 95.0     | 90.0     | 95.0     | 95.0      |
| **simple**   | 95.0     | 95.0     | 95.0     | 95.0     | 95.0     | 85.0      |
| **union**    | 100.0    | 100.0    | 80.0     | 75.0     | 95.0     | 60.0      |
| **advanced** | 90.0     | 95.0     | 80.0     | 95.0     | 90.0     | 90.0      |
| **join**     | **80.0** | **70.0** | **65.0** | **75.0** | **85.0** | **30.0**  |

### 🔍 Deep Dive: The Llama Join Failure

Llama 3.1 struggles to construct valid Multi-Table SELECT statements. Instead of joining, it often hallucinates simple subqueries or invalid syntax.

**Example: Failed Join (Llama 3.1)**

> **Prompt:** "Fetch me all columns from comments and users where id is greater than 153."
>
> **Gold SQL:**
>
> ```sql
> SELECT * FROM comments AS c1 JOIN users AS u1 ON c1.user_id = u1.id WHERE c1.id > 153
> ```
>
> **Llama 3.1 Generated:**
>
> ```sql
> SELECT * FROM comments WHERE id > 153
> ```
>
> _Analysis:_ The model completely ignored the second table (`users`) and the join condition, treating it as a single-table select.

---

## 3. Perturbation Analysis

### Category Accuracy (Systematic vs LLM)

Impact of natural language variations on model performance.

| Category           | Systematic Acc | LLM Acc | Delta    | Insight                                       |
| :----------------- | :------------- | :------ | :------- | :-------------------------------------------- |
| **Typos**          | ~88%           | ~90%    | +2%      | High resilience to noise.                     |
| **Punctuation**    | ~90%           | ~93%    | +3%      | Models ignore punctuation changes.            |
| **Verbosity**      | ~90%           | ~90%    | 0%       | Robust to extra words.                        |
| **Temporal**       | **90%**        | **70%** | **-20%** | LLM temporal logic is significantly harder.   |
| **Operators**      | **60%**        | **75%** | **+15%** | Systematic operators are harder/more obscure. |
| **Implicit Joins** | **20%**        | **20%** | 0%       | **Universal Failure.**                        |

### 🔍 Deep Dive: Temporal Divergence

Systematic perturbations use simple substitutions (e.g., date formats). LLM perturbations introduce logic changes (relative time) that trip up models.

**Example: Temporal Failure (Deepseek)**

> **Prompt:** "update content to 'Updated text 51' for the post where posts.posted_at is less than three weeks ago."
>
> **Gold SQL:** `WHERE posts.posted_at < DATETIME('now', '-18 days')` (Note: Logic is 'less than 3 weeks ago' meaning _older_ than 3 weeks? Or within the last 3 weeks? Gold interprets as 'older/before' < 18 days.)
>
> **Generated:** `WHERE posted_at >= DATETIME('now', '-3 weeks')`
>
> _Analysis:_ The model interpreted "less than three weeks ago" as "within the last 3 weeks" (`>=`), whereas the Gold SQL (and likely the systematic logic) defined it as strict inequality on the timeline. This ambiguity in NL causes valid but "incorrect" SQL generation relative to the strict Gold standard.

### 🔍 Deep Dive: Operator Confusion

Complex compound statements often confuse models into merging clauses inappropriately.

**Example: Operator Failure (Deepseek)**

> **Prompt:** "Run a check for... country_code like '%a%', combined with (removing duplicates) Run a check for... signup_date at most 10 days ago"
>
> **Gold SQL:** `SELECT ... WHERE ... LIKE '%a%' UNION SELECT ... WHERE ...`
>
> **Generated:** `SELECT DISTINCT ... WHERE ... LIKE '%a%' AND ... >= ...`
>
> _Analysis:_ The model saw "combined with (removing duplicates)" and interpreted it as a single `SELECT DISTINCT` with `AND` conditions, effectively intersecting the sets instead of Unioning them. This shows a semantic parsing failure.

---

## 4. Recommendations

1.  **Explicit Joins Required:** For current generation models (including Llama 3.1 and small distilled models), applications _must_ ensure prompt engineering explicitly mentions "join" or "link" tables. Implicit instructions ("users and their posts") are unreliable.
2.  **Temporal Ambiguity Guardrails:** Natural language dates ("last week", "3 days ago") are highly ambiguous (inclusive vs exclusive, strictly before vs within). Systems should resolve these to concrete dates _before_ sending to the SQL LLM.
3.  **Llama 3.1 Warning:** The 8B model is not production-ready for multi-table SQL generation without few-shot prompting or fine-tuning, given its 30% baseline accuracy on Joins.
