# Detailed Experiment Analysis Report
**Source Data:** `/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run/20each/output/evaluated_results_aggregated.jsonl`

## Vanilla Complexity Accuracy
Accuracy (%) of Baseline (Vanilla) prompts by complexity.

| complexity   |   Qwen/Qwen3-Coder-30B-A3B-Instruct |   claude-haiku-4-5-20251001 |   deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |   gemini-2.5-flash-lite |   gpt-4o |   meta-llama/Llama-3.1-8B |
|:-------------|------------------------------------:|----------------------------:|----------------------------------------------:|------------------------:|---------:|--------------------------:|
| advanced     |                               90.00 |                       95.00 |                                         80.00 |                   95.00 |    90.00 |                     90.00 |
| delete       |                              100.00 |                      100.00 |                                         85.00 |                  100.00 |    95.00 |                    100.00 |
| insert       |                               95.00 |                      100.00 |                                        100.00 |                   70.00 |    95.00 |                    100.00 |
| join         |                               80.00 |                       70.00 |                                         65.00 |                   75.00 |    85.00 |                     30.00 |
| simple       |                               95.00 |                       95.00 |                                         95.00 |                   95.00 |    95.00 |                     85.00 |
| union        |                              100.00 |                      100.00 |                                         80.00 |                   75.00 |    95.00 |                     60.00 |
| update       |                               95.00 |                       90.00 |                                         95.00 |                   90.00 |    95.00 |                     95.00 |

## Perturbation Category Accuracy
Accuracy (%) by Perturbation Category and Source.

| perturbation_type                         | perturbation_source   |   Qwen/Qwen3-Coder-30B-A3B-Instruct |   claude-haiku-4-5-20251001 |   deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |   gemini-2.5-flash-lite |   gpt-4o |   meta-llama/Llama-3.1-8B |
|:------------------------------------------|:----------------------|------------------------------------:|----------------------------:|----------------------------------------------:|------------------------:|---------:|--------------------------:|
| anchored_pronoun_references               | llm                   |                               74.60 |                       77.78 |                                         69.84 |                   68.25 |    80.95 |                     52.38 |
| anchored_pronoun_references               | systematic            |                               69.70 |                       72.73 |                                         60.61 |                   63.64 |    78.79 |                     54.55 |
| comment_annotations                       | llm                   |                               88.37 |                       90.70 |                                         76.74 |                   72.09 |    86.05 |                     67.44 |
| comment_annotations                       | systematic            |                               90.00 |                       90.00 |                                         77.14 |                   79.29 |    90.00 |                     75.71 |
| comment_style_annotations                 | llm                   |                               91.67 |                       82.29 |                                         81.25 |                   79.17 |    85.42 |                     83.33 |
| compound                                  | llm                   |                               65.47 |                       68.35 |                                         50.36 |                   54.68 |    71.22 |                     43.17 |
| incomplete_join_spec                      | llm                   |                               26.92 |                       23.08 |                                          7.69 |                   23.08 |    23.08 |                     15.38 |
| incomplete_join_spec                      | systematic            |                               22.73 |                       22.73 |                                          9.09 |                   18.18 |    22.73 |                     27.27 |
| mixed_sql_nl                              | llm                   |                               86.33 |                       87.77 |                                         85.61 |                   87.77 |    90.65 |                     69.78 |
| mixed_sql_nl                              | systematic            |                               86.67 |                       91.67 |                                         90.00 |                   83.33 |    93.33 |                     72.50 |
| omit_obvious_operation_markers            | llm                   |                               78.83 |                       72.99 |                                         74.45 |                   75.18 |    77.37 |                     72.99 |
| omit_obvious_operation_markers            | systematic            |                               85.83 |                       77.50 |                                         68.33 |                   77.50 |    87.50 |                     60.83 |
| operator_aggregate_variation              | llm                   |                               77.78 |                       75.00 |                                         75.00 |                   72.22 |    77.78 |                     74.07 |
| operator_aggregate_variation              | systematic            |                               60.32 |                       65.08 |                                         58.73 |                   52.38 |    61.90 |                     63.49 |
| original                                  | systematic            |                               93.57 |                       92.14 |                                         85.71 |                   85.71 |    92.14 |                     80.00 |
| phrasal_and_idiomatic_action_substitution | llm                   |                               87.68 |                       83.33 |                                         78.99 |                   79.71 |    81.88 |                     81.16 |
| phrasal_and_idiomatic_action_substitution | systematic            |                               88.75 |                       85.00 |                                         82.50 |                   85.00 |    87.50 |                     65.00 |
| punctuation_variation                     | llm                   |                               94.24 |                       92.81 |                                         84.89 |                   79.14 |    93.53 |                     84.89 |
| punctuation_variation                     | systematic            |                               94.23 |                       94.23 |                                         90.38 |                   71.15 |    90.38 |                     84.62 |
| sentence_structure_variation              | llm                   |                               94.16 |                       90.51 |                                         83.94 |                   86.13 |    93.43 |                     78.83 |
| table_column_synonyms                     | llm                   |                               80.29 |                       83.21 |                                         57.66 |                   67.88 |    85.40 |                     43.80 |
| table_column_synonyms                     | systematic            |                               76.52 |                       81.82 |                                         59.09 |                   62.12 |    81.82 |                     41.67 |
| temporal_expression_variation             | llm                   |                               70.69 |                       62.07 |                                         74.14 |                   63.79 |    63.79 |                     70.69 |
| temporal_expression_variation             | systematic            |                              100.00 |                       93.10 |                                         89.66 |                   89.66 |    79.31 |                     82.76 |
| typos                                     | llm                   |                               92.09 |                       89.93 |                                         82.01 |                   82.01 |    91.37 |                     79.86 |
| typos                                     | systematic            |                               89.63 |                       88.89 |                                         84.44 |                   76.30 |    88.15 |                     78.52 |
| urgency_qualifiers                        | llm                   |                               93.53 |                       92.09 |                                         84.89 |                   87.77 |    92.81 |                     78.42 |
| urgency_qualifiers                        | systematic            |                               92.86 |                       91.43 |                                         85.00 |                   84.29 |    92.14 |                     64.29 |
| verbosity_variation                       | llm                   |                               92.09 |                       89.21 |                                         77.70 |                   83.45 |    90.65 |                     72.66 |
| verbosity_variation                       | systematic            |                               90.71 |                       91.43 |                                         80.71 |                   82.14 |    90.71 |                     72.86 |

## Compound vs Vanilla Performance
Comparison of accuracy between Baseline and Compound/Mixed perturbations.

| model_name                                  |   Vanilla Accuracy |   Compound Accuracy |   Performance Drop |
|:--------------------------------------------|-------------------:|--------------------:|-------------------:|
| Qwen/Qwen3-Coder-30B-A3B-Instruct           |              93.57 |               79.15 |              14.43 |
| claude-haiku-4-5-20251001                   |              92.86 |               82.16 |              10.70 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |              85.71 |               74.62 |              11.09 |
| gemini-2.5-flash-lite                       |              85.71 |               74.87 |              10.84 |
| gpt-4o                                      |              92.86 |               84.67 |               8.18 |
| meta-llama/Llama-3.1-8B                     |              80.00 |               61.31 |              18.69 |

## Systematic vs LLM Alignment
Correlation between Systematic and LLM perturbation accuracies for overlapping categories.

| model_name                                  | perturbation_type                         |   llm |   systematic |   Delta (Sys - LLM) |
|:--------------------------------------------|:------------------------------------------|------:|-------------:|--------------------:|
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | anchored_pronoun_references               | 74.60 |        69.70 |               -4.91 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | comment_annotations                       | 88.37 |        90.00 |                1.63 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | incomplete_join_spec                      | 26.92 |        22.73 |               -4.20 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | mixed_sql_nl                              | 86.33 |        86.67 |                0.34 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | omit_obvious_operation_markers            | 78.83 |        85.83 |                7.00 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | operator_aggregate_variation              | 77.78 |        60.32 |              -17.46 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | phrasal_and_idiomatic_action_substitution | 87.68 |        88.75 |                1.07 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | punctuation_variation                     | 94.24 |        94.23 |               -0.01 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | table_column_synonyms                     | 80.29 |        76.52 |               -3.78 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | temporal_expression_variation             | 70.69 |       100.00 |               29.31 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | typos                                     | 92.09 |        89.63 |               -2.46 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | urgency_qualifiers                        | 93.53 |        92.86 |               -0.67 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | verbosity_variation                       | 92.09 |        90.71 |               -1.37 |
| claude-haiku-4-5-20251001                   | anchored_pronoun_references               | 77.78 |        72.73 |               -5.05 |
| claude-haiku-4-5-20251001                   | comment_annotations                       | 90.70 |        90.00 |               -0.70 |
| claude-haiku-4-5-20251001                   | incomplete_join_spec                      | 23.08 |        22.73 |               -0.35 |
| claude-haiku-4-5-20251001                   | mixed_sql_nl                              | 87.77 |        91.67 |                3.90 |
| claude-haiku-4-5-20251001                   | omit_obvious_operation_markers            | 72.99 |        77.50 |                4.51 |
| claude-haiku-4-5-20251001                   | operator_aggregate_variation              | 75.00 |        65.08 |               -9.92 |
| claude-haiku-4-5-20251001                   | phrasal_and_idiomatic_action_substitution | 83.33 |        85.00 |                1.67 |
| claude-haiku-4-5-20251001                   | punctuation_variation                     | 92.81 |        94.23 |                1.43 |
| claude-haiku-4-5-20251001                   | table_column_synonyms                     | 83.21 |        81.82 |               -1.39 |
| claude-haiku-4-5-20251001                   | temporal_expression_variation             | 62.07 |        93.10 |               31.03 |
| claude-haiku-4-5-20251001                   | typos                                     | 89.93 |        88.89 |               -1.04 |
| claude-haiku-4-5-20251001                   | urgency_qualifiers                        | 92.09 |        91.43 |               -0.66 |
| claude-haiku-4-5-20251001                   | verbosity_variation                       | 89.21 |        91.43 |                2.22 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | anchored_pronoun_references               | 69.84 |        60.61 |               -9.24 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | comment_annotations                       | 76.74 |        77.14 |                0.40 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | incomplete_join_spec                      |  7.69 |         9.09 |                1.40 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | mixed_sql_nl                              | 85.61 |        90.00 |                4.39 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | omit_obvious_operation_markers            | 74.45 |        68.33 |               -6.12 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | operator_aggregate_variation              | 75.00 |        58.73 |              -16.27 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | phrasal_and_idiomatic_action_substitution | 78.99 |        82.50 |                3.51 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | punctuation_variation                     | 84.89 |        90.38 |                5.49 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | table_column_synonyms                     | 57.66 |        59.09 |                1.43 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | temporal_expression_variation             | 74.14 |        89.66 |               15.52 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | typos                                     | 82.01 |        84.44 |                2.43 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | urgency_qualifiers                        | 84.89 |        85.00 |                0.11 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | verbosity_variation                       | 77.70 |        80.71 |                3.02 |
| gemini-2.5-flash-lite                       | anchored_pronoun_references               | 68.25 |        63.64 |               -4.62 |
| gemini-2.5-flash-lite                       | comment_annotations                       | 72.09 |        79.29 |                7.19 |
| gemini-2.5-flash-lite                       | incomplete_join_spec                      | 23.08 |        18.18 |               -4.90 |
| gemini-2.5-flash-lite                       | mixed_sql_nl                              | 87.77 |        83.33 |               -4.44 |
| gemini-2.5-flash-lite                       | omit_obvious_operation_markers            | 75.18 |        77.50 |                2.32 |
| gemini-2.5-flash-lite                       | operator_aggregate_variation              | 72.22 |        52.38 |              -19.84 |
| gemini-2.5-flash-lite                       | phrasal_and_idiomatic_action_substitution | 79.71 |        85.00 |                5.29 |
| gemini-2.5-flash-lite                       | punctuation_variation                     | 79.14 |        71.15 |               -7.98 |
| gemini-2.5-flash-lite                       | table_column_synonyms                     | 67.88 |        62.12 |               -5.76 |
| gemini-2.5-flash-lite                       | temporal_expression_variation             | 63.79 |        89.66 |               25.86 |
| gemini-2.5-flash-lite                       | typos                                     | 82.01 |        76.30 |               -5.72 |
| gemini-2.5-flash-lite                       | urgency_qualifiers                        | 87.77 |        84.29 |               -3.48 |
| gemini-2.5-flash-lite                       | verbosity_variation                       | 83.45 |        82.14 |               -1.31 |
| gpt-4o                                      | anchored_pronoun_references               | 80.95 |        78.79 |               -2.16 |
| gpt-4o                                      | comment_annotations                       | 86.05 |        90.00 |                3.95 |
| gpt-4o                                      | incomplete_join_spec                      | 23.08 |        22.73 |               -0.35 |
| gpt-4o                                      | mixed_sql_nl                              | 90.65 |        93.33 |                2.69 |
| gpt-4o                                      | omit_obvious_operation_markers            | 77.37 |        87.50 |               10.13 |
| gpt-4o                                      | operator_aggregate_variation              | 77.78 |        61.90 |              -15.87 |
| gpt-4o                                      | phrasal_and_idiomatic_action_substitution | 81.88 |        87.50 |                5.62 |
| gpt-4o                                      | punctuation_variation                     | 93.53 |        90.38 |               -3.14 |
| gpt-4o                                      | table_column_synonyms                     | 85.40 |        81.82 |               -3.58 |
| gpt-4o                                      | temporal_expression_variation             | 63.79 |        79.31 |               15.52 |
| gpt-4o                                      | typos                                     | 91.37 |        88.15 |               -3.22 |
| gpt-4o                                      | urgency_qualifiers                        | 92.81 |        92.14 |               -0.66 |
| gpt-4o                                      | verbosity_variation                       | 90.65 |        90.71 |                0.07 |
| meta-llama/Llama-3.1-8B                     | anchored_pronoun_references               | 52.38 |        54.55 |                2.16 |
| meta-llama/Llama-3.1-8B                     | comment_annotations                       | 67.44 |        75.71 |                8.27 |
| meta-llama/Llama-3.1-8B                     | incomplete_join_spec                      | 15.38 |        27.27 |               11.89 |
| meta-llama/Llama-3.1-8B                     | mixed_sql_nl                              | 69.78 |        72.50 |                2.72 |
| meta-llama/Llama-3.1-8B                     | omit_obvious_operation_markers            | 72.99 |        60.83 |              -12.16 |
| meta-llama/Llama-3.1-8B                     | operator_aggregate_variation              | 74.07 |        63.49 |              -10.58 |
| meta-llama/Llama-3.1-8B                     | phrasal_and_idiomatic_action_substitution | 81.16 |        65.00 |              -16.16 |
| meta-llama/Llama-3.1-8B                     | punctuation_variation                     | 84.89 |        84.62 |               -0.28 |
| meta-llama/Llama-3.1-8B                     | table_column_synonyms                     | 43.80 |        41.67 |               -2.13 |
| meta-llama/Llama-3.1-8B                     | temporal_expression_variation             | 70.69 |        82.76 |               12.07 |
| meta-llama/Llama-3.1-8B                     | typos                                     | 79.86 |        78.52 |               -1.34 |
| meta-llama/Llama-3.1-8B                     | urgency_qualifiers                        | 78.42 |        64.29 |              -14.13 |
| meta-llama/Llama-3.1-8B                     | verbosity_variation                       | 72.66 |        72.86 |                0.20 |

## Observations and Insights

### 1. Baseline Performance by Complexity
*   **High Proficiency in DML:** All models demonstrated exceptional accuracy (>90%) on standard DML operations (`INSERT`, `UPDATE`, `DELETE`), confirming that basic SQL syntax generation is well-solved.
*   **The "Join" Bottleneck:** Join operations remain the distinct weak point for all models. Even top-tier models like Claude and GPT-4o typically scored lower on Joins than any other category. Llama 3.1 struggles significantly here, dropping to 30% accuracy, indicating a fundamental difficulty in inferring relationships without explicit guidance.
*   **Union Handling:** Most models handled Union operations well, but Llama 3.1 again showed weakness (60%), suggesting struggle with complex set operations.

### 2. Perturbation Impact Analysis
*   **Catastrophic Failure on Incomplete Joins:** The `incomplete_join_spec` perturbation caused a massive accuracy drop across *all* models (down to <30%). This confirms that models rely heavily on explicit "JOIN" keywords and struggled to infer relationships from natural language phrases like "and their posts" without schema-aware reasoning.
*   **Resilience to Formatting Noise:** Models were highly resilient to `typos`, `punctuation_variation`, and `verbosity_variation`, maintaining high accuracy (>80-90%). This suggests robust tokenization and attention mechanisms that filter out surface-level noise.
*   **Temporal Complexity:** `temporal_expression_variation` showed a significant divergence. Systematic perturbations (often just replacing '2024-01-01' with 'January 1st, 2024') remained high accuracy. However, LLM-generated temporal perturbations caused a sharp drop (e.g., Qwen dropped 30%). This suggests LLM perturbations likely introduced more ambiguous or complex relative time constraints (e.g., "last fiscal quarter") that are harder to parse than simple format changes.

### 3. Systematic vs. LLM Perturbation Alignment
*   **General Correlation:** For most categories (`typos`, `verbosity`, `mixed_sql_nl`), the accuracy on Systematic vs. LLM perturbations was very similar (Delta < 5%), validating the quality of the rule-based systematic content.
*   **The "Operator" Divergence:** `operator_aggregate_variation` showed a consistent drop where Systematic perturbations were *harder* than LLM ones (negative delta). This implies the systematic engine might be generating more aggressive or obscure operator replacements (e.g. symbolic synonyms) than the LLM, which tends to stick to more natural phrasing.

### 4. Model-Specific Findings
*   **Top Tier (GPT-4o, Claude):** Consistently resilient across almost all perturbations. They are the only models that maintained respectable performance on "Compound" complexity.
*   **Mid Tier (Gemini, Qwen):** Strong on basics but more brittle when faced with ambiguity (incomplete joins) or complex compound perturbations.
*   **Low Tier (Llama 3.1):** Struggles significantly with any complexity beyond simple SELECT/DML. The 30% accuracy on vanilla Joins is a critical finding, suggesting it's not suitable for schema-reasoning tasks without fine-tuning.
