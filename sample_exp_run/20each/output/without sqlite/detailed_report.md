# Detailed Experiment Analysis Report
**Source Data:** `sample_exp_run/20each/output/evaluated_results_aggregated.jsonl`

## Vanilla Complexity Accuracy
Accuracy (%) of Baseline (Vanilla) prompts by complexity.

| complexity   |   Qwen/Qwen3-Coder-30B-A3B-Instruct |   claude-haiku-4-5-20251001 |   deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |   gemini-2.5-flash-lite |   gpt-4o |   meta-llama/Llama-3.1-8B |
|:-------------|------------------------------------:|----------------------------:|----------------------------------------------:|------------------------:|---------:|--------------------------:|
| advanced     |                               75.00 |                       75.00 |                                         65.00 |                   90.00 |    75.00 |                     65.00 |
| delete       |                               70.00 |                       70.00 |                                         70.00 |                   95.00 |    70.00 |                     65.00 |
| insert       |                                0.00 |                        0.00 |                                          0.00 |                  100.00 |    55.00 |                      0.00 |
| join         |                               75.00 |                       70.00 |                                         60.00 |                   80.00 |    80.00 |                     40.00 |
| simple       |                               90.00 |                       90.00 |                                         90.00 |                   95.00 |    90.00 |                     80.00 |
| union        |                               50.00 |                       50.00 |                                         30.00 |                   95.00 |    50.00 |                     30.00 |
| update       |                               35.00 |                       75.00 |                                         35.00 |                   90.00 |    75.00 |                     35.00 |

## Perturbation Category Accuracy
Accuracy (%) by Perturbation Category and Source.

| perturbation_type                         | perturbation_source   |   Qwen/Qwen3-Coder-30B-A3B-Instruct |   claude-haiku-4-5-20251001 |   deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |   gemini-2.5-flash-lite |   gpt-4o |   meta-llama/Llama-3.1-8B |
|:------------------------------------------|:----------------------|------------------------------------:|----------------------------:|----------------------------------------------:|------------------------:|---------:|--------------------------:|
| ambiguous_pronouns                        | systematic            |                               51.52 |                       51.52 |                                         33.33 |                   69.70 |    54.55 |                     39.39 |
| anchored_pronoun_references               | llm                   |                               60.32 |                       63.49 |                                         49.21 |                   76.19 |    69.84 |                     41.27 |
| comment_annotations                       | llm                   |                               53.49 |                       58.14 |                                         41.86 |                   88.37 |    60.47 |                     30.23 |
| comment_annotations                       | systematic            |                               55.00 |                       63.57 |                                         45.00 |                   86.43 |    69.29 |                     45.00 |
| comment_style_annotations                 | llm                   |                               54.17 |                       59.38 |                                         52.08 |                   80.21 |    67.71 |                     45.83 |
| compound                                  | llm                   |                               37.41 |                       47.48 |                                         30.22 |                   61.87 |    52.52 |                     23.74 |
| incomplete_join_spec                      | llm                   |                               19.23 |                       19.23 |                                          7.69 |                   23.08 |    23.08 |                      7.69 |
| incomplete_join_spec                      | systematic            |                               18.18 |                       18.18 |                                         13.64 |                   22.73 |    18.18 |                     22.73 |
| mixed_sql_nl                              | llm                   |                               58.99 |                       66.19 |                                         58.27 |                   87.77 |    66.19 |                     49.64 |
| mixed_sql_nl                              | systematic            |                               64.17 |                       71.67 |                                         60.00 |                   92.50 |    73.33 |                     41.67 |
| omit_obvious_clauses                      | systematic            |                               60.83 |                       64.17 |                                         50.83 |                   79.17 |    70.00 |                     40.00 |
| omit_obvious_operation_markers            | llm                   |                               48.91 |                       53.28 |                                         43.80 |                   69.34 |    58.39 |                     42.34 |
| operator_aggregate_variation              | llm                   |                               58.33 |                       65.74 |                                         50.93 |                   75.93 |    68.52 |                     47.22 |
| operator_aggregate_variation              | systematic            |                               44.44 |                       47.62 |                                         41.27 |                   53.97 |    47.62 |                     39.68 |
| original                                  | systematic            |                               57.14 |                       61.43 |                                         50.00 |                   92.14 |    71.43 |                     45.00 |
| phrasal_and_idiomatic_action_substitution | llm                   |                               54.35 |                       63.04 |                                         48.55 |                   84.06 |    64.49 |                     47.83 |
| punctuation_variation                     | llm                   |                               56.83 |                       64.03 |                                         48.92 |                   90.65 |    69.06 |                     46.04 |
| punctuation_variation                     | systematic            |                               50.00 |                       55.77 |                                         44.23 |                   88.46 |    65.38 |                     42.31 |
| sentence_structure_variation              | llm                   |                               58.39 |                       65.69 |                                         48.18 |                   90.51 |    69.34 |                     45.99 |
| synonym_substitution                      | systematic            |                               73.75 |                       71.25 |                                         62.50 |                   92.50 |    75.00 |                     52.50 |
| table_column_synonyms                     | llm                   |                               47.45 |                       54.01 |                                         24.09 |                   71.53 |    67.15 |                     18.25 |
| table_column_synonyms                     | systematic            |                               44.70 |                       54.55 |                                         29.55 |                   74.24 |    63.64 |                     19.70 |
| temporal_expression_variation             | llm                   |                                3.45 |                        5.17 |                                          5.17 |                   41.38 |     8.62 |                     13.79 |
| temporal_expression_variation             | systematic            |                                6.90 |                        0.00 |                                          0.00 |                   82.76 |     0.00 |                      0.00 |
| typos                                     | llm                   |                               56.12 |                       61.87 |                                         50.36 |                   90.65 |    70.50 |                     44.60 |
| typos                                     | systematic            |                               52.59 |                       62.22 |                                         47.41 |                   87.41 |    68.15 |                     45.19 |
| urgency_qualifiers                        | llm                   |                               55.40 |                       61.87 |                                         49.64 |                   88.49 |    71.94 |                     44.60 |
| urgency_qualifiers                        | systematic            |                               58.57 |                       62.86 |                                         50.00 |                   88.57 |    67.14 |                     40.71 |
| verbosity_variation                       | llm                   |                               53.24 |                       60.43 |                                         47.48 |                   86.33 |    69.06 |                     41.01 |
| verbosity_variation                       | systematic            |                               52.86 |                       61.43 |                                         48.57 |                   90.00 |    70.00 |                     40.71 |

## Compound vs Vanilla Performance
Comparison of accuracy between Baseline and Compound/Mixed perturbations.

| model_name                                  |   Vanilla Accuracy |   Compound Accuracy |   Performance Drop |
|:--------------------------------------------|-------------------:|--------------------:|-------------------:|
| Qwen/Qwen3-Coder-30B-A3B-Instruct           |              56.43 |               53.02 |               3.41 |
| claude-haiku-4-5-20251001                   |              61.43 |               61.31 |               0.12 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct |              50.00 |               48.99 |               1.01 |
| gemini-2.5-flash-lite                       |              92.14 |               80.15 |              11.99 |
| gpt-4o                                      |              70.71 |               63.57 |               7.15 |
| meta-llama/Llama-3.1-8B                     |              45.00 |               38.19 |               6.81 |

## Systematic vs LLM Alignment
Correlation between Systematic and LLM perturbation accuracies for overlapping categories.

| model_name                                  | perturbation_type             |   llm |   systematic |   Delta (Sys - LLM) |
|:--------------------------------------------|:------------------------------|------:|-------------:|--------------------:|
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | comment_annotations           | 53.49 |        55.00 |                1.51 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | incomplete_join_spec          | 19.23 |        18.18 |               -1.05 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | mixed_sql_nl                  | 58.99 |        64.17 |                5.17 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | operator_aggregate_variation  | 58.33 |        44.44 |              -13.89 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | punctuation_variation         | 56.83 |        50.00 |               -6.83 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | table_column_synonyms         | 47.45 |        44.70 |               -2.75 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | temporal_expression_variation |  3.45 |         6.90 |                3.45 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | typos                         | 56.12 |        52.59 |               -3.52 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | urgency_qualifiers            | 55.40 |        58.57 |                3.18 |
| Qwen/Qwen3-Coder-30B-A3B-Instruct           | verbosity_variation           | 53.24 |        52.86 |               -0.38 |
| claude-haiku-4-5-20251001                   | comment_annotations           | 58.14 |        63.57 |                5.43 |
| claude-haiku-4-5-20251001                   | incomplete_join_spec          | 19.23 |        18.18 |               -1.05 |
| claude-haiku-4-5-20251001                   | mixed_sql_nl                  | 66.19 |        71.67 |                5.48 |
| claude-haiku-4-5-20251001                   | operator_aggregate_variation  | 65.74 |        47.62 |              -18.12 |
| claude-haiku-4-5-20251001                   | punctuation_variation         | 64.03 |        55.77 |               -8.26 |
| claude-haiku-4-5-20251001                   | table_column_synonyms         | 54.01 |        54.55 |                0.53 |
| claude-haiku-4-5-20251001                   | temporal_expression_variation |  5.17 |         0.00 |               -5.17 |
| claude-haiku-4-5-20251001                   | typos                         | 61.87 |        62.22 |                0.35 |
| claude-haiku-4-5-20251001                   | urgency_qualifiers            | 61.87 |        62.86 |                0.99 |
| claude-haiku-4-5-20251001                   | verbosity_variation           | 60.43 |        61.43 |                1.00 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | comment_annotations           | 41.86 |        45.00 |                3.14 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | incomplete_join_spec          |  7.69 |        13.64 |                5.94 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | mixed_sql_nl                  | 58.27 |        60.00 |                1.73 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | operator_aggregate_variation  | 50.93 |        41.27 |               -9.66 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | punctuation_variation         | 48.92 |        44.23 |               -4.69 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | table_column_synonyms         | 24.09 |        29.55 |                5.46 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | temporal_expression_variation |  5.17 |         0.00 |               -5.17 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | typos                         | 50.36 |        47.41 |               -2.95 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | urgency_qualifiers            | 49.64 |        50.00 |                0.36 |
| deepseek-ai/DeepSeek-Coder-V2-Lite-Instruct | verbosity_variation           | 47.48 |        48.57 |                1.09 |
| gemini-2.5-flash-lite                       | comment_annotations           | 88.37 |        86.43 |               -1.94 |
| gemini-2.5-flash-lite                       | incomplete_join_spec          | 23.08 |        22.73 |               -0.35 |
| gemini-2.5-flash-lite                       | mixed_sql_nl                  | 87.77 |        92.50 |                4.73 |
| gemini-2.5-flash-lite                       | operator_aggregate_variation  | 75.93 |        53.97 |              -21.96 |
| gemini-2.5-flash-lite                       | punctuation_variation         | 90.65 |        88.46 |               -2.19 |
| gemini-2.5-flash-lite                       | table_column_synonyms         | 71.53 |        74.24 |                2.71 |
| gemini-2.5-flash-lite                       | temporal_expression_variation | 41.38 |        82.76 |               41.38 |
| gemini-2.5-flash-lite                       | typos                         | 90.65 |        87.41 |               -3.24 |
| gemini-2.5-flash-lite                       | urgency_qualifiers            | 88.49 |        88.57 |                0.08 |
| gemini-2.5-flash-lite                       | verbosity_variation           | 86.33 |        90.00 |                3.67 |
| gpt-4o                                      | comment_annotations           | 60.47 |        69.29 |                8.82 |
| gpt-4o                                      | incomplete_join_spec          | 23.08 |        18.18 |               -4.90 |
| gpt-4o                                      | mixed_sql_nl                  | 66.19 |        73.33 |                7.15 |
| gpt-4o                                      | operator_aggregate_variation  | 68.52 |        47.62 |              -20.90 |
| gpt-4o                                      | punctuation_variation         | 69.06 |        65.38 |               -3.68 |
| gpt-4o                                      | table_column_synonyms         | 67.15 |        63.64 |               -3.52 |
| gpt-4o                                      | temporal_expression_variation |  8.62 |         0.00 |               -8.62 |
| gpt-4o                                      | typos                         | 70.50 |        68.15 |               -2.36 |
| gpt-4o                                      | urgency_qualifiers            | 71.94 |        67.14 |               -4.80 |
| gpt-4o                                      | verbosity_variation           | 69.06 |        70.00 |                0.94 |
| meta-llama/Llama-3.1-8B                     | comment_annotations           | 30.23 |        45.00 |               14.77 |
| meta-llama/Llama-3.1-8B                     | incomplete_join_spec          |  7.69 |        22.73 |               15.03 |
| meta-llama/Llama-3.1-8B                     | mixed_sql_nl                  | 49.64 |        41.67 |               -7.97 |
| meta-llama/Llama-3.1-8B                     | operator_aggregate_variation  | 47.22 |        39.68 |               -7.54 |
| meta-llama/Llama-3.1-8B                     | punctuation_variation         | 46.04 |        42.31 |               -3.74 |
| meta-llama/Llama-3.1-8B                     | table_column_synonyms         | 18.25 |        19.70 |                1.45 |
| meta-llama/Llama-3.1-8B                     | temporal_expression_variation | 13.79 |         0.00 |              -13.79 |
| meta-llama/Llama-3.1-8B                     | typos                         | 44.60 |        45.19 |                0.58 |
| meta-llama/Llama-3.1-8B                     | urgency_qualifiers            | 44.60 |        40.71 |               -3.89 |
| meta-llama/Llama-3.1-8B                     | verbosity_variation           | 41.01 |        40.71 |               -0.29 |
