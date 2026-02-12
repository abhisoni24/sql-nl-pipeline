# Experiment Analysis: Prompt Fix Impact & Systematic Perturbation Power

## 1. Executive Summary

This report compares two experiment runs (Original vs. Improved NL Prompts) across four LLM models. The integration of systematic NL prompt clarifications (e.g., explicit join cues, date logic fixes) yielded a consistent **+2.4% to +4.3% accuracy gain** across all models.

Crucially, the analysis confirms that **Systematic Perturbations are significantly harder** for LLMs to handle than traditional LLM-generated paraphrases. This highlights the value of our robust perturbation engine: it generates edge cases (incomplete joins, date math) that LLMs rarely produce themselves but frequently fail on.

## 2. Accuracy Comparison (Old vs. New)

All models improved, validating the effectiveness of the recent prompt engineering fixes.

| Model                     | Original Accuracy | Improved Accuracy | Delta        |
| :------------------------ | :---------------- | :---------------- | :----------- |
| **Qwen/Qwen3-Coder-30B**  | 85.5%             | **89.1%**         | 🟢 **+3.6%** |
| **Gemini-2.5-Flash-Lite** | 76.8%             | **81.1%**         | 🟢 **+4.3%** |
| **DeepSeek-Coder-V2**     | 76.4%             | **80.0%**         | 🟢 **+3.6%** |
| **Llama-3.1-8B**          | 69.6%             | **72.0%**         | 🟢 **+2.4%** |

> **Insight:** Gemini showed the largest improvement, suggesting it is highly sensitive to prompt clarity.

## 3. The Power of Systematic Perturbation

Our systematic perturbation engine proves to be a rigorous stress test, consistently pushing model accuracy lower than standard LLM paraphrasing. This delta represents the "Robustness Gap."

| Model           | Baseline Accuracy | LLM Perturbation Accuracy | Systematic Perturbation Accuracy | Robustness Gap (LLM - Sys) |
| :-------------- | :---------------- | :------------------------ | :------------------------------- | :------------------------- |
| **Qwen-30B**    | 97.1%             | 89.9%                     | 87.2%                            | -2.7%                      |
| **DeepSeek-V2** | 87.9%             | 79.9%                     | 79.3%                            | -0.6%                      |
| **Gemini-Lite** | 85.0%             | 81.7%                     | 79.9%                            | -1.8%                      |
| **Llama-8B**    | 84.3%             | 73.5%                     | **68.6%**                        | 🔴 **-4.9%**               |

> **Key Finding:** Smaller models (Llama-8B) crumble under systematic stress (-15.7% drop from baseline), while larger models (Qwen-30B) are more resilient (-9.9% drop).

### Most Difficult Systematic Perturbations

The following perturbations caused the steepest accuracy drops across all models:

1.  🔴 **Incomplete Join Spec (40.2% Accuracy)**: When join conditions are implicit (e.g., "users and their comments"), models fail catastrophically to infer the correct keys.
2.  🟠 **Temporal Expression Variation (60.7% Accuracy)**: Complex date math (e.g., "3 weeks ago") remains a significant weakness.
3.  🟠 **Compound Perturbations (65.2% Accuracy)**: Stacking multiple noise types breaks models effectively.
4.  🟡 **Table/Column Synonyms (67.1% Accuracy)**: Schema linking degrades rapidly with vocabulary mismatch.

## 4. Model Failure Patterns

Each model exhibits distinct failure modes:

### **Qwen/Qwen3-Coder-30B (Top Performer)**

- **Dominant Failure:** Subtle Logic Errors (Wrong Result).
- **Behavior:** Rarely produces invalid SQL. When it fails, it's usually a nuanced semantic error (e.g., wrong filter operator or missing a specific condition).
- **Verdict:** Highly robust, fails gracefully.

### **Gemini-2.5-Flash-Lite (Most Improved)**

- **Dominant Failure:** **Query Type Mismatch** and Wrong Result.
- **Behavior:** Uniquely prone to refusing queries or outputting conversational text instead of SQL when confused.
- **Verdict:** Sensitive to ambiguity; needs strict system prompting to force SQL output.

### **DeepSeek-Coder-V2-Lite**

- **Dominant Failure:** Wrong Result.
- **Behavior:** performance mirrors Qwen but with lower overall accuracy. Often hallucinates simple WHERE clauses.
- **Verdict:** A solid middle-ground performer.

### **Llama-3.1-8B (Weakest)**

- **Dominant Failure:** **Execution Errors** and Wrong Result.
- **Behavior:** Frequently generates invalid SQL (syntax errors, hallucinated columns). It struggles to maintain syntactic validity under perturbation stress.
- **Verdict:** Needs few-shot examples or constrained decoding to improve reliability.

## 5. Conclusion

The "Improved NL Prompts" experiment was a success, raising the accuracy floor for all models. However, the **Systematic Perturbation Engine** remains the ultimate stress test. It reveals that while models can handle linguistic variation (LLM perturbations), they struggle with **structural ambiguity** (incomplete joins) and **reasoning** (date math). Future work should focus on fine-tuning models specifically on these harder systematic patterns.
