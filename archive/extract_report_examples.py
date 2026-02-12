import json
from collections import defaultdict

input_path = '/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run/20each/output/evaluated_results_aggregated.jsonl'

examples = {
    "llama_join_fail": [],
    "incomplete_join_fail": [],
    "temporal_divergence": [],
    "operator_fail": []
}

# Helper to format example
def format_ex(row):
    return {
        "model": row.get('model_name'),
        "prompt": row.get('input_prompt'),
        "generated": row.get('generated_response'),
        "gold": row.get('gold_sql'),
        "type": row.get('perturbation_type'),
        "source": row.get('perturbation_source')
    }

try:
    with open(input_path, 'r') as f:
        rows = [json.loads(line) for line in f]
        
    # 1. Join Bottleneck (Llama 3.1 failing vanilla joins)
    for r in rows:
        if r['model_name'] == 'meta-llama/Llama-3.1-8B' and r.get('complexity') == 'join' and r['perturbation_type'] == 'original':
            if not r['is_equivalent']:
                examples["llama_join_fail"].append(format_ex(r))
                if len(examples["llama_join_fail"]) >= 2: break

    # 2. Incomplete Join Spec Failure (Any model, compare to original if possible, but just failing row is enough)
    for r in rows:
        if r['perturbation_type'] == 'incomplete_join_spec' and not r['is_equivalent']:
            # Try to find a readable one
            examples["incomplete_join_fail"].append(format_ex(r))
            if len(examples["incomplete_join_fail"]) >= 3: break

    # 3. Temporal Divergence (Systematic Pass vs LLM Fail - hard to match exact query ID without mapping, 
    # so just getting one example of LLM temporal failure to show complexity)
    for r in rows:
        if r['perturbation_type'] == 'temporal_expression_variation' and r['perturbation_source'] == 'llm' and not r['is_equivalent']:
             examples["temporal_divergence"].append(format_ex(r))
             if len(examples["temporal_divergence"]) >= 2: break

    # 4. Operator Failure (Systematic)
    for r in rows:
        if r['perturbation_type'] == 'operator_aggregate_variation' and r['perturbation_source'] == 'systematic' and not r['is_equivalent']:
             examples["operator_fail"].append(format_ex(r))
             if len(examples["operator_fail"]) >= 2: break

    print(json.dumps(examples, indent=2))

except Exception as e:
    print(f"Error: {e}")
