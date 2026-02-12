
import pandas as pd
import json
import re

# Paths
old_path = '/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run/20each/output/evaluated_results_aggregated.jsonl'
new_path = '/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run/improved_20each/output/evaluated_results_aggregated.jsonl'

def load_data(path):
    records = []
    with open(path, 'r') as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except: pass
    return pd.DataFrame(records)

print('Loading datasets...')
old_df = load_data(old_path)
new_df = load_data(new_path)

# Filter for common models
common_models = set(old_df['model_name']).intersection(set(new_df['model_name']))
print(f'Common Models: {common_models}')

old_df = old_df[old_df['model_name'].isin(common_models)]
new_df = new_df[new_df['model_name'].isin(common_models)]

# 1. Overall Accuracy Comparison
print('\n=== Accuracy Comparison ===')
old_acc = old_df.groupby('model_name')['is_equivalent'].mean() * 100
new_acc = new_df.groupby('model_name')['is_equivalent'].mean() * 100
diff = new_acc - old_acc
comparison = pd.concat([old_acc, new_acc, diff], axis=1, keys=['Old', 'New', 'Delta'])
print(comparison)

# 2. Perturbation Source Analysis (New Data)
print('\n=== Accuracy by Source (New Data) ===')
source_acc = new_df.groupby(['model_name', 'perturbation_source'])['is_equivalent'].mean() * 100
print(source_acc.unstack())

# 3. Systematic vs LLM Breakdown (New Data)
print('\n=== Systematic vs LLM Breakdown (New Data) ===')
# Calculate accuracy per perturbation type across all models to see difficulty
pert_difficulty = new_df.groupby('perturbation_type')['is_equivalent'].mean() * 100
print(pert_difficulty.sort_values())

# 4. Failure Analysis (New Data)
print('\n=== Failure Analysis Samples ===')
failures = new_df[~new_df['is_equivalent']].copy()

def categorize_error(row):
    details = row.get('equivalence_details', '')
    if 'Execution failed' in details or 'execution failed' in details:
        return 'Execution Error'
    if 'Different denotations' in details:
        return 'Wrong Result'
    if 'Query type mismatch' in details:
        return 'Query Type Mismatch'
    return 'Other'

failures['error_category'] = failures.apply(categorize_error, axis=1)

print('\nError Categories:')
print(failures['error_category'].value_counts())

# Deeper Dive into "Wrong Result"
wrong_results = failures[failures['error_category'] == 'Wrong Result']

def analyze_sql_semantic_failure(row):
    gold = (row.get('gold_sql') or '').upper()
    gen = (row.get('generated_sql') or '').upper()
    
    if not gen: return 'Empty Generation'
    
    reasons = []
    
    # Check for missing WHERE clause (under-constraining)
    if 'WHERE' in gold and 'WHERE' not in gen:
        reasons.append('Missing WHERE')
        
    # Check for missing JOIN (missing relations)
    if 'JOIN' in gold and 'JOIN' not in gen:
        reasons.append('Missing JOIN')
        
    # Check for HALLUCINATED predicates
    if 'WHERE' not in gold and 'WHERE' in gen:
        reasons.append('Hallucinated WHERE')
        
    return ', '.join(reasons) if reasons else 'Subtle Semantic Logic'

failures['semantic_reason'] = failures.apply(analyze_sql_semantic_failure, axis=1)
print('\nSemantic Failure Reasons (Heuristic):')
print(failures['semantic_reason'].value_counts().head(10))

# 5. Model Specific Failures
print('\n=== Model Specific Failure Modes ===')
for model in common_models:
    model_failures = failures[failures['model_name'] == model]
    print(f'\n{model}:')
    print(model_failures['error_category'].value_counts().head(3))
