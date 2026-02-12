
"""
Recalculate evaluation for existing experiment results.
Fixes:
1. Unpacking error in check_equivalence
2. SQL Extraction for Gemini (removing prefixes like "ite\n")
"""
import json
import re
import os
import sys
from tqdm import tqdm

# Add project root path
sys.path.insert(0, os.path.abspath('.'))

def extract_sql_robust(raw_response: str) -> str:
    """Robust SQL extraction handling common LLM artifacts."""
    if not raw_response or not isinstance(raw_response, str):
        return ""
    
    clean = raw_response.strip()
    
    # 1. Markdown code blocks (most reliable)
    # Match ```sql ... ``` or ```sqlite ... ``` or plain ``` ... ```
    match = re.search(r'```(?:sql|sqlite)?\s*([\s\S]*?)```', clean, re.IGNORECASE)
    if match:
        clean = match.group(1).strip()
    
    # 2. Heuristic cleanups
    # Remove "ite" prefix (from "SQLite" being cut off or hallucinations)
    if clean.lower().startswith('ite\n') or clean.lower().startswith('ite '):
        clean = clean[3:].strip()
    elif clean.lower().startswith('sqlite\n'):
        clean = clean[6:].strip()
        
    return clean

def re_evaluate_results(results_path: str, output_path: str):
    from src.equivalence import SQLEquivalenceEngine
    from src.core.schema import SCHEMA
    
    print(f"Loading results from {results_path}...")
    with open(results_path, 'r') as f:
        data = json.load(f)
        
    # Re-extract SQL first
    print("re-extracting SQL...")
    for item in data:
        for model in item['model_results']:
            raw = item['model_results'][model]['raw_response']
            extracted = extract_sql_robust(raw)
            item['model_results'][model]['extracted_sql'] = extracted
            
    # Run evaluation
    engine = SQLEquivalenceEngine.from_schema(
        schema=SCHEMA,
        foreign_keys={},
        db_path='./test_dbs/re_eval.sqlite'
    )
    
    detailed = []
    
    # Init stats
    MODELS = ['gemini', 'openai', 'anthropic']
    summary = {m: {'correct': 0, 'total': 0, 'error': 0} for m in MODELS}
    
    print("\nRunning Equivalence Evaluation...")
    for item in tqdm(data):
        gold = item['gold_sql']
        entry = {
            'query_id': item['query_id'],
            'complexity': item['complexity'],
            'gold_sql': gold,
            'nl_prompt': item['nl_prompt'],
            'model_evaluations': {}
        }
        
        for model in MODELS:
            res = item['model_results'].get(model, {})
            gen = res.get('extracted_sql', '')
            summary[model]['total'] += 1
            
            if notGen := (not gen):
                summary[model]['error'] += 1
                entry['model_evaluations'][model] = {
                    'status': 'EMPTY',
                    'error': 'No SQL extracted',
                    'generated_sql': ''
                }
                continue
                
            try:
                # Correct call without unpacking
                result = engine.check_equivalence(gold, gen)
                
                is_equiv = result.is_equivalent
                if is_equiv:
                    summary[model]['correct'] += 1
                    status = 'PASS'
                else:
                    status = 'FAIL'
                    
                entry['model_evaluations'][model] = {
                    'status': status,
                    'generated_sql': gen,
                    'details': str(result.details)
                }
                
            except Exception as e:
                summary[model]['error'] += 1
                entry['model_evaluations'][model] = {
                    'status': 'EXECUTION_ERROR',
                    'generated_sql': gen,
                    'error': str(e)
                }
                
        detailed.append(entry)
        
    # Build final structure
    final_output = {
        'summary': {
            m: {
                **s,
                'incorrect': s['total'] - s['correct'] - s['error'],
                'accuracy': round(s['correct']/s['total']*100, 1)
            } for m, s in summary.items()
        },
        'detailed': detailed
    }
    
    print("\nWriting results...")
    with open(output_path, 'w') as f:
        json.dump(final_output, f, indent=2)
        
    print("\nResults Summary:")
    print(json.dumps(final_output['summary'], indent=2))
    
    engine.cleanup()

if __name__ == '__main__':
    re_evaluate_results(
        'dataset/current/llm_generation_results.json', 
        'dataset/current/llm_generation_results_evaluation_fixed.json'
    )
