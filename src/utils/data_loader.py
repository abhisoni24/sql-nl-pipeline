import json
import random
from typing import List, Dict, Any


def _unwrap_records(data):
    """Unwrap {metadata, records} envelope if present."""
    if isinstance(data, dict) and 'records' in data:
        return data['records']
    return data


def load_baseline_queries(path: str) -> List[Dict[str, Any]]:
    """Load baseline queries (original NL prompts)."""
    with open(path, 'r') as f:
        queries = _unwrap_records(json.load(f))
    
    tasks = []
    for q in queries:
        tasks.append({
            'job_id': f"{q['id']}_baseline_original",
            'query_id': q['id'],
            'perturbation_source': 'baseline',
            'perturbation_type': 'original',
            'input_prompt': q['nl_prompt'],
            'gold_sql': q['sql'],
            'complexity': q.get('complexity', 'unknown'),
            'tables': q.get('tables', [])
        })
    return tasks

def load_systematic_perturbations(path: str) -> List[Dict[str, Any]]:
    """Load systematic perturbations."""
    with open(path, 'r') as f:
        queries = _unwrap_records(json.load(f))
    
    tasks = []
    for q in queries:
        query_id = q['id']
        gold_sql = q['sql']
        perturbations = q.get('generated_perturbations', {})
        
        # Add original
        original = perturbations.get('original', {})
        if original.get('nl_prompt'):
            tasks.append({
                'job_id': f"{query_id}_systematic_original",
                'query_id': query_id,
                'perturbation_source': 'systematic',
                'perturbation_type': 'original',
                'input_prompt': original['nl_prompt'],
                'gold_sql': gold_sql,
                'complexity': q.get('complexity', 'unknown'),
                'tables': q.get('tables', [])
            })
        
        # Add single perturbations
        for p in perturbations.get('single_perturbations', []):
            if p.get('applicable') and p.get('perturbed_nl_prompt'):
                p_type = p['perturbation_name']
                tasks.append({
                    'job_id': f"{query_id}_systematic_{p_type}",
                    'query_id': query_id,
                    'perturbation_source': 'systematic',
                    'perturbation_type': p_type,
                    'input_prompt': p['perturbed_nl_prompt'],
                    'gold_sql': gold_sql,
                    'complexity': q.get('complexity', 'unknown'),
                    'tables': q.get('tables', [])
                })
    
    return tasks

def load_llm_perturbations(path: str) -> List[Dict[str, Any]]:
    """Load LLM-generated perturbations."""
    with open(path, 'r') as f:
        queries = _unwrap_records(json.load(f))
    
    tasks = []
    for q in queries:
        query_id = q['id']
        gold_sql = q['sql']
        perturbations = q.get('generated_perturbations', {})
        
        # Add single perturbations
        for p in perturbations.get('single_perturbations', []):
            if p.get('applicable') and p.get('perturbed_nl_prompt'):
                p_type = p['perturbation_name']
                tasks.append({
                    'job_id': f"{query_id}_llm_{p_type}",
                    'query_id': query_id,
                    'perturbation_source': 'llm',
                    'perturbation_type': p_type,
                    'input_prompt': p['perturbed_nl_prompt'],
                    'gold_sql': gold_sql,
                    'complexity': q.get('complexity', 'unknown'),
                    'tables': q.get('tables', [])
                })
        
        # Add compound perturbation
        compound = perturbations.get('compound_perturbation', {})
        if compound.get('perturbed_nl_prompt'):
            tasks.append({
                'job_id': f"{query_id}_llm_compound",
                'query_id': query_id,
                'perturbation_source': 'llm',
                'perturbation_type': 'compound',
                'input_prompt': compound['perturbed_nl_prompt'],
                'gold_sql': gold_sql,
                'complexity': q.get('complexity', 'unknown'),
                'tables': q.get('tables', [])
            })
    
    return tasks
