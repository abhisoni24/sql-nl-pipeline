#!/usr/bin/env python3
"""
Re-evaluate experiment results with enhanced evaluator.
Compares old evaluation (with bugs) vs new evaluation (with fixes).
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

# Add project to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.harness.core.evaluation import Evaluator

def re_evaluate_results(input_file: str, output_file: str):
    """Re-evaluate all results with enhanced evaluator."""
    
    print(f"Loading results from: {input_file}")
    
    # Load all results
    results = []
    with open(input_file, 'r') as f:
        for line in f:
            if line.strip():
                results.append(json.loads(line))
    
    print(f"Loaded {len(results)} records")
    print(f"\nRe-evaluating with enhanced evaluator...")
    
    evaluator = Evaluator()
    
    # Statistics
    stats = {
        'total': 0,
        'old_correct': 0,
        'old_incorrect': 0,
        'new_correct': 0,
        'new_incorrect': 0,
        'changed_to_correct': 0,
        'changed_to_incorrect': 0,
        'unchanged': 0,
    }
    
    changes_by_complexity = defaultdict(lambda: {'to_correct': 0, 'to_incorrect': 0})
    changes_by_perturbation = defaultdict(lambda: {'to_correct': 0, 'to_incorrect': 0})
    
    # Re-evaluate all records
    re_evaluated = []
    for record in tqdm(results, desc="Re-evaluating"):
        old_correctness = record['evaluation_result']['correctness']
        
        # Re-evaluate with new evaluator (FAST version - no execution verifier!)
        new_result = evaluator.fast_evaluate(record['gold_sql'], record['normalized_sql'])
        
        # Update record with new evaluation
        new_record = record.copy()
        new_record['old_evaluation_result'] = record['evaluation_result']
        new_record['evaluation_result'] = {
            'correctness': new_result.execution_match,
            'similarity_score': new_result.similarity_score,
            'failure_type': new_result.failure_type
        }
        
        re_evaluated.append(new_record)
        
        # Track statistics
        stats['total'] += 1
        
        if old_correctness:
            stats['old_correct'] += 1
        else:
            stats['old_incorrect'] += 1
        
        if new_result.execution_match:
            stats['new_correct'] += 1
        else:
            stats['new_incorrect'] += 1
        
        # Track changes
        if old_correctness != new_result.execution_match:
            if new_result.execution_match:
                stats['changed_to_correct'] += 1
                changes_by_complexity[record['query_complexity']]['to_correct'] += 1
                changes_by_perturbation[record['perturbation_type']]['to_correct'] += 1
            else:
                stats['changed_to_incorrect'] += 1
                changes_by_complexity[record['query_complexity']]['to_incorrect'] += 1
                changes_by_perturbation[record['perturbation_type']]['to_incorrect'] += 1
        else:
            stats['unchanged'] += 1
    
    # Write re-evaluated results
    print(f"\nWriting re-evaluated results to: {output_file}")
    with open(output_file, 'w') as f:
        for record in re_evaluated:
            f.write(json.dumps(record) + '\n')
    
    # Print statistics
    print(f"\n{'='*80}")
    print("RE-EVALUATION RESULTS")
    print(f"{'='*80}")
    print(f"\nTotal Records: {stats['total']}")
    print(f"\nOLD Evaluation (with bugs):")
    print(f"  Correct:   {stats['old_correct']:>6} ({stats['old_correct']/stats['total']*100:.2f}%)")
    print(f"  Incorrect: {stats['old_incorrect']:>6} ({stats['old_incorrect']/stats['total']*100:.2f}%)")
    print(f"\nNEW Evaluation (with fixes):")
    print(f"  Correct:   {stats['new_correct']:>6} ({stats['new_correct']/stats['total']*100:.2f}%)")
    print(f"  Incorrect: {stats['new_incorrect']:>6} ({stats['new_incorrect']/stats['total']*100:.2f}%)")
    print(f"\nChanges:")
    print(f"  Changed to CORRECT:   {stats['changed_to_correct']:>6} (false negatives fixed)")
    print(f"  Changed to INCORRECT: {stats['changed_to_incorrect']:>6} (false positives caught)")
    print(f"  Unchanged:            {stats['unchanged']:>6}")
    print(f"\nAccuracy Improvement: {(stats['new_correct'] - stats['old_correct'])/stats['total']*100:+.2f} percentage points")
    
    print(f"\n{'='*80}")
    print("CHANGES BY COMPLEXITY")
    print(f"{'='*80}")
    for complexity, changes in sorted(changes_by_complexity.items()):
        print(f"{complexity:15s}: +{changes['to_correct']:4d} correct, -{changes['to_incorrect']:4d} incorrect")
    
    print(f"\n{'='*80}")
    print("TOP 10 PERTURBATIONS WITH MOST CORRECTIONS")
    print(f"{'='*80}")
    sorted_perturbs = sorted(changes_by_perturbation.items(), 
                            key=lambda x: x[1]['to_correct'], reverse=True)[:10]
    for pert, changes in sorted_perturbs:
        print(f"{pert:25s}: +{changes['to_correct']:4d} correct")
    
    return stats, changes_by_complexity, changes_by_perturbation


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Re-evaluate experiment results with enhanced evaluator')
    parser.add_argument('input_file', help='Input JSONL file with original evaluation')
    parser.add_argument('output_file', help='Output JSONL file for re-evaluated results')
    
    args = parser.parse_args()
    
    re_evaluate_results(args.input_file, args.output_file)
