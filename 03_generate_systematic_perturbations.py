"""
Script to generate systematic prompt perturbations using deterministic rules.
"""

import json
import sys
import os
from typing import Dict, Any, List

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer, PerturbationType, PerturbationConfig

INPUT_FILE = './dataset/current/nl_social_media_queries_20.json'
OUTPUT_FILE = './dataset/current/nl_social_media_queries_systematic_20.json'

PERTURBATION_DESCRIPTIONS = {
    PerturbationType.OMIT_OBVIOUS_CLAUSES: "Removed explicit SQL clause keywords.",
    PerturbationType.SYNONYM_SUBSTITUTION: "Replaced query action verbs with synonyms.",
    PerturbationType.VERBOSITY_VARIATION: "Inserted conversational fillers.",
    PerturbationType.OPERATOR_AGGREGATE_VARIATION: "Varied operator/aggregate format.",
    PerturbationType.TYPOS: "Injected keyboard typos.",
    PerturbationType.COMMENT_ANNOTATIONS: "Added SQL comments/notes.",
    PerturbationType.TEMPORAL_EXPRESSION_VARIATION: "Used relative temporal terms.",
    PerturbationType.PUNCTUATION_VARIATION: "Modified sentence rhythm.",
    PerturbationType.URGENCY_QUALIFIERS: "Added urgency markers.",
    PerturbationType.MIXED_SQL_NL: "Blended raw SQL keywords.",
    PerturbationType.TABLE_COLUMN_SYNONYMS: "Used human-centric schema synonyms.",
    PerturbationType.INCOMPLETE_JOIN_SPEC: "Omitted explicit JOIN/ON syntax.",
    PerturbationType.AMBIGUOUS_PRONOUNS: "Replaced one reference with it/that."
}

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        queries = json.load(f)

    output_data = []
    base_renderer = SQLToNLRenderer()
    
    print(f"Processing {len(queries)} queries for systematic perturbations...")
    
    for i, query_item in enumerate(queries):
        sql = query_item['sql']
        
        output_item = {
            "id": query_item.get('id', i+1),
            "sql": sql,
            "generated_perturbations": {
                "original": {"nl_prompt": query_item.get('nl_prompt', '')},
                "single_perturbations": [],
                "metadata": {}
            }
        }
        
        try:
            ast = parse_one(sql, dialect='mysql')
        except Exception:
            continue

        applicable_count = 0
        for p_type in PerturbationType:
            is_app = base_renderer.is_applicable(ast, p_type)
            entry = {"perturbation_name": p_type.value, "applicable": is_app, "perturbed_nl_prompt": None}
            
            if is_app:
                config = PerturbationConfig(active_perturbations={p_type}, seed=i*100)
                try:
                    entry["perturbed_nl_prompt"] = SQLToNLRenderer(config).render(ast)
                    entry["changes_made"] = PERTURBATION_DESCRIPTIONS[p_type]
                    applicable_count += 1
                except Exception as e:
                    entry["applicable"] = False
            
            output_item["generated_perturbations"]["single_perturbations"].append(entry)
            
        output_item["generated_perturbations"]["metadata"]["total_applicable"] = applicable_count
        output_data.append(output_item)
        
        if (i+1) % 50 == 0: print(f"Progress: {i+1} queries...")
            
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    print(f"Dataset generated at {OUTPUT_FILE}")

if __name__ == "__main__":
    main()