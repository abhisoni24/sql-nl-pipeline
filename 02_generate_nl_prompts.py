"""
Pipeline to generate natural language prompts with variations from SQL queries.
Loads social_media_queries.json, parses each SQL, renders vanilla + 3 variations, and updates JSON.
"""

import json
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def generate_nl_prompts(input_file='./dataset/current/raw_social_media_queries_20.json', output_file='./dataset/current/nl_social_media_queries_20.json'):
    """Generate NL prompts with variations for all SQL queries in the dataset."""
    
    # Load existing queries
    print(f"Loading {input_file}...")
    with open(input_file, 'r') as f:
        queries = json.load(f)
    
    print(f"Loaded {len(queries)} queries.")
    
    # Initialize renderer
    renderer = SQLToNLRenderer()
    
    # Process each query
    print("Generating natural language prompts with variations...")
    success_count = 0
    error_count = 0
    
    for i, query_data in enumerate(queries):
        sql = query_data['sql']
        
        try:
            # Parse SQL
            ast = parse_one(sql, dialect=USED_SQL_DIALECT)
            
            # Generate vanilla with unique seed per query
            renderer.config.seed = 42 + i
            vanilla_prompt = renderer.render(ast)
            
            # Add to data
            query_data['nl_prompt'] = vanilla_prompt

            success_count += 1
            
        except Exception as e:
            print(f"Error processing query {i}: {sql[:50]}... - {e}")
            query_data['nl_prompt'] = "[Error: Could not generate NL prompt]"
            error_count += 1
    
    print(f"Successfully rendered {success_count} natural language prompts, {error_count} errors.")
    
    # Save updated JSON
    print(f"Saving to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(queries, f, indent=2)
    
    print("Done!")
    
    # Print a few examples
    print("\n" + "="*80)
    print("Sample Natural Language Prompts:")
    print("="*80)
    for i in range(min(3, len(queries))):
        print(f"\nQuery {i+1}:")
        print(f"SQL: {queries[i]['sql']}")
        print(f"Vanilla: {queries[i]['nl_prompt']}")
    print("="*80)


if __name__ == "__main__":
    generate_nl_prompts()
