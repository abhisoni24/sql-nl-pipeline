"""
Pipeline to generate natural language prompts with variations from SQL queries.
Loads social_media_queries.json, parses each SQL, renders vanilla + 3 variations, and updates JSON.

Supports two rendering modes:
  - Legacy (default): Direct render() producing final NL text using hardcoded synonym banks.
  - Two-pass (--two-pass): render_template() → TemplateResolver.resolve() using a LinguisticDictionary
    built from a schema YAML, enabling schema-agnostic NL rendering.
"""

import argparse
import json
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def generate_nl_prompts(input_file='./dataset/current/raw_social_media_queries_20.json',
                        output_file='./dataset/current/nl_social_media_queries_20.json',
                        two_pass=False, schema_path=None, dictionary_path=None):
    """Generate NL prompts with variations for all SQL queries in the dataset.

    Parameters
    ----------
    input_file : str
        Path to the raw SQL dataset JSON.
    output_file : str
        Path to write the NL-augmented dataset JSON.
    two_pass : bool
        If True, use the two-pass pipeline (render_template + TemplateResolver).
    schema_path : str or None
        Path to schema YAML (used when two_pass=True and no dictionary_path).
    dictionary_path : str or None
        Path to a pre-built dictionary YAML. When provided with --two-pass,
        loads the verified dictionary instead of generating one on-the-fly.
    """
    
    # Load existing queries
    print(f"Loading {input_file}...")
    with open(input_file, 'r') as f:
        queries = json.load(f)
    
    print(f"Loaded {len(queries)} queries.")
    
    # Initialize renderer
    renderer = SQLToNLRenderer()

    # Set up two-pass pipeline if requested
    resolver = None
    if two_pass:
        from src.core.template_resolver import TemplateResolver

        if dictionary_path:
            # Load pre-verified dictionary (recommended workflow)
            from src.core.dictionary_builder import load_dictionary
            dictionary = load_dictionary(dictionary_path)
            resolver = TemplateResolver(dictionary, seed=42)
            print(f"Two-pass mode: loaded verified dictionary from {dictionary_path}")
        else:
            # Fallback: build on-the-fly (not recommended for production)
            if not schema_path:
                schema_path = 'schemas/social_media.yaml'
                print(f"No --schema provided, defaulting to {schema_path}")
            from src.core.schema_loader import load_from_yaml
            from src.core.dictionary_builder import build_dictionary
            schema_cfg = load_from_yaml(schema_path)
            dictionary = build_dictionary(schema_cfg, use_wordnet=True)
            resolver = TemplateResolver(dictionary, seed=42)
            print(f"WARNING: Building dictionary on-the-fly (synonyms not reviewed).")
            print(f"  Consider using: python generate_dictionary.py --schema {schema_path}")
            print(f"  Then pass the reviewed YAML via: --dictionary <path>")
            print(f"Two-pass mode: using schema '{schema_cfg.schema_name}' with dictionary resolver")
    
    # Process each query
    mode_label = "two-pass" if two_pass else "legacy"
    print(f"Generating natural language prompts ({mode_label} mode)...")
    success_count = 0
    error_count = 0
    
    for i, query_data in enumerate(queries):
        sql = query_data['sql']
        
        try:
            # Parse SQL
            ast = parse_one(sql, dialect=USED_SQL_DIALECT)
            
            # Generate vanilla with unique seed per query
            renderer.config.seed = 42 + i

            if two_pass and resolver is not None:
                # Pass 1: AST → IR template
                template = renderer.render_template(ast)
                # Pass 2: IR → NL (dictionary-based)
                resolver.rng.seed(42 + i)
                vanilla_prompt = resolver.resolve(template)
                # Store template for downstream analysis
                query_data['ir_template'] = template
            else:
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
        if 'ir_template' in queries[i]:
            print(f"Template: {queries[i]['ir_template']}")
    print("="*80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate NL prompts from SQL queries")
    parser.add_argument('--input', '-i', default='./dataset/current/raw_social_media_queries_20.json',
                        help='Input raw SQL dataset JSON')
    parser.add_argument('--output', '-o', default='./dataset/current/nl_social_media_queries_20.json',
                        help='Output NL-augmented dataset JSON')
    parser.add_argument('--two-pass', action='store_true',
                        help='Use two-pass rendering (render_template + TemplateResolver)')
    parser.add_argument('--schema', default=None,
                        help='Path to schema YAML (fallback when --dictionary not provided)')
    parser.add_argument('--dictionary', default=None,
                        help='Path to a pre-verified dictionary YAML (use with --two-pass)')
    args = parser.parse_args()
    generate_nl_prompts(input_file=args.input, output_file=args.output,
                        two_pass=args.two_pass, schema_path=args.schema,
                        dictionary_path=args.dictionary)
