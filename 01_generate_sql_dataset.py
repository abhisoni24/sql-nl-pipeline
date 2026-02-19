import json
import random
import sys
import os

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.core.generator import SQLQueryGenerator
from src.core.schema import SCHEMA, FOREIGN_KEYS

from sqlglot import exp

def main():
    generator = SQLQueryGenerator(SCHEMA, FOREIGN_KEYS)
    output_data = []
    
    print("Generating queries...")
    
    # Generate 300 queries per complexity type
    output_data = generator.generate_dataset(num_per_complexity=50)
            
    print(f"Successfully generated {len(output_data)} queries.")
    
    output_file = './dataset/current/raw_social_media_queries_20.json'
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    main()
