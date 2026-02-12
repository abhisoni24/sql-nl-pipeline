import json

input_path = '/Users/obby/Documents/experiment/random/sql-nl/sample_exp_run/20each/output/evaluated_results_aggregated.jsonl'
count = 0

try:
    with open(input_path, 'r') as f:
        for line in f:
            try:
                data = json.loads(line)
                response = str(data.get('generated_response', '')).strip()
                if len(response) < 10:
                    print(f"Short Response (len={len(response)}): '{response}'")
                    print(f"Query ID: {data.get('query_id')}")
                    print(f"Model Name: {data.get('model_name', 'unknown')}")
                    print("-" * 40)
                    count += 1
                    if count >= 20:
                        break
            except json.JSONDecodeError:
                continue
except FileNotFoundError:
    print("File not found.")
