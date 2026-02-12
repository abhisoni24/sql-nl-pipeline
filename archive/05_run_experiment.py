"""
Central Orchestrator CLI.
"""
import argparse
import sys
import os
import uuid
import json

# Ensure src is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.harness.config import ConfigLoader
from src.harness.core.execution import ExecutionEngine

def main():
    parser = argparse.ArgumentParser(description="Central LLM Evaluation Harness")
    parser.add_argument("--config", required=True, help="Path to experiments.yaml")
    parser.add_argument("--prompts", required=True, help="Path to input prompts JSON/JSONL")
    parser.add_argument("--model", help="Specific model name from config to run")
    parser.add_argument("--output", default="experiment_results.jsonl", help="Output log file")
    parser.add_argument("--run-id", help="Resume/Append to specific Run ID")
    
    args = parser.parse_args()
    
    # Load Experiments
    experiments = ConfigLoader.load_experiments(args.config)
    
    # Filter if model specified
    if args.model:
        experiments = [e for e in experiments if e.name == args.model]
        if not experiments:
             print(f"Model '{args.model}' not found in config.")
             return

    # Run ID
    run_id = args.run_id or str(uuid.uuid4())
    print(f"Experiment Run ID: {run_id}")
    
    # Already processed prompt IDs (Resume logic)
    processed_ids = set()
    if os.path.exists(args.output):
        with open(args.output, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        if record['run_id'] == run_id:
                            processed_ids.add(record['prompt_id'])
                    except json.JSONDecodeError:
                        pass
    print(f"Found {len(processed_ids)} already processed prompts for this Run ID.")

    for experiment in experiments:
        print(f"\nRunning Experiment: {experiment.name} (Adapter: {experiment.adapter_type})")
        
        try:
            adapter = ConfigLoader.get_adapter(experiment)
            
            # Pass rate limit config to execution engine
            engine = ExecutionEngine(
                adapter=adapter,
                run_id=run_id,
                output_path=args.output,
                rate_limit_config=experiment.rate_limit  # New: pass rate limit settings
            )
            
            # Simple resume logic by filtering processed_ids is best done here 
            # or we rely on the engine? Engine loads from file. 
            # If we want to skip already processed, we should pass that info to engine 
            # or pre-filter the file. 
            # Modifying ExecutionEngine._load_prompts dynamically:
            
            original_load = engine._load_prompts
            def load_and_filter(path):
                data = original_load(path)
                filtered = [d for d in data if str(d.get('id', '')) not in processed_ids]
                if len(filtered) < len(data):
                    print(f"Skipping {len(data) - len(filtered)} already processed items.")
                return filtered
            
            engine._load_prompts = load_and_filter
            
            engine.execute_experiment(args.prompts)
            
        except Exception as e:
            print(f"Failed to run experiment {experiment.name}: {e}")
            # Continue to next experiment

if __name__ == "__main__":
    main()
