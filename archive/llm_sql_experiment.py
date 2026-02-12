"""
LLM SQL Generation Experiment Notebook
======================================
This notebook runs SQL generation experiments using three LLM providers:
1. Gemini (gemini-2.5-flash-lite)
2. OpenAI/ChatGPT (gpt-4o)
3. Anthropic/Claude (claude-haiku-4-5-20251001)

Pipeline:
1. Load dataset (nl_social_media_queries_20.json)
2. Generate SQL from each model using NL prompts + schema context
3. Extract SQL from raw responses
4. Run equivalence evaluation against gold SQL
5. Report analytics and detailed results
"""

import json
import os
import sys
import re
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# ============================================================================
# 1. CONFIGURATION
# ============================================================================

# Models to test (from experiments.yaml)
MODELS = {
    "gemini": {
        "adapter": "gemini",
        "model_id": "gemini-2.5-flash-lite",
        "rpm": 2000
    },
    "openai": {
        "adapter": "openai", 
        "model_id": "gpt-4o",
        "rpm": 500
    },
    "anthropic": {
        "adapter": "anthropic",
        "model_id": "claude-haiku-4-5-20251001",
        "rpm": 5
    }
}

# Paths
INPUT_DATASET = "./dataset/current/nl_social_media_queries_20.json"
OUTPUT_RESULTS = "./dataset/current/llm_generation_results.json"

# Schema for prompts (from src/core/schema.py)
SCHEMA_CONTEXT = """
Database Schema (SQLite):

Table: users
  - id (INTEGER, PRIMARY KEY)
  - username (TEXT)
  - email (TEXT)
  - signup_date (DATETIME)
  - is_verified (INTEGER, 0 or 1)
  - country_code (TEXT)

Table: posts
  - id (INTEGER, PRIMARY KEY)
  - user_id (INTEGER, FOREIGN KEY -> users.id)
  - content (TEXT)
  - posted_at (DATETIME)
  - view_count (INTEGER)

Table: comments
  - id (INTEGER, PRIMARY KEY)
  - user_id (INTEGER, FOREIGN KEY -> users.id)
  - post_id (INTEGER, FOREIGN KEY -> posts.id)
  - comment_text (TEXT)
  - created_at (DATETIME)

Table: likes
  - user_id (INTEGER, FOREIGN KEY -> users.id)
  - post_id (INTEGER, FOREIGN KEY -> posts.id)
  - liked_at (DATETIME)
  - PRIMARY KEY (user_id, post_id)

Table: follows
  - follower_id (INTEGER, FOREIGN KEY -> users.id)
  - followee_id (INTEGER, FOREIGN KEY -> users.id)
  - followed_at (DATETIME)
  - PRIMARY KEY (follower_id, followee_id)
"""

SYSTEM_PROMPT = """You are a SQL expert. Generate a valid SQLite SQL statement for the given natural language query.

IMPORTANT RULES:
1. Return ONLY the raw SQL statement - no explanations, no markdown code blocks, no comments.
2. Use SQLite syntax (e.g., datetime() for dates, 1/0 for booleans).
3. The SQL must be executable against the provided schema.
4. For DML statements (INSERT, UPDATE, DELETE), generate valid statements that respect constraints.
"""

# ============================================================================
# 2. ADAPTER INITIALIZATION
# ============================================================================

def create_adapter(adapter_type: str, model_id: str):
    """Create an adapter instance for the specified model."""
    if adapter_type == "gemini":
        from src.harness.adapters.gemini import GeminiAdapter
        return GeminiAdapter(model_name=model_id)
    elif adapter_type == "openai":
        from src.harness.adapters.openai import OpenAIAdapter
        return OpenAIAdapter(model_name=model_id)
    elif adapter_type == "anthropic":
        from src.harness.adapters.anthropic import AnthropicAdapter
        return AnthropicAdapter(model_name=model_id)
    else:
        raise ValueError(f"Unknown adapter type: {adapter_type}")

# ============================================================================
# 3. SQL EXTRACTION
# ============================================================================

def extract_sql_from_response(raw_response: str) -> str:
    """
    Extract SQL statement from raw LLM response.
    Handles various response formats:
    - Pure SQL
    - SQL in markdown code blocks
    - SQL with explanations
    """
    if not raw_response or not raw_response.strip():
        return ""
    
    text = raw_response.strip()
    
    # Pattern 1: SQL in markdown code block (```sql ... ``` or ``` ... ```)
    # We use \w* to skip any language identifier like 'sql' or 'sqlite'
    code_block_pattern = r'```(?:\w*)\s*([\s\S]*?)```'
    matches = re.findall(code_block_pattern, text, re.IGNORECASE)
    if matches:
        return matches[0].strip()
    
    # Pattern 2: Search for the first valid SQL keyword used at the start of a statement
    sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CREATE', 'DROP', 'ALTER']
    
    # Find the earliest occurrence of any keyword
    # We look for word boundaries to avoid catching keywords inside other words
    pattern = r'\b(' + '|'.join(sql_keywords) + r')\b'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        sql_start_pos = match.start()
        # Take the text from the first keyword onwards
        sql_text = text[sql_start_pos:]
        
        # Now clean up any trailing non-SQL text (explanations after the statement)
        # We assume the SQL statement ends at the last semicolon or at a double newline
        # if no semicolon is present
        if ';' in sql_text:
            sql_text = sql_text.split(';')[0] + ';'
        else:
            # Look for double newline as a separator if no semicolon
            parts = re.split(r'\n\s*\n', sql_text)
            if parts:
                sql_text = parts[0]
        
        return sql_text.strip()
    
    return text

# ============================================================================
# 4. EXPERIMENT EXECUTION
# ============================================================================

@dataclass
class ExperimentResult:
    query_id: int
    complexity: str
    gold_sql: str
    nl_prompt: str
    model_results: Dict[str, Dict[str, Any]]  # model -> {raw_response, extracted_sql, error}

def load_dataset(path: str) -> List[Dict]:
    """Load the input dataset."""
    with open(path, 'r') as f:
        return json.load(f)

def build_prompt(nl_prompt: str) -> str:
    """Build the full prompt with schema context."""
    return f"""{SCHEMA_CONTEXT}

Natural Language Query:
{nl_prompt}

Generate the SQLite SQL statement:"""

def run_generation_for_model(
    model_name: str,
    adapter,
    prompts: List[str],
    rpm: int = 60
) -> List[Dict[str, Any]]:
    """Run generation for a single model on all prompts."""
    results = []
    delay = 60.0 / rpm  # Delay between requests to respect rate limit
    
    print(f"\n  Running {model_name} ({len(prompts)} prompts, {rpm} RPM)...")
    
    for i, prompt in enumerate(tqdm(prompts, desc=f"  {model_name}")):
        start = time.time()
        
        try:
            # Generate using adapter
            response = adapter.generate([prompt])
            raw_response = response[0] if response else ""
            extracted_sql = extract_sql_from_response(raw_response)
            error = None
        except Exception as e:
            raw_response = ""
            extracted_sql = ""
            error = str(e)
        
        results.append({
            "raw_response": raw_response,
            "extracted_sql": extracted_sql,
            "error": error
        })
        
        # Rate limiting
        elapsed = time.time() - start
        if elapsed < delay and i < len(prompts) - 1:
            time.sleep(delay - elapsed)
    
    return results

def run_experiment(dataset_path: str) -> List[ExperimentResult]:
    """Run the full experiment across all models."""
    # Load dataset
    dataset = load_dataset(dataset_path)
    print(f"Loaded {len(dataset)} queries from {dataset_path}")
    
    # Build prompts
    prompts = [build_prompt(item['nl_prompt']) for item in dataset]
    
    # Initialize results
    results = []
    for item in dataset:
        results.append(ExperimentResult(
            query_id=item['id'],
            complexity=item['complexity'],
            gold_sql=item['sql'],
            nl_prompt=item['nl_prompt'],
            model_results={}
        ))
    
    # Run each model
    for model_name, config in MODELS.items():
        print(f"\n{'='*60}")
        print(f"Running Model: {model_name} ({config['model_id']})")
        print(f"{'='*60}")
        
        try:
            adapter = create_adapter(config['adapter'], config['model_id'])
            model_results = run_generation_for_model(
                model_name=model_name,
                adapter=adapter,
                prompts=prompts,
                rpm=config['rpm']
            )
            
            # Merge into results
            for i, mr in enumerate(model_results):
                results[i].model_results[model_name] = mr
                
        except Exception as e:
            print(f"  ERROR initializing {model_name}: {e}")
            for i in range(len(results)):
                results[i].model_results[model_name] = {
                    "raw_response": "",
                    "extracted_sql": "",
                    "error": str(e)
                }
    
    return results

def save_results(results: List[ExperimentResult], path: str):
    """Save results to JSON."""
    data = [asdict(r) for r in results]
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {path}")

# ============================================================================
# 5. EQUIVALENCE EVALUATION
# ============================================================================

def run_equivalence_evaluation(results: List[ExperimentResult]) -> Dict[str, Any]:
    """
    Run SQL equivalence evaluation for all model-generated SQL against gold SQL.
    Returns detailed analytics.
    """
    from src.equivalence import SQLEquivalenceEngine
    from src.core.schema import SCHEMA
    
    # Initialize equivalence engine
    engine = SQLEquivalenceEngine.from_schema(
        schema=SCHEMA,
        foreign_keys={},
        db_path='./test_dbs/eval_experiment.sqlite'
    )
    
    # Store evaluation results
    eval_results = {
        "summary": {},
        "by_complexity": {},
        "detailed": []
    }
    
    # Per-model stats
    model_stats = {model: {"correct": 0, "incorrect": 0, "error": 0} for model in MODELS}
    complexity_stats = {}
    
    print("\n" + "="*60)
    print("RUNNING EQUIVALENCE EVALUATION")
    print("="*60)
    
    for result in tqdm(results, desc="Evaluating"):
        gold_sql = result.gold_sql
        detail_entry = {
            "query_id": result.query_id,
            "complexity": result.complexity,
            "gold_sql": gold_sql,
            "nl_prompt": result.nl_prompt,
            "model_evaluations": {}
        }
        
        # Initialize complexity stats
        if result.complexity not in complexity_stats:
            complexity_stats[result.complexity] = {
                model: {"correct": 0, "incorrect": 0, "error": 0} 
                for model in MODELS
            }
        
        for model_name in MODELS:
            mr = result.model_results.get(model_name, {})
            gen_sql = mr.get("extracted_sql", "")
            
            if not gen_sql:
                # Empty generation = error
                model_stats[model_name]["error"] += 1
                complexity_stats[result.complexity][model_name]["error"] += 1
                detail_entry["model_evaluations"][model_name] = {
                    "generated_sql": "",
                    "status": "GENERATION_ERROR",
                    "error": mr.get("error", "Empty response")
                }
                continue
            
            try:
                # Run equivalence check
                check_result = engine.check_equivalence(gold_sql, gen_sql)
                
                if check_result.is_equivalent:
                    model_stats[model_name]["correct"] += 1
                    complexity_stats[result.complexity][model_name]["correct"] += 1
                    status = "PASS"
                else:
                    model_stats[model_name]["incorrect"] += 1
                    complexity_stats[result.complexity][model_name]["incorrect"] += 1
                    status = "FAIL"
                
                detail_entry["model_evaluations"][model_name] = {
                    "generated_sql": gen_sql,
                    "status": status,
                    "equivalence_details": asdict(check_result)
                }
                
            except Exception as e:
                model_stats[model_name]["error"] += 1
                complexity_stats[result.complexity][model_name]["error"] += 1
                detail_entry["model_evaluations"][model_name] = {
                    "generated_sql": gen_sql,
                    "status": "EXECUTION_ERROR",
                    "error": str(e)
                }
        
        eval_results["detailed"].append(detail_entry)
    
    # Compute summary
    total = len(results)
    for model_name, stats in model_stats.items():
        accuracy = stats["correct"] / total * 100 if total > 0 else 0
        eval_results["summary"][model_name] = {
            "total": total,
            "correct": stats["correct"],
            "incorrect": stats["incorrect"],
            "error": stats["error"],
            "accuracy": round(accuracy, 2)
        }
    
    eval_results["by_complexity"] = complexity_stats
    
    # Cleanup
    engine.cleanup()
    
    return eval_results

def print_analytics(eval_results: Dict[str, Any]):
    """Print formatted analytics report."""
    print("\n" + "="*70)
    print("EXPERIMENT ANALYTICS REPORT")
    print("="*70)
    
    # Overall summary
    print("\n📊 OVERALL ACCURACY BY MODEL:")
    print("-"*50)
    print(f"{'Model':<20} {'Correct':<10} {'Fail':<10} {'Error':<10} {'Accuracy':<10}")
    print("-"*50)
    
    for model, stats in eval_results["summary"].items():
        print(f"{model:<20} {stats['correct']:<10} {stats['incorrect']:<10} {stats['error']:<10} {stats['accuracy']:.1f}%")
    
    # By complexity
    print("\n\n📈 ACCURACY BY COMPLEXITY:")
    print("-"*70)
    
    complexities = eval_results["by_complexity"].keys()
    models = list(MODELS.keys())
    
    header = f"{'Complexity':<15}"
    for model in models:
        header += f" {model:<18}"
    print(header)
    print("-"*70)
    
    for complexity in sorted(complexities):
        row = f"{complexity:<15}"
        for model in models:
            stats = eval_results["by_complexity"][complexity][model]
            total = stats["correct"] + stats["incorrect"] + stats["error"]
            acc = stats["correct"] / total * 100 if total > 0 else 0
            row += f" {stats['correct']}/{total} ({acc:.0f}%)".ljust(18)
        print(row)
    
    # Sample failures
    print("\n\n❌ SAMPLE FAILURES (first 3 per model):")
    print("-"*70)
    
    for model in models:
        print(f"\n{model.upper()}:")
        failures = [
            d for d in eval_results["detailed"]
            if d["model_evaluations"].get(model, {}).get("status") in ["FAIL", "EXECUTION_ERROR"]
        ][:3]
        
        if not failures:
            print("  No failures!")
        else:
            for f in failures:
                eval_info = f["model_evaluations"][model]
                print(f"  ID {f['query_id']} ({f['complexity']}): {eval_info['status']}")
                print(f"    Gold: {f['gold_sql'][:60]}...")
                print(f"    Gen:  {eval_info.get('generated_sql', '')[:60]}...")

# ============================================================================
# 6. MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("LLM SQL GENERATION EXPERIMENT")
    print(f"Started: {datetime.now().isoformat()}")
    print("="*70)
    
    # Step 1: Run generation experiment
    results = run_experiment(INPUT_DATASET)
    
    # Step 2: Save raw results
    save_results(results, OUTPUT_RESULTS)
    
    # Step 3: Run equivalence evaluation
    eval_results = run_equivalence_evaluation(results)
    
    # Step 4: Save evaluation results
    eval_output = OUTPUT_RESULTS.replace('.json', '_evaluation.json')
    with open(eval_output, 'w') as f:
        json.dump(eval_results, f, indent=2, default=str)
    print(f"Evaluation results saved to {eval_output}")
    
    # Step 5: Print analytics
    print_analytics(eval_results)
    
    print("\n" + "="*70)
    print(f"EXPERIMENT COMPLETE: {datetime.now().isoformat()}")
    print("="*70)
