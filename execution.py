"""
Execution Engine.
Orchestrates the generation and evaluation loop.
"""
import json
import os
import time
import uuid
import sys
import threading
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..adapters.base import BaseModelAdapter
from .normalization import normalize_sql
from .evaluation import Evaluator

# Import schema for prompt construction
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from src.core.schema import SCHEMA, FOREIGN_KEYS


class TokenBucket:
    """
    Token bucket rate limiter for precise rate limiting.
    Allows bursts while maintaining average rate over time.
    """
    
    def __init__(self, rate_per_minute: float, max_tokens: Optional[float] = None):
        """
        Args:
            rate_per_minute: Maximum requests per minute
            max_tokens: Maximum bucket capacity (defaults to rate_per_minute)
        """
        self.rate_per_second = rate_per_minute / 60.0
        self.max_tokens = max_tokens or rate_per_minute
        self.tokens = self.max_tokens
        self.last_update = time.time()
        self.lock = threading.Lock()
    
    def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens from the bucket, blocking if necessary.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            Time waited in seconds
        """
        wait_time = 0.0
        
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Refill bucket based on elapsed time
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.rate_per_second)
            self.last_update = now
            
            # Check if we have enough tokens
            if self.tokens >= tokens:
                self.tokens -= tokens
            else:
                # Calculate wait time for required tokens
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate_per_second
                self.tokens = 0
        
        # Wait outside the lock to allow other threads to check
        if wait_time > 0:
            time.sleep(wait_time)
        
        return wait_time

class ExecutionEngine:
    
    def __init__(self, adapter: BaseModelAdapter, run_id: str, output_path: str, rate_limit_config: Dict[str, Any] = None):
        self.adapter = adapter
        self.run_id = run_id
        self.output_path = output_path
        self.evaluator = Evaluator()
        self.schema = SCHEMA
        self.foreign_keys = FOREIGN_KEYS
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Rate limiting configuration
        self.rate_limit_config = rate_limit_config or {}
        self.requests_per_minute = self.rate_limit_config.get('requests_per_minute', None)
        self.max_retries = self.rate_limit_config.get('max_retries', 0)
        
        # Calculate delay between batches if rate limiting is enabled
        if self.requests_per_minute:
            # Add a small buffer to be safe
            self.batch_delay = (60.0 / self.requests_per_minute) * 1.1
            print(f"⏱️  Rate limiting enabled: {self.requests_per_minute} req/min (delay: {self.batch_delay:.1f}s between batches)")
        else:
            self.batch_delay = 0
            print(f"⚡ No rate limiting (local model or unlimited API)")
        
        # Determine batch size
        if adapter.model_family() == "open": # VLLM
            self.batch_size = 32
        else: # API
            self.batch_size = 5
        
        # Concurrent execution settings
        self.max_concurrent = self.rate_limit_config.get('max_concurrent_requests', None)
        
        # Initialize token bucket if rate limiting is enabled
        self.token_bucket = None
        if self.requests_per_minute:
            self.token_bucket = TokenBucket(rate_per_minute=self.requests_per_minute)
            
            # Set default max_concurrent based on rate limit if not specified
            if self.max_concurrent is None:
                # Conservative default: allow enough concurrency to reach rate limit
                # but not so much that we overwhelm the API
                self.max_concurrent = min(100, int(self.requests_per_minute / 10))
            
            print(f"🚀 Concurrent execution enabled: {self.max_concurrent} max concurrent requests")
        else:
            # No rate limiting means we can use higher concurrency for local models
            if self.max_concurrent is None:
                self.max_concurrent = 32  # Good default for local models
            print(f"🚀 Concurrent execution enabled: {self.max_concurrent} max concurrent requests (no rate limit)")

    def execute_experiment(self, prompts_path: str):
        """End-to-end execution: load, generate concurrently or in batch, normalize, evaluate, log."""
        # 1. Load Prompts (extracts all perturbations)
        prompts_data = self._load_prompts(prompts_path)
        print(f"Loaded {len(prompts_data)} prompts from {prompts_path}")

        # 2. Execute with concurrency
        print(f"Processing {len(prompts_data)} prompts with max {self.max_concurrent} concurrent requests...")

        # Track statistics for progress tracking
        total_processed = 0
        total_correct = 0
        
        # Thread-safe counter and lock for statistics
        stats_lock = threading.Lock()
        
        pbar_lock = threading.Lock()
        
        def process_single_prompt(prompt_item: Dict[str, Any]):
            """Process a single prompt: generate, normalize, evaluate, return record."""
            nonlocal total_processed, total_correct
            
            # Rate limiting: acquire token before making request
            if self.token_bucket:
                self.token_bucket.acquire(1)
            
            # Construct full prompt with schema context
            prompt_text = self._construct_full_prompt(prompt_item['prompt_text'])
            
            # Generate (adapter handles single prompt)
            try:
                raw_output = self.adapter.generate([prompt_text])[0]
            except Exception as e:
                import logging
                logging.error(f"Generation failed for prompt {prompt_item.get('prompt_id', 'unknown')}: {e}")
                raw_output = ""
            
            # Process the result
            record, is_correct = self._create_record_from_result(prompt_item, raw_output)
            
            # Update statistics (thread-safe)
            with stats_lock:
                total_processed += 1
                if is_correct:
                    total_correct += 1
            
            # Log immediately
            self._log_record(record)
            
            return record

        def update_pbar(pbar, _total_processed, _total_correct):        
            current_accuracy = (_total_correct / _total_processed * 100) if _total_processed > 0 else 0
            pbar.set_postfix_str(f"Correct: {_total_correct}/{_total_processed} ({current_accuracy:.1f}%)")
            pbar.update(1)

        # Choose execution strategy based on model family
        start_time = time.time()
        if self.adapter.model_family() == "open":
            # BATCHED EXECUTION for local models (vLLM)
            print("🚀 Using BATCHED execution for local model (chunk_size=500)...")
            chunk_size = 500
            
            with tqdm(total=len(prompts_data), desc="Processing Prompts", unit="prompt") as pbar:
                for i in range(0, len(prompts_data), chunk_size):
                    chunk = prompts_data[i:i + chunk_size]
                    
                    # 1. Prepare prompts
                    chunk_prompts = [self._construct_full_prompt(p['prompt_text']) for p in chunk]
                    
                    # 2. Generate batch
                    try:
                        chunk_outputs = self.adapter.generate(chunk_prompts)
                    except Exception as e:
                        print(f"ERROR: Batch generation failed: {e}")
                        chunk_outputs = [""] * len(chunk)
                    
                    # 3. Process results
                    for prompt_item, raw_output in zip(chunk, chunk_outputs):
                        record, is_correct = self._create_record_from_result(prompt_item, raw_output)
                        self._log_record(record)
                        
                        # Update progress locally for the chunk loop
                        total_processed += 1
                        if is_correct:
                            total_correct += 1
                        
                        # Update pbar
                        update_pbar(pbar, total_processed, total_correct)
                        
        else:
            # CONCURRENT EXECUTION for API models
            with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
                # Submit all tasks
                future_to_prompt = {
                    executor.submit(process_single_prompt, prompt): prompt 
                    for prompt in prompts_data
                }
                
                # Process completed tasks with progress bar
                with tqdm(total=len(prompts_data), desc="Processing Prompts", unit="prompt") as pbar:
                    for future in as_completed(future_to_prompt):
                        try:
                            future.result()
                            # Stats are updated inside process_single_prompt
                            update_pbar(pbar, total_processed, total_correct)
                            
                        except Exception as e:
                            import logging
                            logging.error(f"Task failed: {e}")
                            pbar.update(1)
        
        # Calculate throughput
        elapsed_time = time.time() - start_time
        throughput_rpm = (len(prompts_data) / elapsed_time) * 60 if elapsed_time > 0 else 0
        
        # Print final summary
        final_accuracy = (total_correct / total_processed * 100) if total_processed > 0 else 0
        print(f"\\n{'='*70}")
        print(f"Execution Complete!")
        print(f"  Total Processed: {total_processed}")
        print(f"  Total Correct: {total_correct}")
        print(f"  Final Accuracy: {final_accuracy:.2f}%")
        print(f"  Time Elapsed: {elapsed_time:.1f}s")
        print(f"  Throughput: {throughput_rpm:.1f} requests/minute")
        if self.requests_per_minute:
            utilization = (throughput_rpm / self.requests_per_minute * 100)
            print(f"  Rate Limit Utilization: {utilization:.1f}%")
        print(f"{'='*70}")

    def _create_record_from_result(self, prompt_item: Dict[str, Any], raw_output: str) -> Tuple[Dict[str, Any], bool]:
        """Helper to creation record and update stats safely."""
        # Normalize
        normalized_sql = normalize_sql(raw_output)
        
        # Evaluate
        gold_sql = prompt_item.get('sql', '')
        eval_result = self.evaluator.evaluate(gold_sql, normalized_sql)
        
        record = {
            "run_id": self.run_id,
            "model_name": self.adapter.model_name(),
            "model_family": self.adapter.model_family(),
            "prompt_id": prompt_item.get('prompt_id', str(uuid.uuid4())),
            "prompt_text": prompt_item['prompt_text'],
            "perturbation_type": prompt_item.get('perturbation_type', 'unknown'),
            "perturbation_id": prompt_item.get('perturbation_id', -1),
            "query_complexity": prompt_item.get('complexity', 'unknown'),
            "tables_involved": prompt_item.get('tables', []),
            "gold_sql": gold_sql,
            "raw_output": raw_output,
            "normalized_sql": normalized_sql,
            "evaluation_result": {
                "correctness": eval_result.execution_match,
                "similarity_score": eval_result.similarity_score,
                "failure_type": eval_result.failure_type
            },
            "decoding_config": self.adapter.decoding_config(),
            "metadata": prompt_item.get('metadata', {}),
            "timestamp": datetime.utcnow().isoformat()
        }
        return record, eval_result.execution_match

    
    def _generate_with_retry(self, prompt_texts: List[str]) -> List[str]:
        """
        Generate with retry logic for rate limit errors.
        
        Args:
            prompt_texts: List of prompts to generate for
            
        Returns:
            List of generated SQL strings (empty string on failure)
        """
        for attempt in range(self.max_retries + 1):
            try:
                return self.adapter.generate(prompt_texts)
            except Exception as e:
                error_str = str(e)
                # Check if it's a rate limit error (429 or RESOURCE_EXHAUSTED) or Service Unavailable (503)
                is_rate_limit = '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'rate' in error_str.lower() or '503' in error_str
                
                if is_rate_limit and attempt < self.max_retries:
                    # Exponential backoff: 2, 4, 8 seconds
                    delay = 2 ** (attempt + 1)
                    print(f"  ⚠️  Rate limit hit, retrying in {delay}s... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(delay)
                else:
                    # Non-rate-limit error or out of retries - return empty strings
                    import logging
                    logging.error(f"Generation failed after {attempt + 1} attempts: {error_str}")
                    return [""] * len(prompt_texts)
        
        # Should never reach here, but just in case
        return [""] * len(prompt_texts)
                
    def _load_prompts(self, path: str) -> List[Dict[str, Any]]:
        """
        Load prompts from JSON or JSONL, extracting ALL perturbations.
        
        For each query in the dataset, this creates:
        - 1 test case for the original prompt
        - N test cases for each applicable perturbation
        
        This expands ~2100 queries to ~16,800 test cases.
        """
        data = []
        with open(path, 'r') as f:
            if path.endswith('.jsonl'):
                for line in f:
                    if line.strip():
                        item = json.loads(line)
                        data.extend(self._extract_all_prompts_from_query(item))
            else:
                # Assume standard JSON list (our nl_social_media_queries.json format)
                content = json.load(f)
                for item in content:
                    data.extend(self._extract_all_prompts_from_query(item))
                    
        return data
    
    def _extract_all_prompts_from_query(self, query_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all test cases from a single query item.
        
        Args:
            query_item: A single query from the dataset with perturbations.
            
        Returns:
            List of test case dictionaries, one for original + one per applicable perturbation.
        """
        test_cases = []
        
        # Base metadata from query
        query_id = query_item.get('id', str(uuid.uuid4()))
        gold_sql = query_item.get('sql', '')
        complexity = query_item.get('complexity', 'unknown')
        tables = query_item.get('tables', [])
        
        # Check if this query has the new perturbation structure
        if 'generated_perturbations' in query_item:
            perturbations = query_item['generated_perturbations']
            
            # 1. Add original prompt
            original = perturbations.get('original', {})
            original_prompt = original.get('nl_prompt', query_item.get('nl_prompt', ''))
            
            if original_prompt:
                test_cases.append({
                    'prompt_id': f"{query_id}_original",
                    'prompt_text': original_prompt,
                    'sql': gold_sql,
                    'complexity': complexity,
                    'tables': tables,
                    'perturbation_type': 'original',
                    'perturbation_id': 0,
                    'metadata': {
                        'query_id': query_id,
                        'is_perturbed': False
                    }
                })
            
            # 2. Add each applicable perturbation
            single_perts = perturbations.get('single_perturbations', [])
            for pert in single_perts:
                if pert.get('applicable', False):
                    perturbed_prompt = pert.get('perturbed_nl_prompt')
                    if perturbed_prompt:
                        test_cases.append({
                            'prompt_id': f"{query_id}_pert_{pert.get('perturbation_id', pert.get('perturbation_name', 'unknown'))}",
                            'prompt_text': perturbed_prompt,
                            'sql': gold_sql,  # Same gold SQL for all perturbations
                            'complexity': complexity,
                            'tables': tables,
                            'perturbation_type': pert.get('perturbation_name', 'unknown'),
                            'perturbation_id': pert.get('perturbation_id', -1),
                            'metadata': {
                                'query_id': query_id,
                                'is_perturbed': True,
                                'changes_made': pert.get('changes_made', ''),
                                'original_prompt': original_prompt
                            }
                        })
            
            # 3. Add compound perturbation
            compound = perturbations.get('compound_perturbation', {})
            compound_prompt = compound.get('perturbed_nl_prompt')
            
            if compound_prompt:
                test_cases.append({
                    'prompt_id': f"{query_id}_compound",
                    'prompt_text': compound_prompt,
                    'sql': gold_sql,
                    'complexity': complexity,
                    'tables': tables,
                    'perturbation_type': 'compound',
                    'perturbation_id': 'compound',
                    'metadata': {
                        'query_id': query_id,
                        'is_perturbed': True,
                        'changes_made': compound.get('changes_made', ''),
                        'original_prompt': original_prompt,
                        'perturbations_applied': compound.get('perturbations_applied', [])
                    }
                })
        
        else:
            # Fallback for simple format without perturbations
            nl_prompt = query_item.get('nl_prompt', query_item.get('prompt_text', ''))
            if nl_prompt:
                test_cases.append({
                    'prompt_id': str(query_id),
                    'prompt_text': nl_prompt,
                    'sql': gold_sql,
                    'complexity': complexity,
                    'tables': tables,
                    'perturbation_type': 'original',
                    'perturbation_id': 0,
                    'metadata': {
                        'query_id': query_id,
                        'is_perturbed': False
                    }
                })
        
        return test_cases

    def _log_record(self, record: Dict[str, Any]):
        """Append-only logging."""
        with open(self.output_path, 'a') as f:
            f.write(json.dumps(record) + "\n")
    
    def _construct_full_prompt(self, nl_query: str) -> str:
        """
        Construct a complete prompt with schema context and instructions.
        
        Args:
            nl_query: The natural language query description.
            
        Returns:
            Complete prompt string ready for LLM.
        """
        schema_text = self._format_schema_text()
        
        prompt = f"""{schema_text}

You are a SQL expert. Generate the exact raw SQL query to handle the following task:

Task: {nl_query}

Requirements:
1. Return ONLY the SQL query, no explanations or additional text
2. Do not include markdown formatting or code blocks
3. Generate syntactically correct MySQL-compatible SQL

SQL:"""
        
        return prompt
    
    def _format_schema_text(self) -> str:
        """
        Format the database schema as text for LLM prompt context.
        
        Returns:
            Formatted string describing tables, columns, types, and foreign keys.
        """
        schema_lines = ["Database Schema:", ""]
        
        # Format tables and columns
        schema_lines.append("TABLES:")
        for table_name, columns in self.schema.items():
            col_defs = []
            for col_name, col_type in columns.items():
                col_defs.append(f"{col_name} {col_type.upper()}")
            schema_lines.append(f"  - {table_name} ({', '.join(col_defs)})")
        
        schema_lines.append("")
        schema_lines.append("FOREIGN KEY RELATIONSHIPS:")
        
        # Format foreign keys (deduplicate bidirectional relationships)
        seen_fks = set()
        for (left_table, right_table), (left_key, right_key) in self.foreign_keys.items():
            # Create a canonical representation to avoid duplicates
            fk_tuple = tuple(sorted([(left_table, left_key), (right_table, right_key)]))
            if fk_tuple not in seen_fks:
                seen_fks.add(fk_tuple)
                schema_lines.append(f"  - {left_table}.{left_key} -> {right_table}.{right_key}")
        
        return "\n".join(schema_lines)
