import time
import asyncio
from typing import List, Dict, Any, Optional
from tqdm.auto import tqdm

from src.core.schema import USED_SQL_DIALECT

# System prompt for SQL generation (consistent across all models)
SYSTEM_PROMPT = f"""You are a SQL expert. Given the database schema and a natural language query,
provide ONLY the SQL code. No explanations. No preamble. Return only valid SQL.
IMPORTANT: Generate SQL compatible with the {USED_SQL_DIALECT.upper()} dialect."""

# Database schema context
SCHEMA_CONTEXT = """
Database Schema:
- users(id INT, username VARCHAR, email VARCHAR, signup_date DATETIME, is_verified BOOLEAN, country_code VARCHAR)
- posts(id INT, user_id INT, content TEXT, posted_at DATETIME, view_count INT)
- comments(id INT, user_id INT, post_id INT, comment_text TEXT, created_at DATETIME)
- likes(user_id INT, post_id INT, liked_at DATETIME)
- follows(follower_id INT, followee_id INT, followed_at DATETIME)
"""

class LLMWorker:
    """Unified wrapper for both local (vLLM) and API-based models."""
    
    def __init__(
        self,
        adapter_type: str,
        model_identifier: str,
        rate_limit: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        self.adapter_type = adapter_type
        self.model_identifier = model_identifier
        self.rate_limit = rate_limit or {}
        self.adapter_config = kwargs
        self._adapter = None
        self._initialize_adapter()
    
    def _initialize_adapter(self):
        """Initialize the appropriate adapter based on type."""
        # Dynamic imports to avoid loading unnecessary heavy libraries
        if self.adapter_type == 'vllm':
            from src.harness.adapters.vllm import VLLMAdapter
            # Pass all extra config args to VLLM adapter
            self._adapter = VLLMAdapter(model_name=self.model_identifier, **self.adapter_config)
        elif self.adapter_type == 'gemini':
            from src.harness.adapters.gemini import GeminiAdapter
            self._adapter = GeminiAdapter(model_name=self.model_identifier)
        elif self.adapter_type == 'openai':
            from src.harness.adapters.openai import OpenAIAdapter
            self._adapter = OpenAIAdapter(model_name=self.model_identifier)
        elif self.adapter_type == 'anthropic':
            from src.harness.adapters.anthropic import AnthropicAdapter
            self._adapter = AnthropicAdapter(model_name=self.model_identifier)
        else:
            raise ValueError(f"Unknown adapter type: {self.adapter_type}")
    
    def _format_prompt(self, nl_prompt: str) -> str:
        """Format prompt with schema context and system instructions."""
        return f"{SYSTEM_PROMPT}\n\n{SCHEMA_CONTEXT}\n\nQuery: {nl_prompt}\n\nSQL:"
    
    def generate_batch(self, prompts: List[str], batch_size: int = 500) -> List[str]:
        """
        Generate responses for a batch of prompts.
        
        Args:
            prompts: List of natural language prompts
            batch_size: Chunk size for vLLM (default 500)
            
        Returns:
            List of generated SQL responses
        """
        formatted_prompts = [self._format_prompt(p) for p in prompts]
        
        if self.adapter_type == 'vllm':
            # vLLM: Process in chunks
            results = []
            for i in range(0, len(formatted_prompts), batch_size):
                chunk = formatted_prompts[i:i+batch_size]
                chunk_results = self._adapter.generate(chunk)
                results.extend(chunk_results)
            return results
        else:
            # API models: Use rate limiting
            return self._generate_with_rate_limit(formatted_prompts)
    
    def _generate_with_rate_limit(self, prompts: List[str]) -> List[str]:
        """Generate with rate limiting for API models."""
        rpm = self.rate_limit.get('requests_per_minute', 60)
        max_retries = self.rate_limit.get('max_retries', 3)
        min_interval = 60.0 / rpm
        
        results = []
        last_request_time = 0
        
        for prompt in tqdm(prompts, desc=f"Generating ({self.model_identifier})"):
            # Rate limiting
            elapsed = time.time() - last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            
            # Retry logic
            for attempt in range(max_retries):
                try:
                    result = self._adapter.generate([prompt])[0]
                    results.append(result)
                    last_request_time = time.time()
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        results.append(f"ERROR: {str(e)}")
                    else:
                        time.sleep(2 ** attempt)  # Exponential backoff
        
        return results
    
    @property
    def model_name(self) -> str:
        return self._adapter.model_name()
