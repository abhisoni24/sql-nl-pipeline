import time
import asyncio
from typing import List, Dict, Any, Optional
from tqdm.auto import tqdm

from src.core.schema_config import SchemaConfig


# Default SQL dialect (used when no schema is provided)
_DEFAULT_DIALECT = "sqlite"


def _build_system_prompt(dialect: str = _DEFAULT_DIALECT) -> str:
    """Build the system prompt with the given SQL dialect."""
    return (
        f"You are a SQL expert. Given the database schema and a natural language query,\n"
        f"provide ONLY the SQL code. No explanations. No preamble. Return only valid SQL.\n"
        f"IMPORTANT: Generate SQL compatible with the {dialect.upper()} dialect."
    )


def _build_schema_context(
    schema: Optional[Dict[str, Dict[str, str]]] = None,
    foreign_keys: Optional[Dict] = None,
    schema_config: Optional[SchemaConfig] = None,
) -> str:
    """Build human-readable schema context from SchemaConfig or legacy dicts."""
    if schema_config is not None:
        lines = [f"Database: {schema_config.schema_name}  (dialect: {schema_config.dialect})"]
        lines.append("")
        lines.append("Database Schema:")
        for table, tdef in schema_config.tables.items():
            col_strs = [f"{c.name} {c.col_type.upper()}" for c in tdef.columns.values()]
            lines.append(f"- {table}({', '.join(col_strs)})")
        return "\n".join(lines)

    if schema is None:
        return ""

    lines = ["Database Schema:"]
    for table, cols in schema.items():
        col_strs = [f"{c} {t.upper()}" for c, t in cols.items()]
        lines.append(f"- {table}({', '.join(col_strs)})")
    return "\n".join(lines)


class LLMWorker:
    """Unified wrapper for both local (vLLM) and API-based models."""
    
    def __init__(
        self,
        adapter_type: str,
        model_identifier: str,
        rate_limit: Optional[Dict[str, Any]] = None,
        schema_config: Optional[SchemaConfig] = None,
        schema: Optional[Dict[str, Dict[str, str]]] = None,
        foreign_keys: Optional[Dict] = None,
        dialect: str = _DEFAULT_DIALECT,
        **kwargs
    ):
        self.adapter_type = adapter_type
        self.model_identifier = model_identifier
        self.rate_limit = rate_limit or {}
        self.adapter_config = kwargs
        self._adapter = None
        
        # Build prompts from schema config
        effective_dialect = schema_config.dialect if schema_config is not None else dialect
        self._system_prompt = _build_system_prompt(effective_dialect)
        if schema_config is not None:
            self._schema_context = _build_schema_context(schema_config=schema_config)
        elif schema is not None:
            self._schema_context = _build_schema_context(schema=schema, foreign_keys=foreign_keys)
        else:
            # Fallback: no schema provided — callers must provide full prompts
            self._schema_context = ""
        
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
        return f"{self._system_prompt}\n\n{self._schema_context}\n\nQuery: {nl_prompt}\n\nSQL:"
    
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
