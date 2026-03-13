"""
VLLM Adapter for Evaluation Harness.
"""
import json
import inspect
from typing import List, Dict, Any
from .base import BaseModelAdapter
try:
    from vllm import LLM, SamplingParams
    try:
        from vllm.sampling_params import StructuredOutputsParams
    except Exception:
        StructuredOutputsParams = None
except ImportError:
    LLM = None
    SamplingParams = None
    StructuredOutputsParams = None

class VLLMAdapter(BaseModelAdapter):
    """Adapter for local open models via vLLM."""

    def __init__(self, model_name: str, **kwargs):
        if LLM is None:
            raise ImportError("vllm package is required for VLLMAdapter")

        self._model_name = model_name

        # Configurable generation params (popped before passing to vLLM engine)
        self._max_tokens = kwargs.pop('max_tokens', 512)
        self._temperature = kwargs.pop('temperature', 0.0)
        self._structured_output = kwargs.pop('structured_output', None)
        self._structured_disable_fallback = kwargs.pop('structured_disable_fallback', False)

        # Traditional SQL stop token (disabled automatically in structured mode)
        self._stop = kwargs.pop('stop', [";"])
        if self._structured_output == 'json_sql':
            self._stop = None

        self._system_prompt = kwargs.pop('system_prompt', None)

        # FIX: Disable custom multiprocessing for Colab compatibility
        import os
        os.environ['VLLM_WORKER_MULTIPROC_METHOD'] = 'spawn'

        # Extract VLLM specific args from kwargs, with defaults for Colab
        tensor_parallel_size = kwargs.get('tensor_parallel_size', 1)
        max_model_len = kwargs.get('max_model_len')  # Default None = auto
        quantization = kwargs.get('quantization')
        gpu_memory_utilization = kwargs.get('gpu_memory_utilization', 0.85)
        enforce_eager = kwargs.get('enforce_eager', True)
        trust_remote_code = kwargs.get('trust_remote_code', True)
        dtype = kwargs.get('dtype', 'auto')

        llm_kwargs = {
            'model': model_name,
            'tensor_parallel_size': tensor_parallel_size,
            'trust_remote_code': trust_remote_code,
            'max_model_len': max_model_len,
            'quantization': quantization,
            'dtype': dtype,
            # Colab stability settings:
            'disable_log_stats': True,
            'enforce_eager': enforce_eager,
            'gpu_memory_utilization': gpu_memory_utilization,
        }

        # Optional vLLM structured outputs engine config passthrough.
        structured_outputs_config = kwargs.get('structured_outputs_config')
        if structured_outputs_config is not None:
            llm_kwargs['structured_outputs_config'] = structured_outputs_config

        # Initialize vLLM engine
        self.llm = LLM(**llm_kwargs)

        sampling_kwargs = {
            'temperature': self._temperature,
            'top_p': 1.0,
            'max_tokens': self._max_tokens,
            'stop': self._stop,
        }

        if self._structured_output == 'json_sql':
            schema = {
                'type': 'object',
                'properties': {
                    'sql': {'type': 'string'},
                },
                'required': ['sql'],
                'additionalProperties': False,
            }

            param_names = set(inspect.signature(SamplingParams).parameters.keys())
            if 'structured_outputs' in param_names and StructuredOutputsParams is not None:
                sampling_kwargs['structured_outputs'] = StructuredOutputsParams(
                    json=schema,
                    disable_fallback=self._structured_disable_fallback,
                )
            elif 'guided_decoding' in param_names:
                # Backward compatibility for older vLLM versions.
                sampling_kwargs['guided_decoding'] = {'json': schema}
            elif 'guided_json' in param_names:
                sampling_kwargs['guided_json'] = schema
            else:
                raise RuntimeError(
                    'structured_output=json_sql requested, but this vLLM '
                    'build has no supported guided/structured decoding API.'
                )

        self.sampling_params = SamplingParams(**sampling_kwargs)

    def generate(self, prompts: List[str]) -> List[Any]:
        # Apply chat template formatting for instruction-tuned models
        formatted_prompts = [self.format_prompt(p) for p in prompts]
        
        # vLLM handles batching internally efficiently
        outputs = self.llm.generate(formatted_prompts, self.sampling_params)
        
        results = []
        for output in outputs:
            # vLLM returns RequestOutput objects
            generated_text = output.outputs[0].text
            if self._structured_output == 'json_sql':
                generated_sql = None
                try:
                    parsed = json.loads(generated_text)
                    sql_val = parsed.get('sql')
                    if isinstance(sql_val, str):
                        generated_sql = sql_val
                except Exception:
                    # Fall back to legacy downstream extraction if parsing fails.
                    pass

                results.append({
                    'generated_response': generated_text,
                    'generated_sql': generated_sql,
                })
            else:
                results.append(generated_text)
        return results

    def format_prompt(self, prompt: str) -> str:
        """Format prompt using the model's chat template when available."""
        tokenizer = self.llm.get_tokenizer()
        if hasattr(tokenizer, 'apply_chat_template') and tokenizer.chat_template:
            messages = []
            if self._system_prompt:
                messages.append({"role": "system", "content": self._system_prompt})
            messages.append({"role": "user", "content": prompt})
            return tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True
            )
        # Fallback for models without chat template
        if self._system_prompt:
            return f"{self._system_prompt}\n\n{prompt}"
        return prompt

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "open"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": self._temperature,
            "top_p": 1.0,
            "max_tokens": self._max_tokens,
            "stop": self._stop,
            "structured_output": self._structured_output,
        }
