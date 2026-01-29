"""
VLLM Adapter for Evaluation Harness.
"""
from typing import List, Dict, Any
from .base import BaseModelAdapter
try:
    from vllm import LLM, SamplingParams
except ImportError:
    LLM = None
    SamplingParams = None

class VLLMAdapter(BaseModelAdapter):
    """Adapter for local open models via vLLM."""

    def __init__(self, model_name: str, tensor_parallel_size: int = 1):
        if LLM is None:
            raise ImportError("vllm package is required for VLLMAdapter")
        
        self._model_name = model_name
        
        # FIX: Disable custom multiprocessing for Colab compatibility
        import os
        os.environ['VLLM_WORKER_MULTIPROC_METHOD'] = 'spawn'
        
        # Initialize vLLM engine with Colab-friendly settings
        self.llm = LLM(
            model=model_name,
            tensor_parallel_size=tensor_parallel_size, 
            trust_remote_code=True,
            # Add these Colab-specific parameters:
            disable_log_stats=True,
            enforce_eager=True,  # Disable CUDA graph for stability
            gpu_memory_utilization=0.85  # Leave some GPU memory free
        )
        
        # Fixed decoding parameters as per requirements
        self.sampling_params = SamplingParams(
            temperature=0.0,
            top_p=1.0,
            max_tokens=512,
            stop=[";"]
        )

    def generate(self, prompts: List[str]) -> List[str]:
        # Apply model-specific formatting to all prompts
        formatted_prompts = [self.format_prompt(p) for p in prompts]
        
        # vLLM handles batching internally efficiently
        outputs = self.llm.generate(formatted_prompts, self.sampling_params)
        
        results = []
        for output in outputs:
            # vLLM returns RequestOutput objects
            generated_text = output.outputs[0].text
            results.append(generated_text)
        return results

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "open"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": 0.0,
            "top_p": 1.0,
            "max_tokens": 512,
            "stop": [";"]
        }
