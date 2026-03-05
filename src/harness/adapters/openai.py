"""
OpenAI Adapter for Evaluation Harness.
"""
import os
from typing import List, Dict, Any
from .base import BaseModelAdapter
# Assuming `openai` package is available or will be installed. 
# Using conditional import to avoid breaking if not installed, but harness implies dependencies.
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None
from dotenv import load_dotenv
load_dotenv()

class OpenAIAdapter(BaseModelAdapter):
    """Adapter for OpenAI models."""

    def __init__(
        self,
        model_name: str = "gpt-4o",
        max_tokens: int = 512,
        temperature: float = 0.0,
        system_prompt: str = None,
        **_kwargs,
    ):
        if OpenAI is None:
            raise ImportError("openai package is required for OpenAIAdapter")

        self._model_name = model_name
        self._max_completed_tokens = max_tokens
        self._temperature = temperature
        self._system_prompt = system_prompt or "You are a helpful assistant."

        self.api_key = os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY not found in environment.")
        # Sanitize key
        self.api_key = self.api_key.strip().splitlines()[0]

        self.client = OpenAI(api_key=self.api_key)

    def generate(self, prompts: List[str]) -> List[str]:
        results = []
        for prompt in prompts:
            formatted_prompt = self.format_prompt(prompt)
            # Let exceptions propagate to LLMWorker for retry logic
            response = self.client.chat.completions.create(
                model=self._model_name,
                messages=[
                    {"role": "system", "content": self._system_prompt},
                    {"role": "user", "content": formatted_prompt},
                ],
                temperature=self._temperature,
                max_completion_tokens=self._max_completed_tokens,
            )
            results.append(response.choices[0].message.content)
        return results

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "openai"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": self._temperature,
            "max_tokens": self._max_completed_tokens,
        }
