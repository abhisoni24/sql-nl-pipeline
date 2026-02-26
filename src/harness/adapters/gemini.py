"""
Gemini Adapter for Evaluation Harness.
"""
import os
import time
from typing import List, Dict, Any
from .base import BaseModelAdapter
from google import genai
from google.genai import types
from dotenv import load_dotenv
load_dotenv()

class GeminiAdapter(BaseModelAdapter):
    """Adapter for Google Gemini models via Google GenAI SDK."""

    def __init__(
        self,
        model_name: str = "gemini-2.0-flash-exp",
        max_tokens: int = 512,
        temperature: float = 0.0,
        system_prompt: str = None,
        **_kwargs,
    ):
        self._model_name = model_name
        self._max_tokens = max_tokens
        self._temperature = temperature
        self._system_prompt = system_prompt

        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment.")
        # Sanitize key
        self.api_key = self.api_key.strip().splitlines()[0]

        # Initialize client with timeout
        self.client = genai.Client(
            api_key=self.api_key,
            http_options=types.HttpOptions(timeout=60000),
        )
        # Suppress noisy AFC logs
        import logging
        logging.getLogger("models").setLevel(logging.WARNING)
        logging.getLogger("google.genai").setLevel(logging.WARNING)

    def generate(self, prompts: List[str]) -> List[str]:
        results = []
        for prompt in prompts:
            formatted_prompt = self.format_prompt(prompt)

            gen_config = types.GenerateContentConfig(
                temperature=self._temperature,
                max_output_tokens=self._max_tokens,
            )
            if self._system_prompt:
                gen_config.system_instruction = self._system_prompt

            # Let exceptions propagate to LLMWorker for retry logic
            response = self.client.models.generate_content(
                model=self._model_name,
                contents=formatted_prompt,
                config=gen_config,
            )
            results.append(response.text if response.text else "")
        return results

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "google"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": self._temperature,
            "max_tokens": self._max_tokens,
        }
