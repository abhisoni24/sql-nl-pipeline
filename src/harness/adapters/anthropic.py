"""
Anthropic Adapter for Evaluation Harness.
"""
import os
from typing import List, Dict, Any
from .base import BaseModelAdapter
try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None
from dotenv import load_dotenv
load_dotenv()

class AnthropicAdapter(BaseModelAdapter):
    """Adapter for Anthropic Claude models."""

    def __init__(self, model_name: str = "claude-3-5-sonnet-20241022"):
        if Anthropic is None:
            raise ImportError("anthropic package is required for AnthropicAdapter")

        self._model_name = model_name
        self.api_key = os.getenv("CLAUDE_API_KEY")
        if not self.api_key:
            raise ValueError("CLAUDE_API_KEY not found in environment.")
            
        self.client = Anthropic(api_key=self.api_key)

    def generate(self, prompts: List[str]) -> List[str]:
        results = []
        for prompt in prompts:
            try:
                formatted_prompt = self.format_prompt(prompt)
                response = self.client.messages.create(
                    model=self._model_name,
                    max_tokens=512,
                    temperature=0.0,
                    messages=[
                        {"role": "user", "content": formatted_prompt}
                    ]
                )
                #results.append(response. content)
                results.append(response.content[0].text)
            except Exception as e:
                import logging
                logging.error(f"Anthropic API error: {str(e)}")
                results.append("")  # Empty result indicates failure
        return results

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "anthropic"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": 0.0,
            "max_tokens": 512
        }
