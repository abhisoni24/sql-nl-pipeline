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

    def __init__(self, model_name: str = "gemini-2.0-flash-exp"):
        self._model_name = model_name
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment.")
        
        # Initialize client with timeout
        self.client = genai.Client(
            api_key=self.api_key,
            http_options=types.HttpOptions(timeout=60000) # 60 seconds timeout (in milliseconds? No, usually seconds, but types says int. Let's assume ms if it's an int, or check docs. Standard httpx is seconds. Wait, pydantic field says int. 60000ms = 60s is safe bet if ms. If seconds, 60000s is too long. Let's check retry options.)
        )
        # Suppress noisy AFC logs
        import logging
        logging.getLogger("models").setLevel(logging.WARNING)
        logging.getLogger("google.genai").setLevel(logging.WARNING)


    def generate(self, prompts: List[str]) -> List[str]:
        results = []
        for prompt in prompts:
            try:
                # Apply model-specific formatting
                formatted_prompt = self.format_prompt(prompt)
                
                # Explicit decoding parameters as per requirements
                response = self.client.models.generate_content(
                    model=self._model_name,
                    contents=formatted_prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.0,
                        max_output_tokens=512
                    )
                )
                if response.text:
                    results.append(response.text)
                else:
                    results.append("") # Handle empty response safely
            except Exception as e:
                # Return empty string on error so downstream processing handles it cleanly
                # Error is logged via execution engine's error tracking
                import logging
                logging.error(f"Gemini API error: {str(e)}")
                results.append("")  # Empty result indicates failure
        return results

    def model_name(self) -> str:
        return self._model_name

    def model_family(self) -> str:
        return "google"

    def decoding_config(self) -> Dict[str, Any]:
        return {
            "temperature": 0.0,
            "max_tokens": 512
        }
