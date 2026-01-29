"""
Abstract Base Class for Model Adapters.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseModelAdapter(ABC):
    """
    Abstract interface for all LLM model adapters.
    Enforces a unified interaction pattern for both API and local models.
    """

    @abstractmethod
    def generate(self, prompts: List[str]) -> List[str]:
        """
        Generate completions for a batch of prompts.
        
        Args:
            prompts: A list of input strings.
            
        Returns:
            A list of raw string completions corresponding 1:1 to the input prompts.
        """
        pass

    @abstractmethod
    def model_name(self) -> str:
        """Return the specific model identifier (e.g., 'gemini-1.5-pro')."""
        pass

    @abstractmethod
    def model_family(self) -> str:
        """Return the model family (e.g., 'open', 'closed', 'google', 'openai')."""
        pass

    @abstractmethod
    def decoding_config(self) -> Dict[str, Any]:
        """
        Return the exact decoding parameters used for generation.
        Ideally: temperature=0.0, max_tokens=512, etc.
        """
        pass
    
    def format_prompt(self, prompt: str) -> str:
        """
        Format a prompt according to model-specific requirements.
        
        This is a hook for adapters to apply model-specific prompt formatting,
        such as chat templates, special tokens, instruction formatting, etc.
        
        Default implementation returns the prompt as-is.
        Subclasses can override to implement model-specific formatting.
        
        Args:
            prompt: The raw prompt string (already includes schema and instructions).
            
        Returns:
            Formatted prompt ready for the specific model.
        """
        return prompt
