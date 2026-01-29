"""
Configuration Loader and Registry.
"""
import yaml
from typing import List, Dict, Any, Type
from dataclasses import dataclass
from .adapters.base import BaseModelAdapter
from .adapters.gemini import GeminiAdapter
from .adapters.openai import OpenAIAdapter
from .adapters.anthropic import AnthropicAdapter
from .adapters.vllm import VLLMAdapter

@dataclass
class ModelConfig:
    name: str # experiment identifier name
    adapter_type: str
    model_identifier: str
    decoding_overrides: Dict[str, Any]
    hardware_notes: str
    rate_limit: Dict[str, Any]  # New: rate limiting configuration

class ConfigLoader:
    
    ADAPTER_MAP: Dict[str, Type[BaseModelAdapter]] = {
        "gemini": GeminiAdapter,
        "openai": OpenAIAdapter,
        "anthropic": AnthropicAdapter,
        "vllm": VLLMAdapter
    }

    @staticmethod
    def load_experiments(config_path: str) -> List[ModelConfig]:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        experiments = []
        for entry in data.get('models', []):
            experiments.append(ModelConfig(
                name=entry.get('name', entry.get('model_identifier')),
                adapter_type=entry['adapter_type'],
                model_identifier=entry['model_identifier'],
                decoding_overrides=entry.get('decoding_overrides', {}),
                hardware_notes=entry.get('hardware_notes', ""),
                rate_limit=entry.get('rate_limit', None)  # New: optional rate limit config
            ))
        return experiments

    @classmethod
    def get_adapter(cls, config: ModelConfig) -> BaseModelAdapter:
        adapter_cls = cls.ADAPTER_MAP.get(config.adapter_type)
        if not adapter_cls:
            raise ValueError(f"Unknown adapter type: {config.adapter_type}")
        
        # Instantiate
        return adapter_cls(model_name=config.model_identifier)
