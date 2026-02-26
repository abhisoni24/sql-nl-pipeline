"""
Configuration Loader and Registry.
"""
import yaml
from typing import List, Dict, Any, Type
from dataclasses import dataclass
from .adapters.base import BaseModelAdapter

# Lazy imports — heavy SDK deps (google-genai, openai, anthropic, vllm)
# are resolved only when the corresponding adapter is actually requested.
_ADAPTER_IMPORTS = {
    "gemini": ("src.harness.adapters.gemini", "GeminiAdapter"),
    "openai": ("src.harness.adapters.openai", "OpenAIAdapter"),
    "anthropic": ("src.harness.adapters.anthropic", "AnthropicAdapter"),
    "vllm": ("src.harness.adapters.vllm", "VLLMAdapter"),
}


@dataclass
class ModelConfig:
    name: str  # experiment identifier name
    adapter_type: str
    model_identifier: str
    decoding_overrides: Dict[str, Any]
    hardware_notes: str
    rate_limit: Dict[str, Any]  # rate-limiting configuration


class ConfigLoader:

    @staticmethod
    def _resolve_adapter_cls(adapter_type: str) -> Type[BaseModelAdapter]:
        entry = _ADAPTER_IMPORTS.get(adapter_type)
        if entry is None:
            raise ValueError(
                f"Unknown adapter type: {adapter_type}.  "
                f"Available: {list(_ADAPTER_IMPORTS)}"
            )
        module_path, class_name = entry
        import importlib
        mod = importlib.import_module(module_path)
        return getattr(mod, class_name)

    # Expose a class-level map for external code that inspects adapter types
    ADAPTER_MAP: Dict[str, str] = {k: k for k in _ADAPTER_IMPORTS}

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
    def get_adapter(cls, config: ModelConfig, **extra_kwargs) -> BaseModelAdapter:
        adapter_cls = cls._resolve_adapter_cls(config.adapter_type)

        # Merge decoding_overrides from YAML with caller-supplied kwargs
        kwargs = {**(config.decoding_overrides or {}), **extra_kwargs}
        return adapter_cls(model_name=config.model_identifier, **kwargs)
