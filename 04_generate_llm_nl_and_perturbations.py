"""
Step 4 — Generate NL prompts and perturbations from raw SQL via any LLM.

Unified replacement for the old 04 / 04b scripts.  Takes raw SQL queries
(output of step 01), sends each to an LLM together with the database schema
and perturbation-type definitions, and asks the model in **one shot** to:

  1. Write a natural-language prompt for the SQL.
  2. Generate 13 single-perturbation versions of that NL prompt.
  3. Generate 1 compound perturbation (2-5 combined perturbations).

Works with any model backend supported by the harness adapters
(Gemini, OpenAI, Anthropic, vLLM) and can be configured via
``experiments.yaml`` or ad-hoc CLI flags.

Usage
-----
  # Default: uses local Qwen3.5-27B via vLLM (no API calls)
  python 04_generate_llm_nl_and_perturbations.py \\
      --schema schemas/bank.yaml

  # Explicit model from experiments.yaml
  python 04_generate_llm_nl_and_perturbations.py \\
      --schema schemas/hospital.yaml \\
      --model qwen3.5-27b

  # Ad-hoc model selection (API-based)
  python 04_generate_llm_nl_and_perturbations.py \\
      --schema schemas/hospital.yaml \\
      --adapter-type openai --model-id gpt-4o

  # Explicit I/O + custom token budget
  python 04_generate_llm_nl_and_perturbations.py \\
      --schema schemas/social_media.yaml \\
      -i dataset/social_media/raw_queries.json \\
      -o dataset/social_media/llm_perturbations.json \\
      --max-tokens 8192

  # Mock mode (no API calls — useful for testing the pipeline)
  python 04_generate_llm_nl_and_perturbations.py \\
      --schema schemas/social_media.yaml --mock
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from tqdm import tqdm

# ---------------------------------------------------------------------------
# Project root on sys.path
# ---------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, BASE_DIR)

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------
DEFAULT_MAX_RPM = 60
DEFAULT_MAX_WORKERS = 4
DEFAULT_MAX_TOKENS = 4096
DEFAULT_TEMPERATURE = 0.0
SAVE_EVERY_N = 10
DEFAULT_EXPERIMENTS_CONFIG = os.path.join(BASE_DIR, "experiments.yaml")


# ============================================================================
# Prompt construction
# ============================================================================

def _load_perturbation_text() -> str:
    """Return the perturbation-type definitions + output-format instructions
    from ``cached_info.py`` as a single text block suitable for an LLM prompt.

    The file is deliberately loaded as *raw text* (not imported as Python)
    because it uses JSON ``true`` / ``false`` literals rather than
    Python ``True`` / ``False``.
    """
    path = os.path.join(BASE_DIR, "cached_info.py")
    with open(path, "r") as f:
        text = f.read()

    # Strip the schema / foreign_keys / column-type blocks — we inject those
    # dynamically from the YAML.
    text = re.sub(
        r'\nschema = \{.*?\n\}',
        '',
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r'\n# Define valid join paths.*?\n\}',
        '',
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r'\n# Column type categories.*?BOOLEAN_TYPES.*?\}',
        '',
        text,
        flags=re.DOTALL,
    )
    # Also strip the trailing comments
    text = re.sub(r'\n#Ignore the following text.*', '', text, flags=re.DOTALL)
    return text.strip()


def _build_schema_context(schema_path: str) -> str:
    """Render a human-readable schema description from a schema file."""
    from src.core.schema_loader import load_schema

    cfg = load_schema(schema_path)
    lines: list[str] = [f"Database: {cfg.schema_name}  (dialect: {cfg.dialect})"]
    lines.append("")

    # Tables
    for tname, tdef in cfg.tables.items():
        cols = ", ".join(
            f"{c.name} {c.col_type.upper()}" for c in tdef.columns.values()
        )
        lines.append(f"- {tname}({cols})")

    # Foreign keys
    if cfg.foreign_keys:
        lines.append("")
        lines.append("Foreign key relationships:")
        for fk in cfg.foreign_keys:
            lines.append(
                f"  {fk.source_table}.{fk.source_column} -> "
                f"{fk.target_table}.{fk.target_column}"
            )
    return "\n".join(lines)


def _build_system_prompt(schema_ctx: str, perturbation_text: str) -> str:
    """Assemble the full system prompt (reusable across queries)."""
    return f"""\
You are an expert at interpreting SQL queries and expressing them as natural \
language prompts that a developer might type into an NL-to-SQL system.  You are \
also expert at generating realistic perturbations of those natural language \
prompts. You always return valid JSON and nothing else.

# Database Schema
{schema_ctx}

# Perturbation Type Definitions & Instructions
{perturbation_text}
"""


def _build_user_prompt(query: Dict[str, Any]) -> str:
    """Build the per-query user prompt."""
    sql = query.get("sql", "")
    tables = query.get("tables", [])
    complexity = query.get("complexity", "unknown")

    return f"""\
Given the following SQL query, perform the two steps below.

```sql
{sql}
```
Tables: {tables}
Complexity: {complexity}

## Step 1 — Generate a natural-language prompt
Write a clear, concise NL request that a developer might type to produce this \
SQL query.  It should sound natural, not like a mechanical SQL-to-English \
translation.

## Step 2 — Generate perturbations
Following the perturbation type definitions provided in the system context, \
generate 13 single-perturbation versions (one per perturbation type) and 1 \
compound-perturbation version (2-5 perturbations combined) of the NL prompt \
you created in Step 1.

## Output — return ONLY a JSON object with this structure
{{
  "generated_nl_prompt": "<your NL prompt from Step 1>",
  "original": {{
    "nl_prompt": "<same as generated_nl_prompt>",
    "sql": "<the input SQL>",
    "tables": [<table list>],
    "complexity": "<complexity>"
  }},
  "single_perturbations": [
    {{
      "perturbation_id": 1,
      "perturbation_name": "<name>",
      "applicable": true,
      "perturbed_nl_prompt": "<perturbed text>",
      "changes_made": "<brief note>",
      "reason_not_applicable": null
    }},
    {{
      "perturbation_id": 2,
      "perturbation_name": "<name>",
      "applicable": false,
      "perturbed_nl_prompt": null,
      "changes_made": null,
      "reason_not_applicable": "<reason>"
    }}
  ],
  "compound_perturbation": {{
    "perturbations_applied": [
      {{"perturbation_id": 1, "perturbation_name": "<name>"}}
    ],
    "perturbed_nl_prompt": "<compound version>",
    "changes_made": "<all changes>"
  }},
  "metadata": {{
    "total_applicable_perturbations": 8,
    "total_not_applicable": 6,
    "applicability_rate": 0.57
  }}
}}

Important:
- Ensure the JSON is valid.
- For not-applicable perturbations set perturbed_nl_prompt to null.
- For the compound perturbation use only applicable perturbations.
- Return ONLY the JSON object, no other text.
"""


# ============================================================================
# I/O helpers
# ============================================================================

def _load_records(path: str):
    """Load records from either a bare JSON list or a metadata-wrapped
    envelope produced by an upstream pipeline step."""
    with open(path, "r") as fh:
        data = json.load(fh)
    if isinstance(data, dict) and "records" in data:
        return data["records"], data.get("metadata", {})
    return data, {}


def _save_output(
    path: str,
    records: List[Dict[str, Any]],
    schema_path: Optional[str],
    upstream_meta: Dict[str, Any],
    model_id: str,
) -> None:
    """Persist records wrapped in a metadata envelope."""
    schema_name = upstream_meta.get("schema_name", "unknown")
    if schema_path:
        from src.core.schema_loader import load_schema
        schema_name = load_schema(schema_path).schema_name

    envelope = {
        "metadata": {
            "pipeline_step": "04_llm_nl_and_perturbations",
            "schema_name": schema_name,
            "schema_source": schema_path or "legacy_default",
            "model": model_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "num_records": len(records),
            "upstream": upstream_meta if upstream_meta else None,
        },
        "records": records,
    }
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w") as fh:
        json.dump(envelope, fh, indent=2)


def _clean_json_response(text: str) -> str:
    """Strip markdown fences from an LLM response."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


# ============================================================================
# Rate limiter
# ============================================================================

class _RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, max_rpm: int) -> None:
        self._min_interval = 60.0 / max_rpm if max_rpm > 0 else 0.0
        self._lock = threading.Lock()
        self._last: float = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last)
            target = (self._last + self._min_interval) if wait > 0 else now
            self._last = target

        sleep = target - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)


# ============================================================================
# Adapter creation helpers
# ============================================================================

def _create_adapter_from_config(
    model_name: str,
    config_path: str,
    *,
    max_tokens: int,
    temperature: float,
    system_prompt: str,
):
    """Look up *model_name* in ``experiments.yaml`` and instantiate the
    matching adapter with the given generation overrides."""
    from src.harness.config import ConfigLoader

    experiments = ConfigLoader.load_experiments(config_path)
    match = [e for e in experiments if e.name == model_name]
    if not match:
        available = ", ".join(e.name for e in experiments)
        raise ValueError(
            f"Model '{model_name}' not found in {config_path}.  "
            f"Available: {available}"
        )
    cfg = match[0]
    return (
        ConfigLoader.get_adapter(
            cfg,
            max_tokens=max_tokens,
            temperature=temperature,
            system_prompt=system_prompt,
            # For vLLM: disable the SQL-specific stop token
            stop=None,
        ),
        cfg,
    )


def _create_adapter_adhoc(
    adapter_type: str,
    model_id: str,
    *,
    max_tokens: int,
    temperature: float,
    system_prompt: str,
):
    """Create an adapter directly without reading experiments.yaml."""
    from src.harness.config import ConfigLoader

    try:
        adapter_cls = ConfigLoader._resolve_adapter_cls(adapter_type)
    except ValueError:
        raise ValueError(
            f"Unknown adapter type '{adapter_type}'.  "
            f"Available: {list(ConfigLoader.ADAPTER_MAP)}"
        )
    return adapter_cls(
        model_name=model_id,
        max_tokens=max_tokens,
        temperature=temperature,
        system_prompt=system_prompt,
        stop=None,
    )


# ============================================================================
# Single-query processing
# ============================================================================

def _process_one(
    query: Dict[str, Any],
    adapter,
    limiter: _RateLimiter,
    mock: bool,
) -> Dict[str, Any]:
    """Process a single query — designed to run inside a thread."""
    if mock:
        response_text = json.dumps({
            "generated_nl_prompt": f"(mock) NL for: {query.get('sql', '')[:60]}",
            "original": {
                "nl_prompt": f"(mock) NL for: {query.get('sql', '')[:60]}",
                "sql": query.get("sql", ""),
                "tables": query.get("tables", []),
                "complexity": query.get("complexity", "unknown"),
            },
            "single_perturbations": [],
            "compound_perturbation": {
                "perturbations_applied": [],
                "perturbed_nl_prompt": "(mock)",
                "changes_made": "mock",
            },
            "metadata": {
                "total_applicable_perturbations": 0,
                "total_not_applicable": 14,
                "applicability_rate": 0.0,
            },
        })
    else:
        max_retries = 5
        retry_delay = 2.0
        last_error: Optional[Exception] = None

        for _attempt in range(max_retries):
            try:
                limiter.wait()
                user_prompt = _build_user_prompt(query)
                responses = adapter.generate([user_prompt])
                response_text = responses[0]
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                err_msg = str(exc).lower()
                if "429" in err_msg or "rate" in err_msg or "resource" in err_msg:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                raise

        if last_error is not None:
            raise last_error

    cleaned = _clean_json_response(response_text)
    perturbation_data = json.loads(cleaned)

    enriched = query.copy()
    enriched["nl_prompt"] = perturbation_data.get("generated_nl_prompt", "")
    enriched["generated_perturbations"] = perturbation_data
    return enriched


# ============================================================================
# Main processing loop
# ============================================================================

def process_queries(
    input_file: str,
    output_file: str,
    adapter,
    model_id: str,
    schema_path: Optional[str] = None,
    mock: bool = False,
    max_rpm: int = DEFAULT_MAX_RPM,
    max_workers: int = DEFAULT_MAX_WORKERS,
    limit: Optional[int] = None,
) -> None:
    input_path = os.path.abspath(input_file)
    output_path = os.path.abspath(output_file)

    print(f"Reading from:  {input_path}")
    print(f"Writing to:    {output_path}")

    queries, upstream_meta = _load_records(input_path)

    # ---- Resume support ----
    processed_data: List[Dict[str, Any]] = []
    processed_ids: set = set()
    if os.path.exists(output_path):
        try:
            existing, _ = _load_records(output_path)
            processed_data = list(existing)
            processed_ids = {item["id"] for item in processed_data if "id" in item}
            print(f"Resuming — {len(processed_data)} queries already processed.")
        except Exception:
            print("Output file exists but is invalid/empty. Starting fresh.")

    queries_to_process = [q for q in queries if q.get("id") not in processed_ids]
    if limit is not None:
        queries_to_process = queries_to_process[:limit]

    print(f"Total queries:     {len(queries)}")
    print(f"Already processed: {len(processed_ids)}")
    print(f"Remaining:         {len(queries_to_process)}")
    print(f"Concurrency:       {max_workers} workers  |  Rate limit: {max_rpm} RPM")

    if not queries_to_process:
        print("All queries processed!")
        return

    # ---- Concurrent processing ----
    limiter = _RateLimiter(max_rpm)
    success_count = 0
    fail_count = 0
    data_lock = threading.Lock()
    unsaved_count = 0

    def _checkpoint():
        nonlocal unsaved_count
        _save_output(output_path, processed_data, schema_path, upstream_meta, model_id)
        unsaved_count = 0

    pbar = tqdm(total=len(queries_to_process), desc="Processing", unit="query")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_query = {
            pool.submit(_process_one, q, adapter, limiter, mock): q
            for q in queries_to_process
        }
        for future in as_completed(future_to_query):
            query = future_to_query[future]
            try:
                result = future.result()
                with data_lock:
                    processed_data.append(result)
                    success_count += 1
                    unsaved_count += 1
                    if unsaved_count >= SAVE_EVERY_N:
                        _checkpoint()
            except Exception as exc:
                with data_lock:
                    fail_count += 1
                tqdm.write(f"FAIL query {query.get('id', '?')}: {exc}")
            pbar.update(1)
            pbar.set_postfix(ok=success_count, fail=fail_count)

    pbar.close()
    _save_output(output_path, processed_data, schema_path, upstream_meta, model_id)
    print(f"\nDone.  Success: {success_count}  Fail: {fail_count}")
    print(f"Results → {output_path}")


# ============================================================================
# CLI
# ============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate NL prompts + perturbations from raw SQL via any LLM.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s --schema schemas/bank.yaml
  %(prog)s --schema schemas/hospital.yaml --model qwen3.5-27b
  %(prog)s --schema schemas/hospital.yaml --adapter-type openai --model-id gpt-4o
  %(prog)s --schema schemas/social_media.yaml --mock
""",
    )
    # -- I/O & schema --
    p.add_argument(
        "--schema", "-s", required=True,
        help="Path to schema YAML (e.g. schemas/bank.yaml).",
    )
    p.add_argument(
        "--input", "-i", default=None,
        help="Input JSON path (default: dataset/<schema>/raw_queries.json).",
    )
    p.add_argument(
        "--output", "-o", default=None,
        help="Output JSON path (default: dataset/<schema>/llm_perturbations.json).",
    )

    # -- Model selection (mutually exclusive groups) --
    model_group = p.add_argument_group("Model selection")
    model_group.add_argument(
        "--model", "-m", default="qwen3.5-27b",
        help="Model name from experiments.yaml (default: 'qwen3.5-27b').",
    )
    model_group.add_argument(
        "--adapter-type", default=None,
        help="Adapter type for ad-hoc model (gemini|openai|anthropic|vllm).",
    )
    model_group.add_argument(
        "--model-id", default=None,
        help="Model identifier for ad-hoc model (e.g. 'gpt-4o').",
    )
    model_group.add_argument(
        "--experiments-config", default=DEFAULT_EXPERIMENTS_CONFIG,
        help="Path to experiments YAML config (default: experiments.yaml).",
    )

    # -- Generation params --
    gen = p.add_argument_group("Generation parameters")
    gen.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help=f"Max output tokens per query (default: {DEFAULT_MAX_TOKENS}).",
    )
    gen.add_argument(
        "--temperature", type=float, default=DEFAULT_TEMPERATURE,
        help=f"Sampling temperature (default: {DEFAULT_TEMPERATURE}).",
    )

    # -- Runtime params --
    rt = p.add_argument_group("Runtime parameters")
    rt.add_argument(
        "--max-rpm", type=int, default=None,
        help="Max requests/min — overrides experiments.yaml rate_limit.",
    )
    rt.add_argument(
        "--max-workers", type=int, default=None,
        help="Concurrent worker threads — overrides experiments.yaml.",
    )
    rt.add_argument(
        "--limit", type=int, default=None,
        help="Process only the first N unprocessed queries (for testing).",
    )

    p.add_argument("--mock", action="store_true", help="Run without API calls.")

    return p.parse_args()


def main() -> None:
    args = _parse_args()

    # ── Resolve schema ──────────────────────────────────────────────
    from src.core.schema_loader import load_schema

    schema_cfg = load_schema(args.schema)
    schema_name = schema_cfg.schema_name

    input_file = args.input or f"dataset/{schema_name}/raw_queries.json"
    output_file = args.output or f"dataset/{schema_name}/llm_perturbations.json"

    # ── Build system prompt ─────────────────────────────────────────
    schema_ctx = _build_schema_context(args.schema)
    perturbation_text = _load_perturbation_text()
    system_prompt = _build_system_prompt(schema_ctx, perturbation_text)

    # ── Create adapter ──────────────────────────────────────────────
    model_id: str
    max_rpm = args.max_rpm or DEFAULT_MAX_RPM
    max_workers = args.max_workers or DEFAULT_MAX_WORKERS

    if args.mock:
        adapter = None
        model_id = "mock"
    elif args.model and not (args.adapter_type and args.model_id):
        adapter, model_cfg = _create_adapter_from_config(
            args.model,
            args.experiments_config,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            system_prompt=system_prompt,
        )
        model_id = model_cfg.model_identifier
        # Use rate limit / concurrency from config if not overridden
        if model_cfg.rate_limit and args.max_rpm is None:
            max_rpm = model_cfg.rate_limit.get("requests_per_minute", DEFAULT_MAX_RPM)
        if model_cfg.rate_limit and args.max_workers is None:
            max_workers = model_cfg.rate_limit.get(
                "max_concurrent_requests", DEFAULT_MAX_WORKERS
            )
    elif args.adapter_type and args.model_id:
        adapter = _create_adapter_adhoc(
            args.adapter_type,
            args.model_id,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            system_prompt=system_prompt,
        )
        model_id = args.model_id
    else:
        sys.exit(
            "ERROR: Provide either --model <name> (from experiments.yaml) "
            "or both --adapter-type and --model-id."
        )

    print(f"Model:  {model_id}")
    print(f"Schema: {schema_name} ({args.schema})")

    # ── Run ──────────────────────────────────────────────────────────
    process_queries(
        input_file=input_file,
        output_file=output_file,
        adapter=adapter,
        model_id=model_id,
        schema_path=args.schema,
        mock=args.mock,
        max_rpm=max_rpm,
        max_workers=max_workers,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
