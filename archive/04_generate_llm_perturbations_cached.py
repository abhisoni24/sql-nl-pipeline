"""
Step 4: Generate LLM-based perturbations using Gemini and context caching.

Takes NL-augmented queries and sends them to Gemini to generate 14 single-
perturbation versions and 1 compound perturbation.

Usage
-----
  # Schema-driven (recommended)
  python 04_generate_llm_perturbations_cached.py --schema schemas/bank.yaml

  # With explicit I/O
  python 04_generate_llm_perturbations_cached.py \\
      -i dataset/current/nl_bank_queries.json \\
      -o dataset/current/nl_bank_queries_llm_perturbed.json \\
      --schema schemas/bank.yaml

  # Legacy (social_media defaults)
  python 04_generate_llm_perturbations_cached.py

  # Mock mode (no API calls)
  python 04_generate_llm_perturbations_cached.py --mock
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

MODEL_NAME = "gemini-2.5-flash-lite"
CACHE_TTL = "3600s"
DEFAULT_MAX_RPM = 4000


def setup_client() -> genai.Client:
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set.")
    return genai.Client(api_key=api_key)


def load_cached_info_text(schema_path: Optional[str] = None) -> str:
    """Load the cached info template text, optionally injecting a schema."""
    base_dir = os.path.abspath(os.path.dirname(__file__))
    cached_info_path = os.path.join(base_dir, "cached_info.py")
    with open(cached_info_path, "r") as f:
        text = f.read()

    # If a schema YAML is supplied, replace the hardcoded schema/FK block
    if schema_path:
        from src.core.schema_loader import load_from_yaml
        cfg = load_from_yaml(schema_path)
        schema_dict = cfg.get_legacy_schema()
        fk_dict = cfg.get_fk_pairs()
        # Convert to a Python-literal string that mirrors the original format
        schema_str = "schema = " + json.dumps(schema_dict, indent=4)
        fk_str = "foreign_keys = " + repr(fk_dict)
        # Replace existing schema block (from "schema = {" through the FK block)
        import re
        text = re.sub(
            r'schema = \{.*?\n\}',
            schema_str,
            text,
            flags=re.DOTALL,
        )
        text = re.sub(
            r'foreign_keys = \{.*?\n\}',
            fk_str,
            text,
            flags=re.DOTALL,
        )
    return text


def create_cache(
    client: genai.Client,
    model_name: str,
    schema_path: Optional[str] = None,
) -> types.CachedContent:
    cached_text = load_cached_info_text(schema_path)

    cache = client.caches.create(
        model=model_name,
        config=types.CreateCachedContentConfig(
            display_name="task_info_llm_perturbations_v1",
            system_instruction=(
                "You are an expert at generating realistic perturbations of natural language "
                "database query prompts. The task details are provided in the cached content."
            ),
            contents=[cached_text],
            ttl=CACHE_TTL,
        ),
    )
    return cache


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _load_records(path: str):
    """Load records from either a bare JSON list or a metadata-wrapped envelope."""
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, dict) and "records" in data:
        return data["records"], data.get("metadata", {})
    return data, {}


def clean_json_response(response_text: str) -> str:
    cleaned_text = response_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[7:]
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[3:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3]
    return cleaned_text.strip()


class RateLimiter:
    def __init__(self, max_rpm: int) -> None:
        self.min_interval = 60.0 / max_rpm
        self._last_time = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_time = time.monotonic()


def build_prompt(query: Dict[str, Any]) -> str:
    nl_prompt = query.get("nl_prompt", "")
    sql = query.get("sql", "")
    tables = query.get("tables", [])
    complexity = query.get("complexity", "unknown")

    return f"""
# Task
Following the instructions in the cached content, generate 14 single-perturbation versions and 1 compound-perturbation version (with 2-5 perturbations) of the given natural language prompt.

# Input Data
nl_prompt: {nl_prompt}
sql: {sql}
tables: {tables}
complexity: {complexity}

Ensure you return ONLY the JSON object as specified in the output format instructions.
"""


def process_queries(
    input_file: str,
    output_file: str,
    schema_path: Optional[str] = None,
    mock: bool = False,
    max_rpm: int = DEFAULT_MAX_RPM,
    limit: Optional[int] = None,
) -> None:
    input_path = os.path.abspath(input_file)
    output_path = os.path.abspath(output_file)

    print(f"Reading from: {input_path}")
    print(f"Writing to: {output_path}")

    queries, upstream_meta = _load_records(input_path)

    processed_data: List[Dict[str, Any]] = []
    processed_ids = set()
    if os.path.exists(output_path):
        try:
            existing, _ = _load_records(output_path)
            processed_data = existing
            processed_ids = {item["id"] for item in processed_data if "id" in item}
            print(f"Found existing output with {len(processed_data)} processed queries. Resuming...")
        except (json.JSONDecodeError, Exception):
            print("Output file exists but is invalid/empty. Starting fresh.")

    queries_to_process = [q for q in queries if q.get("id") not in processed_ids]
    if limit is not None:
        queries_to_process = queries_to_process[:limit]
    print(f"Total queries: {len(queries)}")
    print(f"Already processed: {len(processed_ids)}")
    print(f"Remaining: {len(queries_to_process)}")

    if not queries_to_process:
        print("All queries processed!")
        return

    client = None
    cache = None
    limiter = RateLimiter(max_rpm)

    if not mock:
        client = setup_client()
        print("Setting up cache...")
        cache = create_cache(client, MODEL_NAME, schema_path=schema_path)
        print(f"Cache created: {cache.name}")

    success_count = 0
    fail_count = 0

    pbar = tqdm(queries_to_process, desc="Processing Queries", unit="query")

    for query in pbar:
        try:
            if mock:
                response_text = json.dumps({
                    "original": {"nl_prompt": query.get("nl_prompt", "")},
                    "single_perturbations": [],
                    "compound_perturbation": {
                        "perturbations_applied": [],
                        "perturbed_nl_prompt": query.get("nl_prompt", ""),
                        "changes_made": "mock"
                    },
                    "metadata": {}
                })
            else:
                max_retries = 5
                retry_delay = 2.0
                last_error = None

                for attempt in range(max_retries):
                    try:
                        limiter.wait()
                        response = client.models.generate_content(
                            model=MODEL_NAME,
                            contents=build_prompt(query),
                            config=types.GenerateContentConfig(cached_content=cache.name),
                        )
                        response_text = response.text
                        last_error = None
                        break
                    except Exception as e:
                        last_error = e
                        err_msg = str(e).lower()
                        if "429" in err_msg or "rate" in err_msg:
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        raise

                if last_error is not None:
                    raise last_error

            cleaned_text = clean_json_response(response_text)
            perturbation_data = json.loads(cleaned_text)

            enriched_query = query.copy()
            enriched_query["generated_perturbations"] = perturbation_data
            processed_data.append(enriched_query)
            success_count += 1
        except Exception as e:
            fail_count += 1
            tqdm.write(f"Failed query ID {query.get('id', 'unknown')}: {e}")
            # Do not append failed item so it can be retried later

        pbar.set_postfix({"Success": success_count, "Fail": fail_count})

        if success_count % 10 == 0:
            _save_output(output_path, processed_data, schema_path, upstream_meta)

    _save_output(output_path, processed_data, schema_path, upstream_meta)

    print(f"\nCompleted. Success: {success_count}, Fail: {fail_count}")
    print(f"Results saved to {output_path}")


def _save_output(
    path: str,
    records: List[Dict[str, Any]],
    schema_path: Optional[str],
    upstream_meta: Dict[str, Any],
) -> None:
    """Persist records wrapped in a metadata envelope."""
    schema_name = upstream_meta.get("schema_name", "unknown")
    if schema_path:
        from src.core.schema_loader import load_from_yaml
        cfg = load_from_yaml(schema_path)
        schema_name = cfg.schema_name

    envelope = {
        "metadata": {
            "pipeline_step": "04_llm_perturbations",
            "schema_name": schema_name,
            "schema_source": schema_path or "legacy_default",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "num_records": len(records),
            "model": MODEL_NAME,
            "upstream": upstream_meta if upstream_meta else None,
        },
        "records": records,
    }
    with open(path, "w") as f:
        json.dump(envelope, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate LLM perturbations using Gemini context caching.",
    )
    parser.add_argument(
        "--schema", "-s",
        default=None,
        help="Path to schema YAML (e.g. schemas/bank.yaml). Injects schema into "
             "the Gemini cache context and derives default I/O paths.",
    )
    parser.add_argument(
        "--input", "-i",
        default=None,
        help="Input JSON path (default: dataset/current/nl_<schema>_queries.json).",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output JSON path (default: dataset/current/nl_<schema>_queries_llm_perturbed.json).",
    )
    parser.add_argument("--mock", action="store_true", help="Run without API calls.")
    parser.add_argument(
        "--max-rpm",
        type=int,
        default=DEFAULT_MAX_RPM,
        help="Max requests per minute to avoid rate limiting (default: 4000).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N unprocessed queries (for testing).",
    )
    args = parser.parse_args()

    # Derive schema-aware defaults when --schema is supplied
    schema_name = "social_media"  # legacy fallback
    if args.schema:
        from src.core.schema_loader import load_from_yaml
        schema_name = load_from_yaml(args.schema).schema_name

    input_file = args.input or f"dataset/current/nl_{schema_name}_queries.json"
    output_file = args.output or f"dataset/current/nl_{schema_name}_queries_llm_perturbed.json"

    process_queries(
        input_file=input_file,
        output_file=output_file,
        schema_path=args.schema,
        mock=args.mock,
        max_rpm=args.max_rpm,
        limit=args.limit,
    )
