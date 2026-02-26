"""
Step 4b: Generate NL prompts + LLM perturbations from raw SQL using Gemini.

Sends ONLY the SQL to the LLM and asks it to:
  1. Generate a natural language prompt for the SQL
  2. Generate 14 single-perturbation versions + 1 compound perturbation of
     the NL prompt it created.

Uses concurrent requests (ThreadPoolExecutor) to maximize throughput.

Usage
-----
  # Schema-driven (recommended)
  python 04b_generate_nl_from_sql_cached.py --schema schemas/bank.yaml

  # With explicit I/O
  python 04b_generate_nl_from_sql_cached.py \\
      -i dataset/current/raw_bank_queries.json \\
      -o dataset/current/nl_bank_queries_llm_generated.json \\
      --schema schemas/bank.yaml

  # Legacy (social_media defaults)
  python 04b_generate_nl_from_sql_cached.py

  # Mock mode (no API calls)
  python 04b_generate_nl_from_sql_cached.py --mock
"""

import os
import sys
import json
import time
import threading
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Constants
MODEL_NAME = "gemini-2.5-flash-lite"
CACHE_TTL = "3600s"
DEFAULT_MAX_RPM = 4000
DEFAULT_MAX_WORKERS = 28
SAVE_EVERY_N = 10


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

    if schema_path:
        import re as _re
        from src.core.schema_loader import load_from_yaml
        cfg = load_from_yaml(schema_path)
        schema_dict = cfg.get_legacy_schema()
        fk_dict = cfg.get_fk_pairs()
        schema_str = "schema = " + json.dumps(schema_dict, indent=4)
        fk_str = "foreign_keys = " + repr(fk_dict)
        text = _re.sub(r'schema = \{.*?\n\}', schema_str, text, flags=_re.DOTALL)
        text = _re.sub(r'foreign_keys = \{.*?\n\}', fk_str, text, flags=_re.DOTALL)
    return text


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


def create_cache(
    client: genai.Client,
    model_name: str,
    schema_path: Optional[str] = None,
) -> types.CachedContent:
    cached_text = load_cached_info_text(schema_path)

    cache = client.caches.create(
        model=model_name,
        config=types.CreateCachedContentConfig(
            display_name="task_info_sql_to_nl_perturbations_v1",
            system_instruction=(
                "You are an expert at interpreting SQL queries and expressing them as "
                "natural language prompts that a developer might type into an NL-to-SQL "
                "system. You are also expert at generating realistic perturbations of "
                "those natural language prompts. The perturbation type definitions, "
                "database schema, and output format instructions are provided in the "
                "cached content."
            ),
            contents=[cached_text],
            ttl=CACHE_TTL,
        ),
    )
    return cache


def clean_json_response(response_text: str) -> str:
    cleaned_text = response_text.strip()
    if cleaned_text.startswith("```json"):
        cleaned_text = cleaned_text[7:]
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[3:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3]
    return cleaned_text.strip()


class ThreadSafeRateLimiter:
    """Token-bucket style rate limiter that is safe for concurrent use."""

    def __init__(self, max_rpm: int) -> None:
        self.min_interval = 60.0 / max_rpm
        self._lock = threading.Lock()
        self._last_time = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_time = self.min_interval - (now - self._last_time)
            if wait_time > 0:
                target = self._last_time + self.min_interval
            else:
                target = now
            self._last_time = target

        # Sleep outside the lock so other threads can schedule
        sleep_time = target - time.monotonic()
        if sleep_time > 0:
            time.sleep(sleep_time)


def build_prompt(query: Dict[str, Any]) -> str:
    sql = query.get("sql", "")
    tables = query.get("tables", [])
    complexity = query.get("complexity", "unknown")

    return f"""
# Task
You are given ONLY a SQL query (no natural language prompt). You must:

1. **Generate a natural language prompt** — Write a clear, concise natural language
   request that a developer might type into an NL-to-SQL system to produce the given
   SQL query. This should read like something a real person would write (not a
   mechanical SQL-to-English translation).

2. **Generate perturbations** — Following the perturbation type instructions in the
   cached content, generate 14 single-perturbation versions and 1 compound-perturbation
   version (with 2-5 perturbations) of the natural language prompt you created in step 1.

# Input Data
sql: {sql}
tables: {tables}
complexity: {complexity}

# Output Format
Return ONLY a valid JSON object with this exact structure:
```json
{{
  "generated_nl_prompt": "<the natural language prompt you created for the SQL>",
  "original": {{
    "nl_prompt": "<same as generated_nl_prompt>",
    "sql": "<original SQL>",
    "tables": ["<table names>"],
    "complexity": "<complexity level>"
  }},
  "single_perturbations": [
    {{
      "perturbation_id": 1,
      "perturbation_name": "<perturbation type name>",
      "applicable": true,
      "perturbed_nl_prompt": "<perturbed version>",
      "changes_made": "<brief description of what was changed>",
      "reason_not_applicable": null
    }},
    {{
      "perturbation_id": 2,
      "perturbation_name": "<perturbation type name>",
      "applicable": false,
      "perturbed_nl_prompt": null,
      "changes_made": null,
      "reason_not_applicable": "<explanation why this perturbation doesn't apply>"
    }}
  ],
  "compound_perturbation": {{
    "perturbations_applied": [
      {{
        "perturbation_id": 1,
        "perturbation_name": "<name>"
      }}
    ],
    "perturbed_nl_prompt": "<compound perturbed version>",
    "changes_made": "<description of all changes made>"
  }},
  "metadata": {{
    "total_applicable_perturbations": 8,
    "total_not_applicable": 6,
    "applicability_rate": 0.57
  }}
}}
```

# Important Notes
1. The generated_nl_prompt should be natural and developer-like, NOT a mechanical
   word-for-word translation of the SQL.
2. All perturbations are applied to the nl_prompt YOU generated, not the SQL.
3. Ensure the JSON is valid and properly formatted.
4. For not_applicable perturbations, set perturbed_nl_prompt to null.
5. For the compound perturbation, only use applicable perturbations.
6. Return ONLY the JSON object, no additional text.
"""


def process_single_query(
    query: Dict[str, Any],
    client: genai.Client,
    cache: types.CachedContent,
    limiter: ThreadSafeRateLimiter,
    mock: bool,
) -> Dict[str, Any]:
    """Process a single query — designed to run inside a thread."""
    if mock:
        response_text = json.dumps({
            "generated_nl_prompt": f"Show everything from the {', '.join(query.get('tables', ['table']))} table",
            "original": {
                "nl_prompt": f"Show everything from the {', '.join(query.get('tables', ['table']))} table",
                "sql": query.get("sql", ""),
                "tables": query.get("tables", []),
                "complexity": query.get("complexity", "unknown"),
            },
            "single_perturbations": [],
            "compound_perturbation": {
                "perturbations_applied": [],
                "perturbed_nl_prompt": f"Show everything from the {', '.join(query.get('tables', ['table']))} table",
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
    enriched_query["nl_prompt"] = perturbation_data.get("generated_nl_prompt", "")
    enriched_query["generated_perturbations"] = perturbation_data
    return enriched_query


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
            "pipeline_step": "04b_nl_from_sql",
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


def process_queries(
    input_file: str,
    output_file: str,
    schema_path: Optional[str] = None,
    mock: bool = False,
    max_rpm: int = DEFAULT_MAX_RPM,
    max_workers: int = DEFAULT_MAX_WORKERS,
    limit: Optional[int] = None,
) -> None:
    input_path = os.path.abspath(input_file)
    output_path = os.path.abspath(output_file)

    print(f"Reading from: {input_path}")
    print(f"Writing to:   {output_path}")

    queries, upstream_meta = _load_records(input_path)

    # ---- Resume support ----
    processed_data: List[Dict[str, Any]] = []
    processed_ids = set()
    if os.path.exists(output_path):
        try:
            existing, _ = _load_records(output_path)
            processed_data = existing
            processed_ids = {item["id"] for item in processed_data if "id" in item}
            print(f"Resuming — {len(processed_data)} queries already processed.")
        except (json.JSONDecodeError, Exception):
            print("Output file exists but is invalid/empty. Starting fresh.")

    queries_to_process = [q for q in queries if q.get("id") not in processed_ids]
    if limit is not None:
        queries_to_process = queries_to_process[:limit]

    print(f"Total queries: {len(queries)}")
    print(f"Already processed: {len(processed_ids)}")
    print(f"Remaining: {len(queries_to_process)}")
    print(f"Concurrency: {max_workers} workers  |  Rate limit: {max_rpm} RPM")

    if not queries_to_process:
        print("All queries processed!")
        return

    client = None
    cache = None
    limiter = ThreadSafeRateLimiter(max_rpm)

    if not mock:
        client = setup_client()
        print("Setting up cache...")
        cache = create_cache(client, MODEL_NAME, schema_path=schema_path)
        print(f"Cache created: {cache.name}")

    # ---- Concurrent processing ----
    success_count = 0
    fail_count = 0
    data_lock = threading.Lock()
    unsaved_count = 0

    def save_checkpoint():
        nonlocal unsaved_count
        _save_output(output_path, processed_data, schema_path, upstream_meta)
        unsaved_count = 0

    pbar = tqdm(total=len(queries_to_process), desc="Processing Queries", unit="query")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_query = {
            executor.submit(
                process_single_query, query, client, cache, limiter, mock
            ): query
            for query in queries_to_process
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
                        save_checkpoint()
            except Exception as e:
                with data_lock:
                    fail_count += 1
                tqdm.write(f"Failed query ID {query.get('id', 'unknown')}: {e}")

            pbar.update(1)
            pbar.set_postfix({"OK": success_count, "Fail": fail_count})

    pbar.close()

    # Final save
    _save_output(output_path, processed_data, schema_path, upstream_meta)

    print(f"\nCompleted. Success: {success_count}, Fail: {fail_count}")
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate NL prompts + perturbations from raw SQL using Gemini context caching.",
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
        help="Input JSON path (default: dataset/current/raw_<schema>_queries.json).",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output JSON path (default: dataset/current/nl_<schema>_queries_llm_generated.json).",
    )
    parser.add_argument("--mock", action="store_true", help="Run without API calls.")
    parser.add_argument(
        "--max-rpm",
        type=int,
        default=DEFAULT_MAX_RPM,
        help=f"Max requests per minute (default: {DEFAULT_MAX_RPM}).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Number of concurrent worker threads (default: {DEFAULT_MAX_WORKERS}).",
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

    input_file = args.input or f"dataset/current/raw_{schema_name}_queries.json"
    output_file = args.output or f"dataset/current/nl_{schema_name}_queries_llm_generated.json"

    process_queries(
        input_file=input_file,
        output_file=output_file,
        schema_path=args.schema,
        mock=args.mock,
        max_rpm=args.max_rpm,
        max_workers=args.max_workers,
        limit=args.limit,
    )
