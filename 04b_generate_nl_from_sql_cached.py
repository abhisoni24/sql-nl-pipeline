"""
Generate NL prompts and LLM-based perturbations from raw SQL queries using Gemini
and context caching. Sends ONLY the SQL to the LLM and asks it to:
  1. Generate a natural language prompt for the SQL
  2. Generate 14 single-perturbation versions + 1 compound perturbation of
     the NL prompt it created.
Produces nl_social_media_queries_llm_generated_20.json.

Uses concurrent requests (ThreadPoolExecutor) to maximize throughput.
"""

import os
import sys
import json
import time
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Constants
INPUT_FILE = "./dataset/current/raw_social_media_queries_20.json"
OUTPUT_FILE = "./dataset/current/nl_social_media_queries_llm_generated_20.json"
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


def load_cached_info_text() -> str:
    base_dir = os.path.abspath(os.path.dirname(__file__))
    cached_info_path = os.path.join(base_dir, "cached_info.py")
    with open(cached_info_path, "r") as f:
        return f.read()


def create_cache(client: genai.Client, model_name: str) -> types.CachedContent:
    cached_text = load_cached_info_text()

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


def process_queries(
    mock: bool = False,
    max_rpm: int = DEFAULT_MAX_RPM,
    max_workers: int = DEFAULT_MAX_WORKERS,
    limit: Optional[int] = None,
) -> None:
    base_dir = os.path.abspath(os.path.dirname(__file__))
    input_path = os.path.join(base_dir, INPUT_FILE)
    output_path = os.path.join(base_dir, OUTPUT_FILE)

    print(f"Reading from: {input_path}")
    print(f"Writing to:   {output_path}")

    with open(input_path, "r") as f:
        queries = json.load(f)

    # ---- Resume support ----
    processed_data: List[Dict[str, Any]] = []
    processed_ids = set()
    if os.path.exists(output_path):
        try:
            with open(output_path, "r") as f:
                processed_data = json.load(f)
                processed_ids = {item["id"] for item in processed_data if "id" in item}
            print(f"Resuming — {len(processed_data)} queries already processed.")
        except json.JSONDecodeError:
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
        cache = create_cache(client, MODEL_NAME)
        print(f"Cache created: {cache.name}")

    # ---- Concurrent processing ----
    success_count = 0
    fail_count = 0
    data_lock = threading.Lock()
    unsaved_count = 0

    def save_checkpoint():
        nonlocal unsaved_count
        with open(output_path, "w") as f:
            json.dump(processed_data, f, indent=2)
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
    with open(output_path, "w") as f:
        json.dump(processed_data, f, indent=2)

    print(f"\nCompleted. Success: {success_count}, Fail: {fail_count}")
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate NL prompts + perturbations from raw SQL using Gemini context caching."
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

    process_queries(
        mock=args.mock,
        max_rpm=args.max_rpm,
        max_workers=args.max_workers,
        limit=args.limit,
    )
