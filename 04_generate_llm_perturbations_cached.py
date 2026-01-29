"""
Generate LLM-based perturbations for nl_social_media_queries.json using Gemini
and context caching, producing nl_social_mediia_queries_llm_perturbedd.json.
"""

import os
import sys
import json
import time
import argparse
from typing import Any, Dict, List
from tqdm import tqdm
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Ensure project root is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# Constants
INPUT_FILE = "./dataset/current/nl_social_media_queries_20.json"    
OUTPUT_FILE = "./dataset/current/nl_social_media_queries_llm_perturbed_20.json"
MODEL_NAME = "gemini-2.5-flash-lite"
CACHE_TTL = "12600s"
DEFAULT_MAX_RPM = 4000


def setup_client() -> genai.Client:
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set.")
    return genai.Client(api_key=api_key)


def load_cached_info_text() -> str:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    cached_info_path = os.path.join(base_dir, "src/scripts/cached_info.py")
    with open(cached_info_path, "r") as f:
        return f.read()


def create_cache(client: genai.Client, model_name: str) -> types.CachedContent:
    cached_text = load_cached_info_text()

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
Generate 14 single-perturbation versions and 1 compound-perturbation version
(with 2-5 perturbations) of the given natural language prompt.

# Input Data
nl_prompt: {nl_prompt}
sql: {sql}
tables: {tables}
complexity: {complexity}
"""


def process_queries(
    mock: bool = False,
    max_rpm: int = DEFAULT_MAX_RPM,
    limit: int | None = None,
) -> None:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    input_path = os.path.join(base_dir, INPUT_FILE)
    output_path = os.path.join(base_dir, OUTPUT_FILE)

    print(f"Reading from: {input_path}")
    print(f"Writing to: {output_path}")

    with open(input_path, "r") as f:
        queries = json.load(f)

    processed_data: List[Dict[str, Any]] = []
    processed_ids = set()
    if os.path.exists(output_path):
        try:
            with open(output_path, "r") as f:
                processed_data = json.load(f)
                processed_ids = {item["id"] for item in processed_data if "id" in item}
            print(f"Found existing output with {len(processed_data)} processed queries. Resuming...")
        except json.JSONDecodeError:
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
        cache = create_cache(client, MODEL_NAME)
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
            with open(output_path, "w") as f:
                json.dump(processed_data, f, indent=2)

    with open(output_path, "w") as f:
        json.dump(processed_data, f, indent=2)

    print(f"\nCompleted. Success: {success_count}, Fail: {fail_count}")
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate LLM perturbations using Gemini context caching."
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

    process_queries(mock=args.mock, max_rpm=args.max_rpm, limit=args.limit)
