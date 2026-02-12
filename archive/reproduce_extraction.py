
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.abspath('.'))

from src.utils.sql_utils import extract_sql

# Test cases from the log
test_cases = [
    "\n```sql\nSELECT *\nFROM likes\nLEFT JOIN posts ON likes.post_id = posts.id\nWHERE likes.user_id < 606",
    "\n```sql\nSELECT *\nFROM comments\nWHERE post_id = 147\nLIMIT 84",
    "\n```sql\nUPDATE users\nSET signup_date = NOW()\nWHERE is_verified = 1"
]

print("--- Testing Extraction ---")
for i, case in enumerate(test_cases):
    extracted = extract_sql(case)
    print(f"Case {i+1}:")
    print(f"Input: {repr(case)}")
    print(f"Extracted: {repr(extracted)}")
    print(f"Expected: No backticks")
    print("-" * 20)
