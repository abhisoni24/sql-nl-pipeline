
import sys
import os
from sqlglot import parse_one, exp

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig

def check(sql, expected_snippet, should_contain=True):
    renderer = SQLToNLRenderer()
    ast = parse_one(sql)
    nl = renderer.render(ast)
    print(f"SQL: {sql}")
    print(f"NL:  {nl}")
    
    if should_contain:
        if expected_snippet in nl:
            print(f"[PASS] Found '{expected_snippet}'")
        else:
            print(f"[FAIL] Missing '{expected_snippet}'")
    else:
        if expected_snippet in nl:
            print(f"[FAIL] Found banned '{expected_snippet}'")
        else:
            print(f"[PASS] Successfully avoided '{expected_snippet}'")
    return nl

def test_renderer():
    print("--- Test 1: Indistinguishable Twins (Bug 1 - Phase 1) ---")
    # Should render: "Show the u5.username, p1.id, u5.id..." (disambiguated)
    check(
        "SELECT u5.username, p1.id, u5.id, u5.email FROM posts AS p1 RIGHT JOIN users AS u5 ON p1.user_id = u5.id",
        "id, id",
        False
    )

    print("\n--- Test 2: Date Logic (Bug 2 - Phase 1) ---")
    # Should NOT render "greater than ... ago"
    check(
        "SELECT * FROM users WHERE signup_date > DATETIME('now', '-25 days')",
        "greater than", 
        False
    )
    
    print("\n--- Test 3: Alias Leakage (Bug 1 - Phase 2) ---")
    # Should render: "...ordered by the user's email" (u1 -> "the user's")
    # Because u1 is the ONLY user alias.
    check(
        "SELECT * FROM users AS u1 ORDER BY u1.email",
        "u1.email",
        False
    )
    
    print("\n--- Test 4: Alias Preservation (Negative Test - Phase 2) ---")
    # Should render: "...p1.user_id equals u5.id" or similar 
    # Because BOTH p1 and u5 exist (different tables), AND if we had multiple u's they should stay.
    # Let's test specific multi-alias valid collision avoidance:
    # Query with self-join: users u1, users u2.
    # Should NOT render: "the user's email equals the user's email"
    check(
        "SELECT u1.email, u2.email FROM users AS u1 JOIN users AS u2 ON u1.id = u2.referrer_id",
        "u1.email", # We EXPECT to see u1/u2 here, NOT "the user's"
        True
    )

    print("\n--- Test 5: Raw SQL Injection (Bug 2 - Phase 2) ---")
    # Should not contain: "exists (Select 1 from..."
    check(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)",
        "exists (Select",
        False
    )
    
    print("\n--- Test 6: Internal Artifacts (Bug 3 - Phase 2) ---")
    # Should not contain: "inner_users"
    check(
        "SELECT * FROM (SELECT * FROM users) AS inner_users WHERE inner_users.id = 1",
        "inner_users",
        False
    )

    print("\n--- Test 7: Date Logic (>= and <=) (Bug 5 - Phase 3) ---")
    # Should render "within the last 22 days inclusive" for >= -22 days
    check(
        "SELECT * FROM posts WHERE posted_at >= DATETIME('now', '-22 days')",
        "within the last 22 days inclusive",
        True
    )
    # Should render "older than 22 days inclusive" for <= -22 days
    check(
        "SELECT * FROM posts WHERE posted_at <= DATETIME('now', '-22 days')",
        "older than 22 days inclusive",
        True
    )

    print("\n--- Test 8: Derived Table Leakage (Bug C - Phase 4) ---")
    # Query with derived table
    # Should render: "the result's id" instead of "the results.id"
    check(
        "SELECT * FROM (SELECT * FROM users) AS derived_table WHERE derived_table.id = 1",
        "result's id",
        True
    )
    
    print("\n--- Test 9: Redundant Qualification (Bug D - Phase 4) ---")
    # Single table query
    # Should render: "delete users where country_code equals" (no users.country_code)
    check(
        "DELETE FROM users WHERE users.country_code = '123'",
        "users.country_code",
        False
    )
    # Check positive case: "country_code equals"
    check(
        "DELETE FROM users WHERE users.country_code = '123'",
        "country_code equals",
        True
    )

    print("\n--- Test 10: Double 'The' Stutter (Bug E - Phase 4) ---")
    # Should render: "Display the user's email" (not "Display the the user's email")
    # This happens when we map u1 -> "the user's" and render_select adds another "the".
    check(
        "SELECT u1.email FROM users AS u1",
        "the the",
        False
    )
    # Positive check
    check(
        "SELECT u1.email FROM users AS u1",
        "the user's email",
        True
    )

if __name__ == "__main__":
    test_renderer()
