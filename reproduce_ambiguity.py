
import sys
import os
from sqlglot import parse_one, exp

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from src.core.nl_renderer import SQLToNLRenderer, PerturbationConfig, PerturbationType

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

def check_with_config(sql, expected_snippet, config, should_contain=True):
    """Like check(), but accepts a PerturbationConfig to test specific perturbation combos."""
    renderer = SQLToNLRenderer(config)
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

    # --- Phase 5: Perturbation Interaction Bug Fixes ---
    print("\n" + "="*60)
    print("--- Phase 5: Perturbation Interaction Bug Fixes ---")
    print("="*60)

    # Test 11a: Grammar Salad — TEMPORAL_EXPRESSION_VARIATION ALONE
    print("\n--- Test 11a: Grammar Salad - TEMPORAL only (Phase 5) ---")
    temporal_only_config = PerturbationConfig(
        active_perturbations={PerturbationType.TEMPORAL_EXPRESSION_VARIATION}
    )
    # Should NOT produce "greater than or equal to within the last"
    check_with_config(
        "SELECT * FROM posts WHERE posted_at >= DATETIME('now', '-22 days')",
        "greater than",
        temporal_only_config,
        False
    )

    # Test 11b: Grammar Salad — BOTH active
    print("\n--- Test 11b: Grammar Salad - BOTH active (Phase 5) ---")
    combo_config = PerturbationConfig(
        active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION, PerturbationType.TEMPORAL_EXPRESSION_VARIATION}
    )
    check_with_config(
        "SELECT * FROM posts WHERE posted_at >= DATETIME('now', '-22 days')",
        "greater than",
        combo_config,
        False
    )
    # Also ensure no "no less than" or "exceeds" with temporal
    check_with_config(
        "SELECT * FROM posts WHERE posted_at >= DATETIME('now', '-22 days')",
        "no less than",
        combo_config,
        False
    )

    # Test 12: Semantic Inversion / Date Logic Ambiguity — OPERATOR_AGGREGATE only
    print("\n--- Test 12: Date Logic Ambiguity - OPERATOR only (Phase 5) ---")
    op_config = PerturbationConfig(
        active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION}
    )
    # Should NOT produce "exceeds" for temporal context
    check_with_config(
        "SELECT * FROM posts WHERE posted_at > DATETIME('now', '-30 days')",
        "exceeds",
        op_config,
        False
    )
    # Should KEEP "ago" so meaning is clear (e.g., "on or after 22 days ago" not "on or after 22 days")
    nl = check_with_config(
        "SELECT * FROM posts WHERE posted_at >= DATETIME('now', '-22 days')",
        "ago",
        op_config,
        True
    )

    # Bug 3: Russian Roulette - AMBIGUOUS_PRONOUNS with many columns
    print("\n--- Test 13: Russian Roulette Reference (Bug 3 - Phase 5) ---")
    pronoun_config = PerturbationConfig(
        active_perturbations={PerturbationType.AMBIGUOUS_PRONOUNS}
    )
    # Multi-column query: 4+ distinct columns mentioned -> no pronoun should be used
    check_with_config(
        "SELECT c.id, c.comment_text, c.post_id, p.user_id FROM posts AS p FULL JOIN comments AS c ON p.id = c.post_id",
        "the aforementioned column",
        pronoun_config,
        False
    )
    check_with_config(
        "SELECT f.follower_id, f.followee_id FROM follows AS f ORDER BY f.follower_id",
        "the aforementioned column",
        pronoun_config,
        False
    )

    # Bug 4: Self-Join Pronoun Ambiguity
    print("\n--- Test 14: Self-Join Pronoun Ambiguity (Bug 4 - Phase 5) ---")
    pronoun_config = PerturbationConfig(
        active_perturbations={PerturbationType.AMBIGUOUS_PRONOUNS}
    )
    # Self-join: same table twice -> no pronouns should be used at all
    check_with_config(
        "SELECT c1.comment_text, c2.comment_text FROM comments AS c1 JOIN comments AS c2 ON c1.post_id = c2.post_id WHERE c1.id != c2.id",
        "that table",
        pronoun_config,
        False
    )
    check_with_config(
        "SELECT c1.comment_text, c2.comment_text FROM comments AS c1 JOIN comments AS c2 ON c1.post_id = c2.post_id WHERE c1.id != c2.id",
        "the aforementioned",
        pronoun_config,
        False
    )

    # Bug 5: "From X days ago" -> "From X days ago onwards"
    print("\n--- Test 15: Temporal Directionality (Bug 5 - Phase 5) ---")
    op_config = PerturbationConfig(
        active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION}
    )
    # Run multiple times to check if "from" variant includes "onwards"
    # We'll check the generated output doesn't end with just "ago" when using "from"
    found_from = False
    for seed in range(20):
        op_config_seed = PerturbationConfig(
            active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION},
            seed=seed
        )
        from src.core.nl_renderer import SQLToNLRenderer as R
        r = R(op_config_seed)
        ast = parse_one("DELETE FROM comments WHERE created_at >= DATETIME('now', '-1 days')")
        nl = r.render(ast)
        if "from" in nl and "ago" in nl:
            found_from = True
            if "onwards" in nl:
                print(f"  [PASS] seed={seed}: '{nl}' -> has 'onwards'")
            else:
                print(f"  [FAIL] seed={seed}: '{nl}' -> MISSING 'onwards'")
            break
    if not found_from:
        # Try to verify at least that 'from' isn't used without 'onwards' 
        print("  [INFO] 'from' variant not hit in first 20 seeds, running broader check...")
        check_with_config(
            "DELETE FROM comments WHERE created_at >= DATETIME('now', '-1 days')",
            "from 1 day ago onwards",
            PerturbationConfig(active_perturbations={PerturbationType.OPERATOR_AGGREGATE_VARIATION}, seed=0),
            True
        )
