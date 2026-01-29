"""
Test script for SQL-NL Pipeline V2 Bug Fixes 3 & 4.

V2 Bug 3: Derived tables should render as natural language, not raw SQL
V2 Bug 4: IN subqueries should not have double parentheses

Run from project root:
    python test_scripts/test_bug_fixes_v2_3_4.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_v2_bug3_derived_tables():
    """Test that derived tables render as natural language."""
    print("\n" + "=" * 60)
    print("V2 BUG 3 TEST: Derived Table Rendering")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        # (SQL, expected_tokens, disallowed_tokens)
        ("SELECT * FROM (SELECT id FROM users) AS dt", 
         ["subquery", "select", "id", "users"], ["(SELECT id FROM users) AS dt"]),
        ("SELECT * FROM (SELECT * FROM posts WHERE id > 5) AS derived WHERE derived.id < 10",
         ["subquery", "posts"], ["(SELECT * FROM posts"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        missing = [t for t in expected_tokens if t.lower() not in result.lower()]
        found_bad = [t for t in disallowed if t in result]
        
        passed = len(missing) == 0 and len(found_bad) == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql[:60]}...")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if missing:
                print(f"  ERROR: Missing expected: {missing}")
            if found_bad:
                print(f"  ERROR: Found disallowed: {found_bad}")
    
    return all_passed


def test_v2_bug4_redundant_parens():
    """Test that IN subqueries don't have double parentheses."""
    print("\n" + "=" * 60)
    print("V2 BUG 4 TEST: Redundant Parentheses")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM posts WHERE id IN (SELECT post_id FROM comments)", 
         ["is in (select"], ["is in ((SELECT"]),
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM likes)",
         ["is in (select"], ["is in ((SELECT"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        missing = [t for t in expected_tokens if t.lower() not in result.lower()]
        found_bad = [t for t in disallowed if t in result]
        
        passed = len(missing) == 0 and len(found_bad) == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if missing:
                print(f"  ERROR: Missing expected: {missing}")
            if found_bad:
                print(f"  ERROR: Found disallowed (double parens): {found_bad}")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE V2 BUG FIXES TEST SUITE (BUGS 3 & 4)")
    print("#" * 60)
    
    bug3_passed = test_v2_bug3_derived_tables()
    bug4_passed = test_v2_bug4_redundant_parens()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"V2 Bug 3 (Derived Tables): {'✓ ALL TESTS PASSED' if bug3_passed else '✗ SOME TESTS FAILED'}")
    print(f"V2 Bug 4 (Parens):         {'✓ ALL TESTS PASSED' if bug4_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug3_passed and bug4_passed:
        print("\n🎉 All V2 bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
