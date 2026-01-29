"""
Test script for SQL-NL Pipeline Bug Fixes 3 & 4.

Bug 3: IN subqueries should render properly instead of truncating
Bug 4: ORDER BY and LIMIT should be included in the output

Run from project root:
    python test_scripts/test_bug_fixes_3_4.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_bug3_in_subquery():
    """Test that IN subqueries render properly instead of truncating."""
    print("\n" + "=" * 60)
    print("BUG 3 TEST: IN Subquery")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM posts WHERE id IN (SELECT post_id FROM comments WHERE user_id = 1)", 
         ["is in", "select", "post_id", "comments"]),
        ("SELECT * FROM comments WHERE post_id IN (SELECT id FROM posts WHERE posted_at > NOW())",
         ["is in", "select", "id", "posts"]),
        ("SELECT * FROM users WHERE id IN (SELECT user_id FROM likes)",
         ["is in", "select", "user_id", "likes"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check that result contains all expected tokens
        missing = [t for t in expected_tokens if t.lower() not in result.lower()]
        truncated = result.strip().endswith(".") and "is in" not in result
        
        passed = len(missing) == 0 and not truncated
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql[:70]}...")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if missing:
                print(f"  ERROR: Missing tokens: {missing}")
            if truncated:
                print("  ERROR: Output appears truncated (bug not fixed)")
    
    return all_passed


def test_bug4_order_by_limit():
    """Test that ORDER BY and LIMIT are rendered."""
    print("\n" + "=" * 60)
    print("BUG 4 TEST: ORDER BY / LIMIT")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM users ORDER BY name", ["ordered by", "name"]),
        ("SELECT * FROM posts LIMIT 10", ["limit", "10"]),
        ("SELECT * FROM users ORDER BY email LIMIT 5", ["ordered by", "email", "limit", "5"]),
        ("SELECT * FROM comments ORDER BY created_at DESC", ["ordered by", "created_at"]),
        ("SELECT id, name FROM users ORDER BY id LIMIT 100", ["ordered by", "id", "limit", "100"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check that result contains all expected tokens
        missing = [t for t in expected_tokens if t.lower() not in result.lower()]
        
        passed = len(missing) == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            print(f"  ERROR: Missing tokens: {missing}")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE BUG FIXES TEST SUITE (BUGS 3 & 4)")
    print("#" * 60)
    
    bug3_passed = test_bug3_in_subquery()
    bug4_passed = test_bug4_order_by_limit()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Bug 3 (IN Subquery):    {'✓ ALL TESTS PASSED' if bug3_passed else '✗ SOME TESTS FAILED'}")
    print(f"Bug 4 (ORDER/LIMIT):    {'✓ ALL TESTS PASSED' if bug4_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug3_passed and bug4_passed:
        print("\n🎉 All bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
