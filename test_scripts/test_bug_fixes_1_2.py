"""
Test script for SQL-NL Pipeline Bug Fixes 1 & 2.

Bug 1: SELECT * wildcard should render as "all columns" instead of "None"
Bug 2: DATE_SUB intervals should render as "X days ago" instead of just "NOW()"

Run from project root:
    python test_scripts/test_bug_fixes_1_2.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_bug1_select_star():
    """Test that SELECT * renders as 'all columns' instead of 'None'."""
    print("\n" + "=" * 60)
    print("BUG 1 TEST: SELECT * Wildcard")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        "SELECT * FROM users",
        "SELECT * FROM posts WHERE id = 1",
        "SELECT * FROM follows AS f1 WHERE f1.follower_id = 809",
        "SELECT * FROM comments AS c1 ORDER BY c1.created_at",
    ]
    
    all_passed = True
    
    for sql in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check that result does NOT contain "None" and DOES contain "all columns"
        has_none = "None" in result
        has_all_columns = "all columns" in result
        
        passed = has_all_columns and not has_none
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if has_none:
                print("  ERROR: Contains 'None' (bug not fixed)")
            if not has_all_columns:
                print("  ERROR: Missing 'all columns'")
    
    return all_passed


def test_bug2_date_sub_interval():
    """Test that DATE_SUB renders interval information instead of just NOW()."""
    print("\n" + "=" * 60)
    print("BUG 2 TEST: DATE_SUB Temporal Logic")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM likes WHERE liked_at > DATE_SUB(NOW(), INTERVAL 30 DAY)", "30"),
        ("SELECT * FROM posts WHERE posted_at < DATE_SUB(NOW(), INTERVAL 7 DAY)", "7"),
        ("SELECT * FROM users WHERE signup_date >= DATE_SUB(NOW(), INTERVAL 14 DAY)", "14"),
        ("SELECT * FROM comments WHERE created_at <= DATE_SUB(NOW(), INTERVAL 2 DAY)", "2"),
    ]
    
    all_passed = True
    
    for sql, expected_days in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check that result contains the day count and "ago" phrasing
        has_days = expected_days in result
        has_ago = "ago" in result
        has_now_only = result.endswith("NOW().") or "greater than NOW()" in result or "less than NOW()" in result
        
        passed = has_days and has_ago and not has_now_only
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if not has_days:
                print(f"  ERROR: Missing interval value '{expected_days}'")
            if not has_ago:
                print("  ERROR: Missing 'ago' phrasing")
            if has_now_only:
                print("  ERROR: Shows only 'NOW()' without interval (bug not fixed)")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE BUG FIXES TEST SUITE")
    print("#" * 60)
    
    bug1_passed = test_bug1_select_star()
    bug2_passed = test_bug2_date_sub_interval()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Bug 1 (SELECT *):   {'✓ ALL TESTS PASSED' if bug1_passed else '✗ SOME TESTS FAILED'}")
    print(f"Bug 2 (DATE_SUB):   {'✓ ALL TESTS PASSED' if bug2_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug1_passed and bug2_passed:
        print("\n🎉 All bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
