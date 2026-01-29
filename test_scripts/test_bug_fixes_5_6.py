"""
Test script for SQL-NL Pipeline Bug Fixes 5 & 6.

Bug 5: Join types (LEFT, RIGHT, INNER) should be preserved instead of generic "JOIN"
Bug 6: String literals should be quoted, Booleans should be TRUE/FALSE

Run from project root:
    python test_scripts/test_bug_fixes_5_6.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_bug5_join_types():
    """Test that join types are preserved (LEFT, RIGHT, INNER)."""
    print("\n" + "=" * 60)
    print("BUG 5 TEST: Join Type Preservation")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM users u LEFT JOIN posts p ON u.id = p.user_id", "LEFT JOIN"),
        ("SELECT * FROM users u RIGHT JOIN posts p ON u.id = p.user_id", "RIGHT JOIN"),
        ("SELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id", "INNER JOIN"),
        ("SELECT * FROM users u JOIN posts p ON u.id = p.user_id", "JOIN"),  # Default join
    ]
    
    all_passed = True
    
    for sql, expected_join_type in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check that result contains the expected join type
        has_join_type = expected_join_type in result
        
        passed = has_join_type
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql[:60]}...")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            print(f"  ERROR: Expected '{expected_join_type}' not found in output")
    
    return all_passed


def test_bug6_literal_formatting():
    """Test that string literals are quoted and Booleans are TRUE/FALSE."""
    print("\n" + "=" * 60)
    print("BUG 6 TEST: Literal Formatting")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        # (SQL, expected_tokens, disallowed_tokens)
        ("SELECT * FROM users WHERE email = 'test@example.com'", 
         ["'test@example.com'"], ["eq test@example.com."]),
        ("SELECT * FROM users WHERE is_verified = FALSE",
         ["FALSE"], ["False"]),
        ("SELECT * FROM users WHERE is_verified = TRUE",
         ["TRUE"], ["True"]),
        ("SELECT * FROM posts WHERE content LIKE '%hello%'",
         ["'%hello%'"], ["like %hello%."]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check expected tokens present
        missing = [t for t in expected_tokens if t not in result]
        # Check disallowed tokens absent (case-sensitive for Python bool vs SQL bool)
        found_bad = [t for t in disallowed if t in result]
        
        passed = len(missing) == 0 and len(found_bad) == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            if missing:
                print(f"  ERROR: Missing expected tokens: {missing}")
            if found_bad:
                print(f"  ERROR: Found disallowed tokens: {found_bad}")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE BUG FIXES TEST SUITE (BUGS 5 & 6)")
    print("#" * 60)
    
    bug5_passed = test_bug5_join_types()
    bug6_passed = test_bug6_literal_formatting()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Bug 5 (Join Types):     {'✓ ALL TESTS PASSED' if bug5_passed else '✗ SOME TESTS FAILED'}")
    print(f"Bug 6 (Literals):       {'✓ ALL TESTS PASSED' if bug6_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug5_passed and bug6_passed:
        print("\n🎉 All bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
