"""
Test script for SQL-NL Pipeline V2 Bug Fixes 1 & 2.

V2 Bug 1: UNION should use natural language connector, not raw SQL keyword
V2 Bug 2: LIMIT should use grammatically correct phrasing

Run from project root:
    python test_scripts/test_bug_fixes_v2_1_2.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_v2_bug1_union_natural_language():
    """Test that UNION uses natural language connector."""
    print("\n" + "=" * 60)
    print("V2 BUG 1 TEST: UNION Natural Language")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        # (SQL, expected_tokens, disallowed_tokens)
        ("SELECT id FROM users UNION ALL SELECT id FROM posts", 
         ["combined with"], ["UNION ALL", "UNION"]),
        ("SELECT id FROM users UNION SELECT id FROM posts", 
         ["combined with", "distinct"], ["UNION ALL"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check expected tokens
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
                print(f"  ERROR: Found disallowed: {found_bad}")
    
    return all_passed


def test_v2_bug2_limit_grammar():
    """Test that LIMIT uses grammatically correct phrasing."""
    print("\n" + "=" * 60)
    print("V2 BUG 2 TEST: LIMIT Grammar")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        ("SELECT * FROM users LIMIT 10", 
         ["limited to", "10", "results"]),
        ("SELECT * FROM posts ORDER BY id LIMIT 79", 
         ["ordered by", "limited to", "79", "results"]),
        ("SELECT id FROM users LIMIT 5", 
         ["limited to", "5", "results"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check expected tokens
        missing = [t for t in expected_tokens if t.lower() not in result.lower()]
        
        passed = len(missing) == 0
        status = "✓ PASS" if passed else "✗ FAIL"
        
        print(f"\n{status}")
        print(f"  SQL: {sql}")
        print(f"  NL:  {result}")
        
        if not passed:
            all_passed = False
            print(f"  ERROR: Missing expected: {missing}")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE V2 BUG FIXES TEST SUITE (BUGS 1 & 2)")
    print("#" * 60)
    
    bug1_passed = test_v2_bug1_union_natural_language()
    bug2_passed = test_v2_bug2_limit_grammar()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"V2 Bug 1 (UNION NL):    {'✓ ALL TESTS PASSED' if bug1_passed else '✗ SOME TESTS FAILED'}")
    print(f"V2 Bug 2 (LIMIT):       {'✓ ALL TESTS PASSED' if bug2_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug1_passed and bug2_passed:
        print("\n🎉 All V2 bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
