"""
Test script for SQL-NL Pipeline Bug Fixes 7 & 8.

Bug 7: UNION/EXISTS queries should render properly instead of passthrough
Bug 8: INSERT/UPDATE/DELETE should render to NL instead of errors

Run from project root:
    python test_scripts/test_bug_fixes_7_8.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlglot import parse_one
from src.core.nl_renderer import SQLToNLRenderer
from src.core.schema import USED_SQL_DIALECT


def test_bug7_union_exists():
    """Test that UNION and EXISTS queries render properly."""
    print("\n" + "=" * 60)
    print("BUG 7 TEST: UNION / EXISTS")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        # (SQL, expected_tokens, disallowed_tokens)
        ("SELECT id FROM users UNION ALL SELECT id FROM posts", 
         ["UNION ALL"], ["Execute statement"]),
        ("SELECT id FROM users UNION SELECT id FROM posts", 
         ["UNION"], ["Execute statement"]),
        ("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)",
         ["NOT", "exists"], ["Execute statement"]),
        ("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)",
         ["exists"], ["Execute statement"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check expected tokens present (case insensitive for some)
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
                print(f"  ERROR: Missing expected tokens: {missing}")
            if found_bad:
                print(f"  ERROR: Found disallowed tokens: {found_bad}")
    
    return all_passed


def test_bug8_dml():
    """Test that INSERT/UPDATE/DELETE render properly."""
    print("\n" + "=" * 60)
    print("BUG 8 TEST: DML Statements")
    print("=" * 60)
    
    renderer = SQLToNLRenderer()
    
    test_cases = [
        # (SQL, expected_tokens, disallowed_tokens)
        ("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')", 
         ["Insert", "users", "name", "email"], ["Error"]),
        ("UPDATE users SET name = 'Jane' WHERE id = 1", 
         ["Update", "users", "set", "name"], ["Error"]),
        ("DELETE FROM users WHERE id = 1", 
         ["Delete", "users", "where"], ["Error"]),
        ("INSERT INTO posts (user_id, content) VALUES (1, 'Hello')",
         ["Insert", "posts", "user_id", "content"], ["Error"]),
        ("UPDATE posts SET view_count = 100 WHERE id = 5",
         ["Update", "posts", "view_count"], ["Error"]),
        ("DELETE FROM comments WHERE post_id = 10",
         ["Delete", "comments", "where"], ["Error"]),
    ]
    
    all_passed = True
    
    for sql, expected_tokens, disallowed in test_cases:
        ast = parse_one(sql, dialect=USED_SQL_DIALECT)
        result = renderer.render(ast)
        
        # Check expected tokens present
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
                print(f"  ERROR: Missing expected tokens: {missing}")
            if found_bad:
                print(f"  ERROR: Found disallowed tokens: {found_bad}")
    
    return all_passed


def main():
    print("\n" + "#" * 60)
    print("# SQL-NL PIPELINE BUG FIXES TEST SUITE (BUGS 7 & 8)")
    print("#" * 60)
    
    bug7_passed = test_bug7_union_exists()
    bug8_passed = test_bug8_dml()
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Bug 7 (UNION/EXISTS):   {'✓ ALL TESTS PASSED' if bug7_passed else '✗ SOME TESTS FAILED'}")
    print(f"Bug 8 (DML):            {'✓ ALL TESTS PASSED' if bug8_passed else '✗ SOME TESTS FAILED'}")
    print("=" * 60)
    
    if bug7_passed and bug8_passed:
        print("\n🎉 All bug fixes verified successfully!\n")
        return 0
    else:
        print("\n⚠️  Some tests failed. Review the output above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
