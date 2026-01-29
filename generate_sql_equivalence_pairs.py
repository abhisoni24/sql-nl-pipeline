"""
SQL Equivalence Pair Generator.

Generates pairs of semantically equivalent SQL statements that may differ syntactically.
Uses the SocialMediaDB schema defined in src/core/schema.py.

Examples of equivalent transformations:
- Different column ordering: SELECT a, b vs SELECT b, a
- Alias variations: users AS u vs users u
- Join rewriting: JOIN vs INNER JOIN
- Condition reordering: a AND b vs b AND a
- Explicit vs implicit: SELECT * vs SELECT id, name, ...
- BETWEEN vs range: BETWEEN 1 AND 10 vs >= 1 AND <= 10
"""

import json
import random
from typing import List, Dict, Tuple

# Seed for reproducibility
random.seed(42)

# Define equivalence patterns - each is (description, sql1, sql2)
# These are templates using the SocialMediaDB schema

EQUIVALENCE_PATTERNS: List[Dict] = [
    # === COLUMN ORDERING ===
    {
        "type": "column_order",
        "sql1": "SELECT id, username, email FROM users",
        "sql2": "SELECT email, id, username FROM users",
        "description": "Column order in SELECT does not affect semantics (row content same, order different)"
    },
    {
        "type": "column_order",
        "sql1": "SELECT user_id, post_id, liked_at FROM likes",
        "sql2": "SELECT liked_at, post_id, user_id FROM likes",
        "description": "Reordered columns in likes table"
    },
    
    # === JOIN VARIATIONS ===
    {
        "type": "join_syntax",
        "sql1": "SELECT * FROM users JOIN posts ON users.id = posts.user_id",
        "sql2": "SELECT * FROM users INNER JOIN posts ON users.id = posts.user_id",
        "description": "JOIN and INNER JOIN are equivalent"
    },
    {
        "type": "join_syntax", 
        "sql1": "SELECT u.username FROM users u JOIN posts p ON u.id = p.user_id",
        "sql2": "SELECT users.username FROM users INNER JOIN posts ON users.id = posts.user_id",
        "description": "Alias vs full table name with JOIN variation"
    },
    {
        "type": "join_order",
        "sql1": "SELECT * FROM users u JOIN posts p ON u.id = p.user_id JOIN comments c ON p.id = c.post_id",
        "sql2": "SELECT * FROM posts p JOIN users u ON p.user_id = u.id JOIN comments c ON p.id = c.post_id",
        "description": "Join order for inner joins (commutative)"
    },
    
    # === WHERE CONDITION VARIATIONS ===
    {
        "type": "condition_order",
        "sql1": "SELECT * FROM users WHERE is_verified = TRUE AND country_code = 'US'",
        "sql2": "SELECT * FROM users WHERE country_code = 'US' AND is_verified = TRUE",
        "description": "AND conditions are commutative"
    },
    {
        "type": "condition_order",
        "sql1": "SELECT * FROM posts WHERE view_count > 100 AND user_id = 5",
        "sql2": "SELECT * FROM posts WHERE user_id = 5 AND view_count > 100",
        "description": "Reordered AND conditions"
    },
    {
        "type": "comparison_flip",
        "sql1": "SELECT * FROM users WHERE id > 10",
        "sql2": "SELECT * FROM users WHERE 10 < id",
        "description": "Flipped comparison operands"
    },
    {
        "type": "comparison_flip",
        "sql1": "SELECT * FROM posts WHERE user_id = 5",
        "sql2": "SELECT * FROM posts WHERE 5 = user_id",
        "description": "Equality is symmetric"
    },
    
    # === IN vs OR ===
    {
        "type": "in_vs_or",
        "sql1": "SELECT * FROM users WHERE id IN (1, 2, 3)",
        "sql2": "SELECT * FROM users WHERE id = 1 OR id = 2 OR id = 3",
        "description": "IN clause equivalent to OR chain"
    },
    {
        "type": "in_vs_or",
        "sql1": "SELECT * FROM posts WHERE user_id IN (10, 20)",
        "sql2": "SELECT * FROM posts WHERE user_id = 10 OR user_id = 20",
        "description": "IN with two values vs OR"
    },
    
    # === BETWEEN vs RANGE ===
    {
        "type": "between_vs_range",
        "sql1": "SELECT * FROM posts WHERE view_count BETWEEN 100 AND 500",
        "sql2": "SELECT * FROM posts WHERE view_count >= 100 AND view_count <= 500",
        "description": "BETWEEN equivalent to >= AND <="
    },
    {
        "type": "between_vs_range", 
        "sql1": "SELECT * FROM users WHERE id BETWEEN 1 AND 100",
        "sql2": "SELECT * FROM users WHERE id >= 1 AND id <= 100",
        "description": "BETWEEN on id column"
    },
    
    # === ALIAS VARIATIONS ===
    {
        "type": "alias_syntax",
        "sql1": "SELECT * FROM users AS u WHERE u.id = 1",
        "sql2": "SELECT * FROM users u WHERE u.id = 1",
        "description": "AS keyword is optional for aliases"
    },
    {
        "type": "alias_syntax",
        "sql1": "SELECT p.content AS post_content FROM posts AS p",
        "sql2": "SELECT p.content post_content FROM posts p",
        "description": "Both table and column alias without AS"
    },
    
    # === NOT VARIATIONS ===
    {
        "type": "not_equivalent",
        "sql1": "SELECT * FROM users WHERE NOT is_verified = TRUE",
        "sql2": "SELECT * FROM users WHERE is_verified = FALSE",
        "description": "NOT TRUE equals FALSE for boolean"
    },
    {
        "type": "not_equivalent",
        "sql1": "SELECT * FROM posts WHERE NOT view_count > 100",
        "sql2": "SELECT * FROM posts WHERE view_count <= 100",
        "description": "NOT greater than equals less than or equal"
    },
    {
        "type": "not_in_vs_and",
        "sql1": "SELECT * FROM users WHERE id NOT IN (1, 2)",
        "sql2": "SELECT * FROM users WHERE id <> 1 AND id <> 2",
        "description": "NOT IN equivalent to AND of not equals"
    },
    
    # === DISTINCT variations ===
    {
        "type": "distinct",
        "sql1": "SELECT DISTINCT user_id FROM likes",
        "sql2": "SELECT user_id FROM likes GROUP BY user_id",
        "description": "DISTINCT equivalent to GROUP BY without aggregates"
    },
    {
        "type": "distinct",
        "sql1": "SELECT DISTINCT user_id, post_id FROM comments",
        "sql2": "SELECT user_id, post_id FROM comments GROUP BY user_id, post_id",
        "description": "Multi-column DISTINCT vs GROUP BY"
    },
    
    # === EXISTS vs IN with subquery ===
    {
        "type": "exists_vs_in",
        "sql1": "SELECT * FROM users WHERE id IN (SELECT user_id FROM posts)",
        "sql2": "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id)",
        "description": "IN subquery equivalent to correlated EXISTS"
    },
    
    # === COUNT variations ===
    {
        "type": "count_star_vs_column",
        "sql1": "SELECT COUNT(*) FROM users",
        "sql2": "SELECT COUNT(1) FROM users",
        "description": "COUNT(*) and COUNT(1) are equivalent"
    },
    
    # === UNION vs UNION ALL with DISTINCT ===
    {
        "type": "union_distinct",
        "sql1": "SELECT id FROM users UNION SELECT user_id FROM posts",
        "sql2": "SELECT DISTINCT id FROM (SELECT id FROM users UNION ALL SELECT user_id FROM posts) t",
        "description": "UNION equals UNION ALL wrapped in DISTINCT"
    },
    
    # === Table prefix variations ===
    {
        "type": "table_prefix",
        "sql1": "SELECT users.id, users.username FROM users WHERE users.is_verified = TRUE",
        "sql2": "SELECT id, username FROM users WHERE is_verified = TRUE",
        "description": "Table prefix optional when unambiguous"
    },
    
    # === LIKE variations ===
    {
        "type": "like_equivalent",
        "sql1": "SELECT * FROM users WHERE username LIKE 'john'",
        "sql2": "SELECT * FROM users WHERE username = 'john'",
        "description": "LIKE without wildcards equals basic equality"
    },
    
    # === NULL checks ===
    {
        "type": "null_check",
        "sql1": "SELECT * FROM users WHERE email IS NOT NULL",
        "sql2": "SELECT * FROM users WHERE NOT email IS NULL",
        "description": "IS NOT NULL vs NOT ... IS NULL"
    },
    
    # === Subquery vs JOIN ===
    {
        "type": "subquery_vs_join",
        "sql1": "SELECT * FROM posts WHERE user_id IN (SELECT id FROM users WHERE is_verified = TRUE)",
        "sql2": "SELECT posts.* FROM posts JOIN users ON posts.user_id = users.id WHERE users.is_verified = TRUE",
        "description": "IN subquery equivalent to JOIN for this pattern"
    },
    
    # === ORDER BY with different expressions ===
    {
        "type": "order_expression",
        "sql1": "SELECT * FROM posts ORDER BY view_count DESC",
        "sql2": "SELECT * FROM posts ORDER BY -view_count ASC",
        "description": "Negated column with ASC equals DESC (for numeric)"
    },
    
    # === CASE equivalents ===
    {
        "type": "case_vs_if",
        "sql1": "SELECT id, CASE WHEN is_verified THEN 'yes' ELSE 'no' END status FROM users",
        "sql2": "SELECT id, IF(is_verified, 'yes', 'no') status FROM users",
        "description": "Simple CASE WHEN equivalent to IF in MySQL"
    },
    
    # === COALESCE / IFNULL ===
    {
        "type": "coalesce_vs_ifnull",
        "sql1": "SELECT COALESCE(email, 'unknown') FROM users",
        "sql2": "SELECT IFNULL(email, 'unknown') FROM users",
        "description": "COALESCE with two args equals IFNULL in MySQL"
    },
    
    # ==========================================================================
    # DML STATEMENTS: INSERT, UPDATE, DELETE
    # ==========================================================================
    
    # === INSERT VARIATIONS ===
    {
        "type": "insert_column_order",
        "sql1": "INSERT INTO users (id, username, email) VALUES (1, 'john', 'john@example.com')",
        "sql2": "INSERT INTO users (email, id, username) VALUES ('john@example.com', 1, 'john')",
        "description": "Column order in INSERT with matching value order"
    },
    {
        "type": "insert_column_order",
        "sql1": "INSERT INTO posts (id, user_id, content) VALUES (10, 5, 'Hello')",
        "sql2": "INSERT INTO posts (content, id, user_id) VALUES ('Hello', 10, 5)",
        "description": "Reordered columns in INSERT for posts"
    },
    {
        "type": "insert_column_order",
        "sql1": "INSERT INTO comments (id, user_id, post_id, comment_text) VALUES (1, 2, 3, 'Nice!')",
        "sql2": "INSERT INTO comments (post_id, comment_text, id, user_id) VALUES (3, 'Nice!', 1, 2)",
        "description": "Reordered columns in INSERT for comments"
    },
    {
        "type": "insert_values_vs_set",
        "sql1": "INSERT INTO users (id, username) VALUES (1, 'john')",
        "sql2": "INSERT INTO users (username, id) VALUES ('john', 1)",
        "description": "Reordered columns and values in INSERT"
    },
    {
        "type": "insert_multi_row",
        "sql1": "INSERT INTO posts (id, user_id) VALUES (5, 10)",
        "sql2": "INSERT INTO posts (user_id, id) VALUES (10, 5)",
        "description": "Column/value reordering in posts INSERT"
    },
    {
        "type": "insert_with_defaults",
        "sql1": "INSERT INTO users (id, username, is_verified) VALUES (1, 'john', FALSE)",
        "sql2": "INSERT INTO users (is_verified, id, username) VALUES (FALSE, 1, 'john')",
        "description": "Reordered INSERT including boolean column"
    },
    
    # === UPDATE VARIATIONS ===
    {
        "type": "update_set_order",
        "sql1": "UPDATE users SET username = 'newname', email = 'new@email.com' WHERE id = 1",
        "sql2": "UPDATE users SET email = 'new@email.com', username = 'newname' WHERE id = 1",
        "description": "SET clause order does not affect UPDATE semantics"
    },
    {
        "type": "update_set_order",
        "sql1": "UPDATE posts SET content = 'Updated', view_count = 100 WHERE id = 5",
        "sql2": "UPDATE posts SET view_count = 100, content = 'Updated' WHERE id = 5",
        "description": "Reordered SET assignments in UPDATE"
    },
    {
        "type": "update_condition_order",
        "sql1": "UPDATE users SET is_verified = TRUE WHERE id = 1 AND country_code = 'US'",
        "sql2": "UPDATE users SET is_verified = TRUE WHERE country_code = 'US' AND id = 1",
        "description": "AND conditions are commutative in UPDATE WHERE"
    },
    {
        "type": "update_condition_order",
        "sql1": "UPDATE posts SET view_count = 0 WHERE user_id = 5 AND view_count > 100",
        "sql2": "UPDATE posts SET view_count = 0 WHERE view_count > 100 AND user_id = 5",
        "description": "Reordered conditions in UPDATE"
    },
    {
        "type": "update_comparison_flip",
        "sql1": "UPDATE users SET is_verified = TRUE WHERE id = 10",
        "sql2": "UPDATE users SET is_verified = TRUE WHERE 10 = id",
        "description": "Flipped equality in UPDATE WHERE"
    },
    {
        "type": "update_in_vs_or",
        "sql1": "UPDATE users SET is_verified = TRUE WHERE id IN (1, 2, 3)",
        "sql2": "UPDATE users SET is_verified = TRUE WHERE id = 1 OR id = 2 OR id = 3",
        "description": "IN clause vs OR chain in UPDATE"
    },
    {
        "type": "update_between_vs_range",
        "sql1": "UPDATE posts SET view_count = 0 WHERE id BETWEEN 10 AND 20",
        "sql2": "UPDATE posts SET view_count = 0 WHERE id >= 10 AND id <= 20",
        "description": "BETWEEN vs range in UPDATE"
    },
    {
        "type": "update_table_alias",
        "sql1": "UPDATE users AS u SET u.is_verified = TRUE WHERE u.id = 1",
        "sql2": "UPDATE users u SET u.is_verified = TRUE WHERE u.id = 1",
        "description": "AS keyword optional in UPDATE alias"
    },
    
    # === DELETE VARIATIONS ===
    {
        "type": "delete_condition_order",
        "sql1": "DELETE FROM users WHERE id = 1 AND is_verified = FALSE",
        "sql2": "DELETE FROM users WHERE is_verified = FALSE AND id = 1",
        "description": "AND conditions are commutative in DELETE WHERE"
    },
    {
        "type": "delete_condition_order",
        "sql1": "DELETE FROM posts WHERE user_id = 5 AND view_count < 10",
        "sql2": "DELETE FROM posts WHERE view_count < 10 AND user_id = 5",
        "description": "Reordered conditions in DELETE"
    },
    {
        "type": "delete_comparison_flip",
        "sql1": "DELETE FROM users WHERE id = 10",
        "sql2": "DELETE FROM users WHERE 10 = id",
        "description": "Flipped equality in DELETE WHERE"
    },
    {
        "type": "delete_comparison_flip",
        "sql1": "DELETE FROM posts WHERE user_id = 5",
        "sql2": "DELETE FROM posts WHERE 5 = user_id",
        "description": "Symmetric equality in DELETE"
    },
    {
        "type": "delete_in_vs_or",
        "sql1": "DELETE FROM users WHERE id IN (1, 2, 3)",
        "sql2": "DELETE FROM users WHERE id = 1 OR id = 2 OR id = 3",
        "description": "IN clause vs OR chain in DELETE"
    },
    {
        "type": "delete_in_vs_or",
        "sql1": "DELETE FROM comments WHERE post_id IN (10, 20)",
        "sql2": "DELETE FROM comments WHERE post_id = 10 OR post_id = 20",
        "description": "IN clause vs OR in DELETE for comments"
    },
    {
        "type": "delete_between_vs_range",
        "sql1": "DELETE FROM posts WHERE id BETWEEN 100 AND 200",
        "sql2": "DELETE FROM posts WHERE id >= 100 AND id <= 200",
        "description": "BETWEEN vs range in DELETE"
    },
    {
        "type": "delete_not_equivalent",
        "sql1": "DELETE FROM users WHERE NOT is_verified = TRUE",
        "sql2": "DELETE FROM users WHERE is_verified = FALSE",
        "description": "NOT TRUE equals FALSE in DELETE WHERE"
    },
    {
        "type": "delete_subquery_vs_join",
        "sql1": "DELETE FROM posts WHERE user_id IN (SELECT id FROM users WHERE is_verified = FALSE)",
        "sql2": "DELETE posts FROM posts JOIN users ON posts.user_id = users.id WHERE users.is_verified = FALSE",
        "description": "DELETE with subquery vs DELETE with JOIN (MySQL syntax)"
    },
    {
        "type": "delete_table_alias",
        "sql1": "DELETE FROM users AS u WHERE u.id = 1",
        "sql2": "DELETE FROM users u WHERE u.id = 1",
        "description": "AS keyword optional in DELETE alias"
    },
]

def generate_variations() -> List[Dict]:
    """
    Generate 100 SQL equivalence pairs by using base patterns and creating variations.
    """
    pairs = []
    pair_id = 1
    
    # First add all base patterns
    for pattern in EQUIVALENCE_PATTERNS:
        pairs.append({
            "id": pair_id,
            "type": pattern["type"],
            "sql1": pattern["sql1"],
            "sql2": pattern["sql2"],
            "description": pattern["description"],
            "should_be_equivalent": True
        })
        pair_id += 1
    
    # Now generate variations to reach 100
    while len(pairs) < 100:
        base = random.choice(EQUIVALENCE_PATTERNS)
        
        # Create variations by modifying values
        sql1, sql2 = base["sql1"], base["sql2"]
        
        # Simple value substitutions
        if "WHERE" in sql1:
            # Change numeric values
            old_num = str(random.randint(1, 10))
            new_num = str(random.randint(11, 1000))
            sql1 = sql1.replace(f"= {old_num}", f"= {new_num}").replace(f"> {old_num}", f"> {new_num}")
            sql2 = sql2.replace(f"= {old_num}", f"= {new_num}").replace(f"> {old_num}", f"> {new_num}")
        
        # Change table references in some patterns
        if base["type"] == "column_order":
            tables = ["users", "posts", "comments", "likes", "follows"]
            cols_map = {
                "users": ["id", "username", "email"],
                "posts": ["id", "user_id", "content"],
                "comments": ["id", "user_id", "post_id"],
                "likes": ["user_id", "post_id", "liked_at"],
                "follows": ["follower_id", "followee_id", "followed_at"]
            }
            table = random.choice(tables)
            cols = cols_map[table]
            random.shuffle(cols)
            cols1 = ", ".join(cols)
            random.shuffle(cols)
            cols2 = ", ".join(cols)
            sql1 = f"SELECT {cols1} FROM {table}"
            sql2 = f"SELECT {cols2} FROM {table}"
        
        pairs.append({
            "id": pair_id,
            "type": base["type"] + "_variation",
            "sql1": sql1,
            "sql2": sql2,
            "description": f"Variation of: {base['description']}",
            "should_be_equivalent": True
        })
        pair_id += 1
    
    # Add a few NON-equivalent pairs for testing negative cases
    non_equiv = [
        # SELECT non-equivalent
        {
            "sql1": "SELECT * FROM users WHERE id = 1",
            "sql2": "SELECT * FROM users WHERE id = 2",
            "description": "Different literal values - NOT equivalent"
        },
        {
            "sql1": "SELECT * FROM users WHERE id > 10",
            "sql2": "SELECT * FROM users WHERE id < 10",
            "description": "Different comparison operators - NOT equivalent"
        },
        {
            "sql1": "SELECT id FROM users",
            "sql2": "SELECT username FROM users",
            "description": "Different columns selected - NOT equivalent"
        },
        {
            "sql1": "SELECT * FROM users",
            "sql2": "SELECT * FROM posts",
            "description": "Different tables - NOT equivalent"
        },
        {
            "sql1": "SELECT * FROM users ORDER BY id ASC",
            "sql2": "SELECT * FROM users ORDER BY id DESC",
            "description": "Different sort order - NOT equivalent"
        },
        # INSERT non-equivalent
        {
            "sql1": "INSERT INTO users (id, username) VALUES (1, 'john')",
            "sql2": "INSERT INTO users (id, username) VALUES (2, 'john')",
            "description": "Different id value in INSERT - NOT equivalent"
        },
        {
            "sql1": "INSERT INTO users (id, username) VALUES (1, 'john')",
            "sql2": "INSERT INTO users (id, username) VALUES (1, 'jane')",
            "description": "Different username value in INSERT - NOT equivalent"
        },
        {
            "sql1": "INSERT INTO users (id) VALUES (1)",
            "sql2": "INSERT INTO posts (id) VALUES (1)",
            "description": "Different tables in INSERT - NOT equivalent"
        },
        # UPDATE non-equivalent
        {
            "sql1": "UPDATE users SET username = 'john' WHERE id = 1",
            "sql2": "UPDATE users SET username = 'jane' WHERE id = 1",
            "description": "Different SET value in UPDATE - NOT equivalent"
        },
        {
            "sql1": "UPDATE users SET username = 'john' WHERE id = 1",
            "sql2": "UPDATE users SET username = 'john' WHERE id = 2",
            "description": "Different WHERE condition in UPDATE - NOT equivalent"
        },
        {
            "sql1": "UPDATE users SET username = 'john' WHERE id = 1",
            "sql2": "UPDATE posts SET content = 'john' WHERE id = 1",
            "description": "Different tables in UPDATE - NOT equivalent"
        },
        # DELETE non-equivalent
        {
            "sql1": "DELETE FROM users WHERE id = 1",
            "sql2": "DELETE FROM users WHERE id = 2",
            "description": "Different id in DELETE - NOT equivalent"
        },
        {
            "sql1": "DELETE FROM users WHERE id = 1",
            "sql2": "DELETE FROM posts WHERE id = 1",
            "description": "Different tables in DELETE - NOT equivalent"
        },
        {
            "sql1": "DELETE FROM users WHERE id > 10",
            "sql2": "DELETE FROM users WHERE id < 10",
            "description": "Different operator in DELETE - NOT equivalent"
        },
    ]
    
    for ne in non_equiv:
        pairs.append({
            "id": pair_id,
            "type": "non_equivalent",
            "sql1": ne["sql1"],
            "sql2": ne["sql2"],
            "description": ne["description"],
            "should_be_equivalent": False
        })
        pair_id += 1
    
    return pairs  # Return all pairs (no truncation)


def main():
    pairs = generate_variations()
    
    output_file = "./dataset/current/sql_equivalence_pairs.json"
    with open(output_file, "w") as f:
        json.dump(pairs, f, indent=2)
    
    equiv_count = sum(1 for p in pairs if p["should_be_equivalent"])
    non_equiv_count = len(pairs) - equiv_count
    
    print(f"Generated {len(pairs)} SQL equivalence pairs:")
    print(f"  - Equivalent pairs: {equiv_count}")
    print(f"  - Non-equivalent pairs: {non_equiv_count}")
    print(f"Saved to: {output_file}")
    
    # Print sample
    print("\nSample pairs:")
    for p in pairs[:3]:
        print(f"\n[{p['id']}] {p['type']}: {p['description']}")
        print(f"  SQL1: {p['sql1']}")
        print(f"  SQL2: {p['sql2']}")


if __name__ == "__main__":
    main()
