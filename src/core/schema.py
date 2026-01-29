# Schema definition for SocialMediaDB

SCHEMA = {
    "users": {
        "id": "int",
        "username": "varchar",
        "email": "varchar",
        "signup_date": "datetime",
        "is_verified": "boolean",
        "country_code": "varchar"
    },
    "posts": {
        "id": "int",
        "user_id": "int",
        "content": "text",
        "posted_at": "datetime",
        "view_count": "int"
    },
    "comments": {
        "id": "int",
        "user_id": "int",
        "post_id": "int",
        "comment_text": "text",
        "created_at": "datetime"
    },
    "likes": {
        "user_id": "int",
        "post_id": "int",
        "liked_at": "datetime"
    },
    "follows": {
        "follower_id": "int",
        "followee_id": "int",
        "followed_at": "datetime"
    }
}

USED_SQL_DIALECT = "sqlite"

# Define valid join paths (left_table, right_table): (left_key, right_key)
FOREIGN_KEYS = {
    ("users", "posts"): ("id", "user_id"),
    ("posts", "users"): ("user_id", "id"),  # Reverse join
    ("users", "comments"): ("id", "user_id"),
    ("comments", "users"): ("user_id", "id"),
    ("posts", "comments"): ("id", "post_id"),
    ("comments", "posts"): ("post_id", "id"),
    ("users", "likes"): ("id", "user_id"),
    ("likes", "users"): ("user_id", "id"),
    ("posts", "likes"): ("id", "post_id"),
    ("likes", "posts"): ("post_id", "id"),
    ("users", "follows"): ("id", "follower_id"),  # Who is following
    ("follows", "users"): ("follower_id", "id"),
}

# Column type categories for smart filtering
NUMERIC_TYPES = {"int"}
TEXT_TYPES = {"varchar", "text"}
DATE_TYPES = {"datetime"}
BOOLEAN_TYPES = {"boolean"}
