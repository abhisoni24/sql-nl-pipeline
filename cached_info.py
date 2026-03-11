# ============================================================================
# Perturbation Type Definitions
# ============================================================================
#
# This file is loaded as raw text by 04_generate_llm_nl_and_perturbations.py
# and injected into the LLM system prompt.  It defines each perturbation type
# that the LLM should generate when creating NL perturbations.
#
# Schema, foreign-key, and column-type blocks (if present) are stripped at
# load time and replaced with information from the YAML schema file.
# ============================================================================

perturbation_types = {

    # ── 1. Keyboard Typos ────────────────────────────────────────────
    "typos": {
        "name": "typos",
        "display_name": "Keyboard Typos",
        "description": "Introduce realistic keyboard typos in the NL prompt.",
        "layer": "post_processing",
        "instructions": (
            "Swap adjacent characters in ~15% of words (minimum 1, maximum 3 "
            "words affected).  Target words with 3+ characters.  The resulting "
            "text should still be vaguely readable but contain plausible "
            "keyboard-proximity errors."
        ),
        "example_before": "Show all users who registered after January",
        "example_after":  "Shwo all uesrs who registered afetr January",
    },

    # ── 2. Verbosity Variation ───────────────────────────────────────
    "verbosity_variation": {
        "name": "verbosity_variation",
        "display_name": "Verbosity Variation",
        "description": "Insert conversational fillers and informal suffixes.",
        "layer": "post_processing",
        "instructions": (
            "Prepend a conversational filler word and append an informal "
            "trailing phrase.  The result should sound like a casual spoken "
            "request from a colleague.  Do NOT change the core meaning."
        ),
        "filler_bank": ["Um", "Uh", "Well", "Okay", "So", "Alright"],
        "informal_bank": [
            "you know", "like", "or something", "or whatever",
            "wanna", "gotta", "a bunch of",
        ],
        "example_before": "Get all orders above 500 dollars.",
        "example_after":  "So get all orders above 500 dollars or something.",
    },

    # ── 3. Urgency Qualifiers ────────────────────────────────────────
    "urgency_qualifiers": {
        "name": "urgency_qualifiers",
        "display_name": "Urgency Qualifiers",
        "description": "Add urgency markers like 'ASAP', 'immediately', etc.",
        "layer": "post_processing",
        "instructions": (
            "Prepend a high- or low-urgency qualifier prefix.  Choose "
            "randomly between high-urgency and low-urgency.  Do NOT modify "
            "the body of the request."
        ),
        "high_urgency": [
            "URGENT:", "ASAP:", "Immediately:", "Critical:", "High priority:",
        ],
        "low_urgency": [
            "When you can,", "No rush,", "At your convenience,",
            "Low priority:",
        ],
        "example_before": "Show me the top 10 customers by revenue.",
        "example_after":  "URGENT: Show me the top 10 customers by revenue.",
    },

    # ── 4. Punctuation Variation ─────────────────────────────────────
    "punctuation_variation": {
        "name": "punctuation_variation",
        "display_name": "Punctuation Variation",
        "description": "Modified sentence rhythm via punctuation changes.",
        "layer": "post_processing",
        "instructions": (
            "Alter punctuation without changing words.  Options: replace the "
            "first comma with a semicolon, replace the trailing period with "
            "an ellipsis ('...'), or replace it with an exclamation mark. "
            "Pick only one transformation."
        ),
        "example_before": "Get orders for customer 42, sorted by date.",
        "example_after":  "Get orders for customer 42; sorted by date.",
    },

    # ── 5. Comment Annotations ───────────────────────────────────────
    "comment_annotations": {
        "name": "comment_annotations",
        "display_name": "Comment Annotations",
        "description": "Added SQL comments/notes to the natural language.",
        "layer": "post_processing",
        "instructions": (
            "Append a short SQL-style comment or parenthetical note at the "
            "end of the NL prompt.  Use formats like '-- for the audit' or "
            "'(note: for analysis)'.  The annotation should look like "
            "something a developer might leave as a reminder."
        ),
        "annotation_bank": [
            "-- for the audit",
            "-- note for later",
            "-- urgent request",
            "(note: for analysis)",
            "-- needed for the report",
            "(specifically for this check)",
            "(referencing recent data)",
        ],
        "example_before": "List all inactive accounts.",
        "example_after":  "List all inactive accounts. -- for the audit",
    },

    # ── 6. Mixed SQL/NL ─────────────────────────────────────────────
    "mixed_sql_nl": {
        "name": "mixed_sql_nl",
        "display_name": "Mixed SQL/NL",
        "description": "Blended raw SQL keywords into natural language.",
        "layer": "template",
        "instructions": (
            "Replace some natural language connectives with their raw SQL "
            "keyword equivalents.  For example use 'SELECT' instead of "
            "'Show', 'WHERE' instead of 'where', 'JOIN' instead of "
            "'combined with', etc.  The result should be a hybrid of SQL "
            "syntax and English that a SQL-savvy developer might type."
        ),
        "sql_keywords": [
            "SELECT", "FROM", "WHERE", "JOIN", "ON", "GROUP BY",
            "ORDER BY", "HAVING", "LIMIT", "INSERT", "UPDATE", "DELETE",
            "SET", "VALUES", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE",
            "IS NULL", "IS NOT NULL", "ASC", "DESC", "DISTINCT",
            "COUNT", "SUM", "AVG", "MIN", "MAX",
            "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN",
        ],
        "not_applicable_when": "Query is an INSERT statement.",
        "example_before": "Show the name and email from users where active is true.",
        "example_after":  "SELECT the name and email FROM users WHERE active is true.",
    },

    # ── 7. Omit Obvious Operation Markers ────────────────────────────
    "omit_obvious_operation_markers": {
        "name": "omit_obvious_operation_markers",
        "display_name": "Omit Obvious Clauses",
        "description": "Removed explicit SQL clause keywords like FROM, WHERE.",
        "layer": "template",
        "instructions": (
            "Remove structural clause markers that a human would consider "
            "obvious from context.  Specifically drop explicit 'from' and "
            "'where' connectives so the request reads more telegraphically. "
            "For example 'Show name from users where active' becomes "
            "'Show name users active'.  Keep enough context so intent is "
            "still recoverable."
        ),
        "omitted_keywords": ["FROM", "WHERE", "ALIAS"],
        "not_applicable_when": "Query is an INSERT statement.",
        "example_before": "Get the name from employees where salary > 50000.",
        "example_after":  "Get the name employees salary > 50000.",
    },

    # ── 8. Incomplete Join Spec ──────────────────────────────────────
    "incomplete_join_spec": {
        "name": "incomplete_join_spec",
        "display_name": "Incomplete Join Spec",
        "description": "Omitted explicit JOIN/ON syntax, assuming the relationship is obvious.",
        "layer": "template",
        "instructions": (
            "Remove or simplify explicit JOIN ... ON ... phrasing.  If a "
            "foreign key relationship exists between the tables, replace the "
            "join clause with a possessive like 'and their <table>'.  "
            "Otherwise use a vague connector like 'with <table>'.  The user "
            "assumes the system knows how to join the tables."
        ),
        "not_applicable_when": "Query does not contain any JOIN.",
        "example_before": "Show orders joined with customers on orders.customer_id = customers.id.",
        "example_after":  "Show orders and their customers.",
    },

    # ── 9. Phrasal & Idiomatic Action Substitution ───────────────────
    "phrasal_and_idiomatic_action_substitution": {
        "name": "phrasal_and_idiomatic_action_substitution",
        "display_name": "Synonym Substitution",
        "description": "Replaced query action verbs with phrasal/idiomatic synonyms.",
        "layer": "dictionary",
        "instructions": (
            "Replace the leading action verb (e.g. 'Get', 'Show', 'Select') "
            "with a different synonym from the same family.  The replacement "
            "must change the first word of the prompt.  Choose a synonym "
            "that sounds natural but is clearly different from the original."
        ),
        "verb_families": {
            "get": [
                "Get", "Retrieve", "Find", "Pull up", "Dig out",
                "Go get", "Fetch me",
            ],
            "select": [
                "Select", "Pick out", "Spot", "Single out", "Choose",
            ],
            "show": [
                "Show", "Display", "Bring up", "Give me a look at",
                "Run a check for", "Produce a listing of",
            ],
        },
        "not_applicable_when": "Query is INSERT, UPDATE, or DELETE.",
        "example_before": "Get all users with more than 5 posts.",
        "example_after":  "Dig out all users with more than 5 posts.",
    },

    # ── 10. Operator/Aggregate Variation ─────────────────────────────
    "operator_aggregate_variation": {
        "name": "operator_aggregate_variation",
        "display_name": "Operator/Aggregate Variation",
        "description": "Varied operator and aggregate function phrasing.",
        "layer": "dictionary",
        "instructions": (
            "Replace comparison operators and aggregate function names with "
            "alternative natural language phrasings.  For example 'greater "
            "than' can become 'exceeds' or 'above'; 'COUNT' can become "
            "'total number of' or 'how many'.  Only applicable when the "
            "query uses >, <, >=, <= operators or aggregate functions."
        ),
        "operator_variations": {
            "gt":  ["exceeds", "more than", "above", "higher than"],
            "lt":  ["below", "under", "fewer than", "lower than"],
            "gte": ["at least", "minimum of", "no less than"],
            "lte": ["at most", "maximum of", "no more than"],
        },
        "aggregate_variations": {
            "COUNT": ["total number of", "how many", "count of", "number of"],
            "SUM":   ["total", "sum of", "add up"],
            "AVG":   ["average", "mean", "typical"],
            "MAX":   ["maximum", "highest", "largest"],
            "MIN":   ["minimum", "lowest", "smallest"],
        },
        "not_applicable_when": (
            "Query has no >, <, >=, <= comparisons and no aggregate functions."
        ),
        "example_before": "Show users where age > 30.",
        "example_after":  "Show users where age exceeds 30.",
    },

    # ── 11. Anchored Pronoun References ──────────────────────────────
    "anchored_pronoun_references": {
        "name": "anchored_pronoun_references",
        "display_name": "Ambiguous Pronouns",
        "description": "Replaced one table/column reference with a pronoun (it/that/this).",
        "layer": "dictionary",
        "instructions": (
            "Replace a repeated table or column reference with a pronoun or "
            "demonstrative phrase such as 'it', 'that value', 'this field', "
            "'the same', 'aforementioned', etc.  Only applicable when the "
            "query references at least 2 distinct (non-alias) tables.  "
            "The pronoun should be structurally plausible but potentially "
            "ambiguous — that is the point of the perturbation."
        ),
        "pronoun_anchors": [
            "that value", "this value", "that field", "it", "the same",
            "aforementioned", "this field", "said", "this column",
            "this attribute", "that column", "the aforementioned",
        ],
        "not_applicable_when": (
            "Query involves fewer than 2 distinct non-alias tables, or is a "
            "self-join."
        ),
        "example_before": "Show orders where orders.amount > 100.",
        "example_after":  "Show orders where that value > 100.",
    },

    # ── 12. Table/Column Synonyms ────────────────────────────────────
    "table_column_synonyms": {
        "name": "table_column_synonyms",
        "display_name": "Table/Column Synonyms",
        "description": "Used human-centric schema synonyms for tables and columns.",
        "layer": "dictionary",
        "instructions": (
            "Replace table and column names with human-friendly synonyms "
            "defined in the schema's synonym dictionary.  For example "
            "'users' might become 'members' or 'accounts'; 'created_at' "
            "might become 'registration date'.  Only applicable when the "
            "schema provides synonyms for at least one referenced table or "
            "column."
        ),
        "not_applicable_when": (
            "No schema synonyms are defined for any table or column in the "
            "query."
        ),
        "example_before": "Get all users where created_at > '2024-01-01'.",
        "example_after":  "Get all members where registration date > '2024-01-01'.",
    },

    # ── 13. Temporal Expression Variation ────────────────────────────
    "temporal_expression_variation": {
        "name": "temporal_expression_variation",
        "display_name": "Temporal Expression Variation",
        "description": "Used relative temporal terms instead of exact dates/times.",
        "layer": "dictionary",
        "instructions": (
            "Replace exact dates (ISO format like '2024-01-15') or "
            "date functions (DATETIME('now', '-7 days')) with natural "
            "relative temporal phrases.  For past references use phrases "
            "like 'within the last 7 days', 'in the past week', "
            "'over the last month'.  For future references use 'in 3 days', "
            "'tomorrow', etc.  Only applicable when the query contains "
            "date literals or date arithmetic functions."
        ),
        "temporal_phrases": [
            "recently", "lately", "in the past", "ago", "last",
            "within the last", "over the past", "the past",
            "previous", "prior", "before", "after", "since",
            "this week", "this month", "this year", "today", "yesterday",
            "tomorrow", "next", "upcoming", "current",
        ],
        "past_alternatives": ["within the last", "in the past", "over the last"],
        "future_alternatives": ["in", "out"],
        "not_applicable_when": (
            "Query contains no date/time literals or date functions.  "
            "Also not applicable to INSERT statements."
        ),
        "example_before": "Show orders where created_at > DATETIME('now', '-7 days').",
        "example_after":  "Show orders where created_at within the last 7 days.",
    },
}

# Total number of perturbation types defined above.
TOTAL_PERTURBATION_TYPES = len(perturbation_types)  # 13
