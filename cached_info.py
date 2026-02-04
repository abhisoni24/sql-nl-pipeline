perturbation_types = [
    {
      "id": 1,
      "name": "omit_obvious_operation_markers",
      "display_name": "Omit Obvious SQL Operation Markers",
      "description": "Remove explicit SQL keywords and clause names while preserving minimal natural-language cues required to unambiguously infer the intended DML operation (SELECT, UPDATE, INSERT, DELETE).",
      "instruction": "Omit explicit SQL keywords and structural markers while retaining sufficient natural-language signals so that the intended database operation and constraints remain clear.",
      "examples": [
        {
          "original": "SELECT username, email FROM users WHERE is_verified = true.",
          "perturbed": "Show username and email for users that are verified."
        },
        {
          "original": "UPDATE users SET is_verified = true WHERE id = 5.",
          "perturbed": "For the user with id 5, mark is_verified as true."
        },
        {
          "original": "DELETE FROM sessions WHERE expires_at < now.",
          "perturbed": "Remove sessions that expired before now."
        },
        {
          "original": "INSERT INTO users (username, email) VALUES ('alice', 'a@example.com').",
          "perturbed": "Add a new user with username alice and email a@example.com."
        }
      ],
      "application_rules": [
        "Remove explicit SQL operation keywords (SELECT, UPDATE, INSERT, DELETE)",
        "Remove structural clause keywords (FROM, WHERE, SET, VALUES, INTO)",
        "Preserve operation polarity using minimal natural-language cues:",
        "  - SELECT: neutral retrieval phrasing (show, list, get) or implied retrieval",
        "  - UPDATE: mutation cues (set, change, mark, should be)",
        "  - DELETE: removal cues (remove, delete, clear)",
        "  - INSERT: creation cues (add, create, new)",
        "Preserve target entities (tables), attributes (columns), and value bindings",
        "Preserve clear scope anchoring for conditions (e.g., 'for user with id 5')",
        "Avoid ambiguity that could cause the operation type to be misinterpreted"
      ],
      "systematic_generation": {
        "feasible": true,
        "method": "Classify query type (SELECT/UPDATE/INSERT/DELETE), remove operation-specific SQL keywords, and inject minimal operation-preserving natural-language verbs from fixed verb banks while linearizing remaining content.",
        "complexity": "Low to Medium - requires query-type detection and operation-aware template rules but no full SQL parsing"
      }
    },
    {
      "id": 2,
      "name": "phrasal_and_idiomatic_action_substitution",
      "display_name": "Idiomatic and Phrasal Action Substitution",
      "description": "Replace standard SQL-lite action verbs (get, show, select) with multi-word phrasal verbs, idiomatic expressions, and functional intent descriptors.",
      "instruction": "Transform the core request verb into a more natural, multi-word phrasal verb or result-oriented idiom while keeping the rest of the query intact.",
      "examples": [
        {
          "original": "Get all columns from posts where view_count > 100.",
          "perturbed_phrasal": "Pull up all columns from posts where view_count > 100.",
          "perturbed_idiomatic": "Dig out all columns from posts where view_count > 100."
        },
        {
          "original": "Show all users where is_verified equals true.",
          "perturbed_phrasal": "Run a check for all users where is_verified equals true.",
          "perturbed_functional": "Give me a listing of all users where is_verified equals true."
        },
        {
          "original": "Select username from users.",
          "perturbed_phrasal": "Fetch back the username from users.",
          "perturbed_idiomatic": "Snag the username from users."
        }
      ],
      "application_rules": [
        "Target only the root action verb (The 'intent' verb).",
        "Replace 1-to-1 verbs with phrasal verbs (e.g., show → bring up, get → pull up).",
        "Use 'Result-Oriented' idioms that imply the action (e.g., select → give me a list of, show → let's see).",
        "Ensure the substitution does not overlap with Operator (ID 5) or Schema (ID 12) synonyms.",
        "Maintain the grammatical flow of the noun phrases following the verb."
      ],
      "substitution_banks": {
        "RETRIEVE (Select/Get)": [
          "pull up", 
          "dig out", 
          "fetch back", 
          "snag", 
          "grab", 
          "grab a hold of", 
          "extract a list of"
        ],
        "DISPLAY (Show/List)": [
          "bring up", 
          "give me a look at", 
          "run a check for", 
          "produce a listing of", 
          "spit out", 
          "display for me"
        ],
        "QUERY (Search/Find)": [
          "look through", 
          "search for", 
          "track down", 
          "filter through", 
          "identify"
        ]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Identify the primary imperative verb at the start of the prompt. Replace with a randomly sampled phrase from the corresponding intent bank. Adjust surrounding 'a/the' articles if necessary for grammatical flow.",
        "complexity": "Medium - requires identifying the primary action verb and managing minor grammatical shifts (e.g., 'Get users' → 'Give me a list of users')."
      }
    },
    {
      "id": 3,
      "name": "sentence_structure_variation",
      "display_name": "Sentence Structure Variation",
      "description": "Transform the sentence structure between active, passive, imperative, and interrogative forms.",
      "instruction": "Rewrite the prompt using different grammatical structures while preserving meaning.",
      "examples": [
        {
          "original": "Get all users where is_verified equals true.",
          "perturbed_active": "I need all users where is_verified equals true.",
          "perturbed_passive": "All users where is_verified equals true should be retrieved.",
          "perturbed_imperative": "Retrieve all users where is_verified equals true.",
          "perturbed_question": "Can you get all users where is_verified equals true?"
        },
        {
          "original": "Select posts with view_count greater than 500.",
          "perturbed_active": "We want posts with view_count greater than 500.",
          "perturbed_passive": "Posts with view_count greater than 500 need to be selected.",
          "perturbed_imperative": "Find posts with view_count greater than 500.",
          "perturbed_question": "Could you select posts with view_count greater than 500?"
        }
      ],
      "application_rules": [
        "Active: Add subject ('I need', 'We want', 'The system requires')",
        "Passive: Use passive voice ('should be retrieved', 'is needed')",
        "Imperative: Use command form ('Get', 'Retrieve', 'Find')",
        "Question: Convert to question form ('Can you', 'Would you', 'Could you')"
      ],
      "structure_patterns": {
        "active": ["I need", "We want", "I'm looking for", "We require"],
        "passive": ["should be retrieved", "needs to be fetched", "is required"],
        "imperative": ["Get", "Retrieve", "Find", "Extract", "Pull"],
        "question": ["Can you get", "Would you fetch", "Could you retrieve", "What are"]
      },
      "systematic_generation": {
        "feasible": false,
        "method": "Requires syntactic parsing and grammatical transformation rules. Would need NLP libraries or LLM for accurate restructuring.",
        "complexity": "High - requires deep linguistic knowledge"
      }
    },
    {
      "id": 4,
      "name": "verbosity_variation",
      "display_name": "Verbosity with Fillers and Informal Language",
      "description": "Add conversational fillers, hedging language, and informal expressions to make the prompt more verbose and casual.",
      "instruction": "Insert filler words, informal language, and conversational elements while maintaining query intent.",
      "examples": [
        {
          "original": "Get all posts where view_count > 100.",
          "perturbed": "So, I'd like to basically get, you know, all the posts where the view_count is, like, greater than 100 or something."
        },
        {
          "original": "Select users where country_code equals 'US'.",
          "perturbed": "Okay, so I kind of need to select, um, users where the country_code is basically 'US', I think."
        }
      ],
      "application_rules": [
        "Add filler words: 'basically', 'like', 'you know', 'kind of', 'sort of'",
        "Add hedging: 'I think', 'maybe', 'probably', 'or something'",
        "Add conversational starts: 'So', 'Well', 'Okay', 'Alright'",
        "Add informal phrasing: 'I'd like to', 'Can we', 'I want'",
        "Add redundancy: 'all the', 'any and all', 'each and every'"
      ],
      "filler_phrases": {
        "hedging": ["basically", "kind of", "sort of", "like", "you know", "I think", "probably"],
        "conversational": ["So", "Well", "Okay", "Alright", "Um", "Uh"],
        "informal": ["gonna", "wanna", "gotta", "a bunch of", "or something", "or whatever"],
        "redundant": ["all the", "any and all", "each and every", "the whole"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Random insertion of filler phrases at strategic positions (start, before verbs, after nouns). Use position-based rules and filler banks.",
        "complexity": "Medium - requires position identification but uses template insertion"
      }
    },
    {
      "id": 5,
      "name": "operator_aggregate_variation",
      "display_name": "Operator Format and Aggregate Description Variation",
      "description": "Express comparison operators and aggregate functions using varied natural language descriptions and symbolic formats.",
      "instruction": "Replace standard operators and aggregate functions with alternative expressions, symbols, or verbose descriptions.",
      "examples": [
        {
          "original": "Get COUNT(*) from comments where view_count > 50.",
          "perturbed_operator": "Get COUNT(*) from comments where view_count is greater than 50.",
          "perturbed_aggregate": "Get the total number of comments where view_count > 50.",
          "perturbed_both": "Get how many comments have a view_count exceeding 50."
        },
        {
          "original": "Select AVG(view_count) from posts where posted_at >= '2024-01-01'.",
          "perturbed_operator": "Select AVG(view_count) from posts where posted_at is at least '2024-01-01'.",
          "perturbed_aggregate": "Select the average view_count from posts where posted_at >= '2024-01-01'.",
          "perturbed_both": "Select the mean view_count from posts posted no earlier than '2024-01-01'."
        }
      ],
      "application_rules": [
        "Replace operators: > → 'greater than'/'exceeds'/'above', = → 'equals'/'is'/'matches'",
        "Replace aggregates: COUNT → 'total number'/'how many', SUM → 'total'/'sum up', AVG → 'average'/'mean'",
        "Use symbolic variants: '>=' → 'at least', '<=' → 'at most', '!=' → 'not equal to'",
        "Mix symbolic and verbal: 'view_count > 50' → 'view_count exceeds 50'"
      ],
      "operator_variations": {
        ">": ["greater than", "exceeds", "more than", "above", "higher than"],
        "<": ["less than", "below", "under", "fewer than", "lower than"],
        ">=": ["at least", "greater than or equal to", "minimum of", "no less than"],
        "<=": ["at most", "less than or equal to", "maximum of", "no more than"],
        "=": ["equals", "is", "matches", "is equal to"],
        "!=": ["not equal to", "is not", "doesn't match", "different from"]
      },
      "aggregate_variations": {
        "COUNT": ["total number of", "how many", "count of", "number of", "quantity of"],
        "SUM": ["total", "sum of", "add up", "combined total"],
        "AVG": ["average", "mean", "average value of", "typical"],
        "MAX": ["maximum", "highest", "largest", "biggest"],
        "MIN": ["minimum", "lowest", "smallest", "least"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Pattern matching for operators (>, <, =, etc.) and aggregate functions (COUNT, SUM, AVG). Dictionary-based replacement with predefined mappings.",
        "complexity": "Low - regex pattern matching and dictionary lookup"
      }
    },
    {
      "id": 6,
      "name": "typos",
      "display_name": "Typos and Misspellings",
      "description": "Introduce realistic developers-like keyboard typos in table names, column names, or query-related terms.",
      "instruction": "Add 1-2 realistic typos while keeping the prompt readable and intent clear.",
      "examples": [
        {
          "original": "Get all columns from users where email equals 'test@example.com'.",
          "perturbed": "Get all columns from users where emial equals 'test@example.com'."
        },
        {
          "original": "Select all posts where view_count exceeds 1000.",
          "perturbed": "Selct all posts where view_count exceeds 1000."
        }
      ],
      "application_rules": [
        "Introduce 1-2 typos per prompt maximum",
        "Use realistic typo patterns: adjacent key swap, missing letter, duplicate letter",
        "Target table names, column names, or common words",
        "Avoid typos that completely obscure meaning",
        "Common patterns: usres, psots, coments, whre, slect"
      ],
      "typo_patterns": {
        "character_swap": ["users → usres", "posts → psots", "email → emial"],
        "missing_character": ["comments → coments", "where → whre", "count → cont"],
        "duplicate_character": ["from → fromm", "likes → likkes", "follows → folllows"],
        "adjacent_key": ["select → sekect", "content → contnet", "user_id → user_ud"]
      },
      "common_typos": {
        "users": ["usres", "uesrs", "usrs"],
        "posts": ["psots", "psts", "posst"],
        "comments": ["coments", "commments", "commnets"],
        "email": ["emial", "emai", "emaill"],
        "where": ["whre", "wher", "hwere"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Rule-based typo generation: (1) swap adjacent characters, (2) drop random character, (3) duplicate random character, (4) substitute with adjacent keyboard key. Target specific word positions.",
        "complexity": "Low - character-level string manipulation with position-based rules"
      }
    },
    {
      "id": 7,
      "name": "comment_annotations",
      "display_name": "Comment-Style Annotations",
      "description": "Add SQL-style comments or parenthetical notes within the natural language prompt.",
      "instruction": "Insert comments, annotations, or clarifications using SQL comment syntax or parenthetical expressions.",
      "examples": [
        {
          "original": "Get all users where country_code equals 'US'.",
          "perturbed": "Get all users -- need to filter by country -- where country_code equals 'US' (United States only)."
        },
        {
          "original": "Select posts with view_count greater than 500.",
          "perturbed": "Select posts (popular ones) with view_count greater than 500 -- for analytics."
        }
      ],
      "application_rules": [
        "Add SQL-style comments: '-- comment text'",
        "Add parenthetical notes: '(clarification here)'",
        "Add inline clarifications that don't change meaning",
        "Place comments mid-sentence or at end",
        "Keep annotations brief and relevant"
      ],
      "comment_patterns": {
        "sql_comment": ["-- this is important", "-- note:", "-- filtering here"],
        "parenthetical": ["(i.e., ...)", "(specifically ...)", "(note: ...)", "(only ...)"],
        "clarification": ["in other words", "that is", "meaning"]
      },
      "example_annotations": [
        "-- for reporting purposes",
        "(active users only)",
        "-- important: check this",
        "(excluding deleted records)"
      ],
      "systematic_generation": {
        "feasible": true,
        "method": "Template-based insertion at predetermined positions (after table names, after WHERE, at end). Use annotation bank with random selection.",
        "complexity": "Low - position-based template insertion"
      }
    },
    {
      "id": 8,
      "name": "temporal_expression_variation",
      "display_name": "Temporal Expression Variation",
      "description": "Express date/time conditions using varied temporal language, from specific dates to relative expressions.",
      "instruction": "Replace date/time conditions with alternative temporal expressions ranging from specific to vague.",
      "examples": [
        {
          "original": "Get all posts where posted_at > '2024-01-01'.",
          "perturbed_relative": "Get all posts from this year.",
          "perturbed_natural": "Get all posts posted after January first, twenty twenty-four."
        },
        {
          "original": "Select comments where created_at >= '2024-06-15'.",
          "perturbed_natural": "Select comments created after June fifteenth, two thousand twenty-four."
        }
      ],
      "application_rules": [
        "Replace ISO dates with natural language: '2024-01-01' → 'January 1st, 2024'",
        "Use relative time only if the original meaning does not change: 'last week', 'yesterday', 'this month', 'recent'",
        "Use vague periods only if the original meaning does not change: 'recently', 'lately', 'a while ago', 'not long ago'",
        "Mix formats: '2024' vs 'twenty twenty-four'",
        "Only apply if original prompt contains date/time conditions"
      ],
      "temporal_patterns": {
        "specific_date": ["January 1st, 2024", "the first of January", "Jan 1 2024", "01/01/2024"],
        "relative": ["last week", "yesterday", "this month", "last year", "two days ago"],
        #"vague": ["recently", "lately", "a while ago", "not long ago", "some time ago"], (too vague in most cases)
        "comparison": ["newer than", "older than", "after", "before", "since"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Regex pattern matching for date formats (YYYY-MM-DD, timestamps). Dictionary-based replacement with context-aware rules (> date → 'after', < date → 'before').",
        "complexity": "Medium - requires date parsing and context-aware replacement"
      }
    },
    {
      "id": 9,
      "name": "punctuation_variation",
      "display_name": "Punctuation Variation",
      "description": "Introduce varied punctuation marks (commas, semicolons, dashes, ellipsis) at different positions in the prompt.",
      "instruction": "Insert or modify punctuation marks to create different sentence rhythms while preserving meaning.",
      "examples": [
        {
          "original": "Get all users where is_verified equals true and country_code equals 'US'.",
          "perturbed": "Get all users, where is_verified, equals true, and country_code, equals 'US'."
        },
        {
          "original": "Select posts with view_count greater than 1000 and posted_at after '2024-01-01'.",
          "perturbed": "Select posts with view_count greater than 1000; posted_at after '2024-01-01'."
        },
        {
          "original": "Retrieve comments from users where comment_text is not empty.",
          "perturbed": "Retrieve comments from users... where comment_text is not empty."
        }
      ],
      "application_rules": [
        "Insert commas before/after WHERE, AND, OR clauses",
        "Replace AND/OR with semicolons for clause separation",
        "Add ellipsis (...) for dramatic pauses",
        "Use em-dashes (--) or en-dashes (–) to separate conditions",
        "Insert 1-3 punctuation modifications per prompt",
        "Maintain sentence readability and meaning"
      ],
      "punctuation_types": {
        "comma_insertion": ["before WHERE", "after table names", "between conditions", "after SELECT"],
        "semicolon_replacement": ["replace AND with ;", "replace OR with ;", "separate major clauses"],
        "ellipsis": ["before WHERE", "between clauses", "after table reference"],
        "dash_insertion": ["before conditions", "around parenthetical info", "between clauses"]
      },
      "insertion_positions": [
        "After SELECT/GET keywords",
        "Before/after WHERE",
        "Between AND/OR conditions",
        "After table names",
        "Before comparison operators"
      ],
      "systematic_generation": {
        "feasible": true,
        "method": "Pattern matching for clause boundaries (WHERE, AND, OR, FROM). Randomly select 1-3 positions and insert punctuation from predefined types. Replace conjunctions with semicolons.",
        "complexity": "Low - position-based insertion with random selection"
      }
    },
    {
      "id": 10,
      "name": "urgency_qualifiers",
      "display_name": "Urgency Qualifiers",
      "description": "Add urgency or priority indicators to the query request.",
      "instruction": "Prepend or append urgency markers, priority levels, or time-sensitive language.",
      "examples": [
        {
          "original": "Get all posts where view_count > 1000.",
          "perturbed_high": "URGENT: Get all posts where view_count > 1000.",
          "perturbed_low": "When you get a chance, get all posts where view_count > 1000.",
          "perturbed_priority": "High priority - get all posts where view_count > 1000."
        },
        {
          "original": "Select users where is_verified equals true.",
          "perturbed_high": "ASAP: Select users where is_verified equals true.",
          "perturbed_low": "No rush, but select users where is_verified equals true.",
          "perturbed_priority": "Critical - select users where is_verified equals true."
        }
      ],
      "application_rules": [
        "Add urgency markers: 'URGENT', 'ASAP', 'immediately', 'right away'",
        "Add low urgency: 'when you can', 'no rush', 'at your convenience'",
        "Add priority levels: 'High priority', 'Low priority', 'Critical'",
        "Add time pressure: 'need this now', 'quickly', 'as soon as possible'",
        "Place at beginning or end of prompt"
      ],
      "urgency_levels": {
        "high": ["URGENT:", "ASAP:", "Immediately:", "Right away:", "Critical:", "High priority:"],
        "medium": ["Soon:", "Please prioritize:", "Important:", "Need this:"],
        "low": ["When you can,", "No rush,", "At your convenience,", "Eventually,", "Low priority:"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Prepend or append urgency markers from predefined lists. Random selection based on urgency level (high/medium/low).",
        "complexity": "Low - simple string prepend/append with random selection"
      }
    },
    {
      "id": 11,
      "name": "mixed_sql_nl",
      "display_name": "Mixed SQL and Natural Language",
      "description": "Blend SQL syntax elements directly into natural language prompts.",
      "instruction": "Intermix actual SQL keywords, operators, or syntax with natural language descriptions.",
      "examples": [
        {
          "original": "Get all users where country_code equals 'US' and is_verified equals true.",
          "perturbed": "SELECT all users WHERE country_code = 'US' and they are verified."
        },
        {
          "original": "Retrieve posts with view_count greater than 500.",
          "perturbed": "Retrieve posts WHERE view_count > 500."
        }
      ],
      "application_rules": [
        "Mix SQL keywords (SELECT, WHERE, AND, OR) with natural language",
        "Use SQL operators (=, >, <) alongside natural descriptions",
        "Combine table.column notation with informal references",
        "Create hybrid syntax that's neither pure SQL nor pure natural language",
        "Keep some structure recognizable to both SQL and natural language"
      ],
      "mixing_patterns": {
        "keyword_mix": ["SELECT from users where verified", "Get * FROM posts WHERE popular"],
        "operator_mix": ["users where email = 'test' and verified", "posts with view_count > 100"],
        "notation_mix": ["Get users.username where active", "SELECT posts.content from posts that are recent"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Pattern matching for natural language components. Replace specific phrases with SQL equivalents (where→WHERE, equals→=, greater than→>). Maintain hybrid structure.",
        "complexity": "Low - dictionary-based replacement with SQL keyword injection"
      }
    },
    {
      "id": 12,
      "name": "table_column_synonyms",
      "display_name": "Synonyms of Tables and Column Names",
      "description": "Replace actual schema table/column names with plausible synonyms that human programmers might naturally use.",
      "instruction": "Substitute schema-specific names with domain-appropriate synonyms while maintaining query intent.",
      "examples": [
        {
          "original": "Get all columns from posts where view_count > 100.",
          "perturbed": "Get all columns from articles where views > 100."
        },
        {
          "original": "Select username and email from users where is_verified equals true.",
          "perturbed": "Select user_name and email_address from members where confirmed equals true."
        }
      ],
      "application_rules": [
        "Replace table names with synonyms maintaining semantic similarity",
        "Replace column names with natural alternatives",
        "Maintain consistency within the same prompt",
        "Use domain-appropriate alternatives",
        "Preserve the essential meaning of the schema element"
      ],
      # needs to be generated based on the schema from a dictionary filtered by LLMs for generality.
      "schema_synonyms": {
        "users": ["members", "accounts", "profiles", "customers"],
        "posts": ["articles", "entries", "content", "publications", "messages"],
        "comments": ["replies", "responses", "feedback", "remarks"],
        "likes": ["reactions", "favorites", "appreciations", "endorsements"],
        "follows": ["subscriptions", "connections", "friendships"],
        "user_id": ["member_id", "account_id", "profile_id", "uid"],
        "post_id": ["article_id", "content_id", "entry_id", "pid"],
        "view_count": ["views", "visit_count", "page_views", "impressions"],
        "email": ["email_address", "contact", "mail"],
        "username": ["user_name", "login", "handle", "screen_name"],
        "content": ["body", "text", "message", "post_text"],
        "comment_text": ["reply_text", "comment_body", "response"],
        "is_verified": ["verified", "is_confirmed", "confirmed", "validation_status"],
        "signup_date": ["registration_date", "joined_date", "created_at", "member_since"],
        "posted_at": ["published_at", "created_at", "timestamp", "post_date"],
        "liked_at": ["reaction_time", "favorited_at", "timestamp"],
        "followed_at": ["connection_date", "subscribed_at", "friend_since"]
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Dictionary-based replacement. Parse prompt for schema elements (table/column names), look up in synonym dictionary, replace with random selection.",
        "complexity": "Low - dictionary lookup and string replacement"
      }
    },
    {
      "id": 13,
      "name": "incomplete_join_spec",
      "display_name": "Incomplete JOIN Specification",
      "description": "When querying multiple related tables, omit explicit join conditions, join types, or relationship details.",
      "instruction": "Remove explicit JOIN syntax and foreign key references, using only natural language to imply relationships.",
      "examples": [
        {
          "original": "Get all columns from users JOIN posts ON users.id = posts.user_id where posts.view_count > 100.",
          "perturbed": "Get all columns from users and their posts where view_count > 100."
        },
        {
          "original": "Select username from users LEFT JOIN comments ON users.id = comments.user_id.",
          "perturbed": "Select username from users along with their comments."
        }
      ],
      "application_rules": [
        "Remove JOIN keywords (JOIN, INNER JOIN, LEFT JOIN)",
        "Remove ON clauses and explicit foreign key relationships",
        "Use natural language: 'and their', 'with', 'along with', 'including'",
        "Omit join type specification if the meaning can be implied by context",
        "Let relationships be implied by context",
        "Only apply when original prompt involves multiple tables"
      ],
      "relationship_phrases": {
        "possessive": ["users and their posts", "posts and their comments", "users with their likes"],
        "inclusive": ["users along with posts", "posts including comments", "users together with follows"],
        "connective": ["users connected to posts", "posts related to comments", "users associated with likes"]
      },
      "join_scenarios": {
        "user_posts": "users and their posts",
        "post_comments": "posts with comments",
        "user_likes": "users and the posts they liked",
        "user_followers": "users and their followers"
      },
      "systematic_generation": {
        "feasible": true,
        "method": "Pattern matching for JOIN keywords and ON clauses. Remove JOIN...ON blocks, replace with relationship phrases from predefined templates based on table pairs.",
        "complexity": "Medium - requires JOIN clause parsing and relationship inference"
      }
    },
    {
      "id": 14,
      "name": "anchored_pronoun_references",
      "display_name": "Anchored Pronoun References",
      "description": "Replace a single repeated table, column, or value reference with a pronoun whose antecedent is uniquely and locally recoverable from context, introducing mild referential noise without losing semantic clarity.",
      "instruction": "Substitute ONE repeated explicit reference with a pronoun such that exactly one plausible antecedent exists in the surrounding context and the intended SQL query remains unambiguous.",
      "examples": [
        {
          "original": "Get all columns from users where country_code = 'US' and country_code is not null.",
          "perturbed": "Get all columns from users where country_code = 'US' and it is not null."
        },
        {
          "original": "Select posts where view_count > 1000 and view_count < 5000.",
          "perturbed": "Select posts where view_count > 1000 and it < 5000."
        },
        {
          "original": "Get users where signup_country = 'US' and billing_country = 'US'.",
          "perturbed": "Get users where signup_country = 'US' and billing_country = that."
        },
        {
          "original": "Select sessions where created_at > '2024-01-01' and last_active_at > '2024-01-01'.",
          "perturbed": "Select sessions where created_at > '2024-01-01' and last_active_at > that date."
        }
      ],
      "application_rules": [
        "Replace ONLY ONE explicit reference per prompt",
        "Replace only a repeated reference; never replace the first occurrence",
        "Ensure exactly one valid antecedent exists in the same clause or immediately preceding context",
        "Target tables, columns, or literal values that are non-critical identifiers",
        "Avoid replacing primary keys, join keys, or schema-unique identifiers",
        "Ensure the resulting prompt would be interpreted consistently by a competent human reader"
      ],
      "pronoun_substitutions": {
        "table_reference": ["it", "that table", "this table"],
        "column_reference": ["it", "that column", "this field"],
        "value_reference": ["that", "that value", "that date"]
      },
      "safe_replacement_strategy": [
        "Identify repeated table, column, or value mentions",
        "Select the second occurrence as the replacement candidate",
        "Verify uniqueness of antecedent within scope",
        "Abort perturbation if multiple plausible antecedents exist",
        "Abort perturbation if replacement would cause semantic ambiguity"
      ],
      "systematic_generation": {
        "feasible": true,
        "method": "Parse prompt to identify repeated references, verify local uniqueness of antecedents, and replace the second occurrence with an appropriate pronoun from a type-specific list.",
        "complexity": "Medium - requires reference tracking and antecedent validation but no full discourse modeling"
      }
    }
  ]
schema = {
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

# Define valid join paths (left_table, right_table): (left_key, right_key)
foreign_keys = {
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


instructions = ''' Instructions

## Part 1: Single Perturbations (14 versions)
For each of the 14 perturbation types:
1. Carefully read the perturbation description and application rules
2. Evaluate if the perturbation is applicable to this specific nl_prompt
3. If applicable: Generate a perturbed version following the rules and examples
4. If NOT applicable: Mark as "not_applicable" and provide a brief reason why

## Part 2: Compound Perturbation (1 version)
1. Select 2-5 perturbations that are applicable to this prompt
2. Apply them simultaneously to create a realistic compound perturbation
3. Prioritize perturbations that commonly co-occur in real developer behavior
4. List all perturbations applied

# Output Format
Return your response as a valid JSON object with this exact structure:
```json
{
  "original": {
    "nl_prompt": "<original prompt>",
    "sql": "<original SQL>",
    "tables": ["<table names>"],
    "complexity": "<complexity level>"
  },
  "single_perturbations": [
    {
      "perturbation_id": 1,
      "perturbation_name": "under_specification",
      "applicable": true,
      "perturbed_nl_prompt": "<perturbed version>",
      "changes_made": "<brief description of what was changed>",
      "reason_not_applicable": null
    },
    {
      "perturbation_id": 2,
      "perturbation_name": "implicit_business_logic",
      "applicable": false,
      "perturbed_nl_prompt": null,
      "changes_made": null,
      "reason_not_applicable": "<explanation why this perturbation doesn't apply>"
    },
    // ... continue for all 10 perturbations
  ],
  "compound_perturbation": {
    "perturbations_applied": [
      {
        "perturbation_id": 1,
        "perturbation_name": "under_specification"
      },
      {
        "perturbation_id": 3,
        "perturbation_name": "synonym_substitution"
      },
      {
        "perturbation_id": 10,
        "perturbation_name": "typos"
      }
    ],
    "perturbed_nl_prompt": "<compound perturbed version>",
    "changes_made": "<description of all changes made>"
  },
  "metadata": {
    "total_applicable_perturbations": 8,
    "total_not_applicable": 2,
    "applicability_rate": 0.8
  }
}
```

# Important Notes
1. Ensure the JSON is valid and properly formatted
2. For not_applicable cases, set perturbed_nl_prompt to null and provide reason_not_applicable
3. For compound perturbation, only use applicable perturbations
4. Maintain the original query intent in all perturbations
5. Make perturbations realistic - simulate actual developer behavior
6. Do not add explanations outside the JSON structure
7. Return ONLY the JSON object, no additional text

Generate the perturbed versions now.
'''
#Ignore the following text, kept for later:
# **Applicability Guidelines:**
# - under_specification: Applicable if prompt has explicit table/column names
# - implicit_business_logic: Applicable if prompt has specific conditions/filters
# - synonym_substitution: Applicable if prompt contains table or column names
# - incomplete_joins: Applicable ONLY if prompt mentions multiple tables or relationships
# - relative_temporal: Applicable ONLY if prompt has date/time conditions
# - ambiguous_pronouns: Applicable if prompt has explicit entity references
# - vague_aggregation: Applicable ONLY if prompt contains aggregation (COUNT, SUM, AVG, GROUP BY)
# - column_variations: Applicable if prompt has explicit column names
# - missing_where_details: Applicable if prompt has WHERE conditions with specific values
# - typos: Always applicable