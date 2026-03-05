"""
Dictionary Builder.

Automatically constructs a LinguisticDictionary from a SchemaConfig using:
  1. Identifier tokenization (snake_case / camelCase splitting).
  2. WordNet synonym expansion (optional, requires NLTK).
  3. Semantic category inference via WordNet hypernym chains.

Also provides YAML save/load for manual review and version control.
"""

import re
import yaml
from typing import List, Optional
from .schema_config import SchemaConfig
from .linguistic_dictionary import LinguisticDictionary


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_dictionary(
    schema: SchemaConfig,
    use_wordnet: bool = True
) -> LinguisticDictionary:
    """
    Build a LinguisticDictionary from a SchemaConfig.

    Steps:
      1. Tokenize each table/column identifier (snake_case splitting).
      2. If use_wordnet=True, expand tokens with WordNet synsets.
      3. Assemble compound synonyms for multi-word column names.
      4. Infer semantic categories for tables (person/event/object).

    Args:
        schema: The SchemaConfig to build a dictionary for.
        use_wordnet: Whether to use WordNet for synonym expansion.
                     Set to False if NLTK is not available.

    Returns:
        A populated LinguisticDictionary.
    """
    dictionary = LinguisticDictionary()

    # Ensure WordNet is available if requested
    if use_wordnet:
        use_wordnet = _ensure_wordnet()

    # Collect all table names (and their underscore/space variants) for
    # cross-table-name synonym filtering.  A synonym of table A that
    # coincides with the *name* of table B would cause false positives
    # in the NL pipeline tests.
    _all_table_name_forms: set[str] = set()
    for tname in schema.tables:
        tl = tname.lower()
        _all_table_name_forms.add(tl)
        _all_table_name_forms.add(tl.replace("_", " "))

    for tname, tdef in schema.tables.items():
        tokens = _tokenize_identifier(tname)

        # --- Table synonyms ---
        base_name = tname.replace("_", " ")
        table_syns = [base_name]
        if use_wordnet:
            wn_syns = _expand_tokens_wordnet(tokens)
            # Filter out synonyms that are too generic or identical
            wn_syns = [s for s in wn_syns if s != base_name and len(s) > 2]
            # Filter out synonyms that collide with another table's name
            wn_syns = [
                s for s in wn_syns
                if s.lower() not in _all_table_name_forms
            ]
            table_syns.extend(wn_syns[:5])
        dictionary.table_synonyms[tname] = table_syns

        # --- Table semantic category ---
        dictionary.table_categories[tname] = _infer_category(tokens, use_wordnet)

        # --- Column synonyms ---
        for cname, cdef in tdef.columns.items():
            col_tokens = _tokenize_identifier(cname)
            col_base = cname.replace("_", " ")
            col_syns = [col_base]

            if use_wordnet:
                if len(col_tokens) >= 2:
                    compound_syns = _expand_compound_wordnet(col_tokens)
                else:
                    compound_syns = _expand_tokens_wordnet(col_tokens)
                compound_syns = [s for s in compound_syns if s != col_base and len(s) > 2]
                col_syns.extend(compound_syns[:4])

            qualified_key = f"{tname}.{cname}"
            dictionary.column_synonyms[qualified_key] = col_syns

    return dictionary


def save_dictionary(dictionary: LinguisticDictionary, path: str) -> None:
    """
    Save a LinguisticDictionary to a YAML file for review and version control.

    Only saves the schema-specific parts (table/column synonyms and categories).
    Universal banks are baked into the LinguisticDictionary defaults.
    """
    data = {
        "table_synonyms": dictionary.table_synonyms,
        "column_synonyms": dictionary.column_synonyms,
        "table_categories": dictionary.table_categories,
    }
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=True, allow_unicode=True)


def load_dictionary(path: str) -> LinguisticDictionary:
    """
    Load a LinguisticDictionary from a previously saved YAML file.

    The universal banks (operators, aggregates, etc.) are populated from
    the LinguisticDictionary defaults. Only schema-specific synonyms
    are loaded from the file.
    """
    with open(path) as f:
        data = yaml.safe_load(f)

    dictionary = LinguisticDictionary()
    dictionary.table_synonyms = data.get("table_synonyms", {})
    dictionary.column_synonyms = data.get("column_synonyms", {})
    dictionary.table_categories = data.get("table_categories", {})
    return dictionary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_wordnet() -> bool:
    """Check if WordNet is available, try to download if not."""
    try:
        from nltk.corpus import wordnet as wn
        # Quick test to see if the corpus is downloaded
        wn.synsets("test")
        return True
    except LookupError:
        try:
            import nltk
            nltk.download("wordnet", quiet=True)
            nltk.download("omw-1.4", quiet=True)
            return True
        except Exception:
            return False
    except ImportError:
        return False


def _tokenize_identifier(name: str) -> List[str]:
    """
    Split a snake_case or camelCase identifier into word tokens.

    Examples:
        'appointment_date' -> ['appointment', 'date']
        'userId'           -> ['user', 'id']
        'is_verified'      -> ['is', 'verified']
        'followee_id'      -> ['followee', 'id']
    """
    # Handle snake_case
    parts = name.split("_")
    # Handle camelCase within each part
    tokens = []
    for part in parts:
        camel_split = re.sub(r"([a-z])([A-Z])", r"\1_\2", part).split("_")
        tokens.extend([t.lower() for t in camel_split if t])
    return tokens


def _expand_tokens_wordnet(tokens: List[str]) -> List[str]:
    """
    Expand individual word tokens with WordNet synonyms.

    Skips very short or common tokens like 'id', 'at', 'is'.
    """
    try:
        from nltk.corpus import wordnet as wn
    except ImportError:
        return []

    skip_tokens = {"id", "at", "is", "by", "to", "of", "in", "on", "a", "an", "the"}
    synonyms = []
    seen = set()

    for token in tokens:
        if token in skip_tokens or len(token) <= 2:
            continue
        for synset in wn.synsets(token)[:3]:  # Top 3 synsets
            for lemma in synset.lemmas()[:4]:  # Top 4 lemmas per synset
                syn = lemma.name().replace("_", " ").lower()
                if syn != token and syn not in seen and len(syn) > 2:
                    seen.add(syn)
                    synonyms.append(syn)

    return synonyms[:6]  # Cap at 6 synonyms


def _expand_compound_wordnet(tokens: List[str]) -> List[str]:
    """
    Expand compound identifiers by substituting the primary semantic token.

    Example:
        ['appointment', 'date'] -> ['booking date', 'engagement date']

    The primary token is the first non-trivial token (not 'id', 'at', etc.).
    """
    try:
        from nltk.corpus import wordnet as wn
    except ImportError:
        return []

    skip_tokens = {"id", "at", "is", "by", "to", "of", "in", "on", "a", "an", "the",
                   "count", "text", "code", "name"}
    # Find the primary semantic token to expand
    primary_candidates = [t for t in tokens if t not in skip_tokens and len(t) > 2]
    if not primary_candidates:
        return _expand_tokens_wordnet(tokens)

    target = primary_candidates[0]
    alternatives = []
    seen = set()

    for synset in wn.synsets(target)[:3]:
        for lemma in synset.lemmas()[:3]:
            alt = lemma.name().replace("_", " ").lower()
            if alt != target and alt not in seen and len(alt) > 2:
                seen.add(alt)
                # Reconstruct the compound by replacing the target token
                new_tokens = [alt if t == target else t for t in tokens]
                alternatives.append(" ".join(new_tokens))

    return alternatives[:5]


def _infer_category(tokens: List[str], use_wordnet: bool) -> str:
    """
    Infer whether a table represents people, objects, events, etc.

    Uses keyword matching first, then falls back to WordNet hypernym chains
    if available.
    """
    person_indicators = {
        "user", "users", "member", "members", "patient", "patients",
        "doctor", "doctors", "employee", "employees", "staff",
        "customer", "customers", "client", "clients", "person", "people",
        "student", "students", "teacher", "teachers", "author", "authors",
    }
    event_indicators = {
        "event", "events", "appointment", "appointments",
        "meeting", "meetings", "session", "sessions", "visit", "visits",
        "transaction", "transactions", "order", "orders",
    }
    relation_indicators = {
        "follow", "follows", "like", "likes", "friendship",
        "connection", "connections", "subscription", "subscriptions",
    }

    if any(t in person_indicators for t in tokens):
        return "person"
    if any(t in event_indicators for t in tokens):
        return "event"
    if any(t in relation_indicators for t in tokens):
        return "relation"

    # WordNet hypernym fallback
    if use_wordnet:
        try:
            from nltk.corpus import wordnet as wn
            for token in tokens:
                synsets = wn.synsets(token)
                for synset in synsets[:2]:
                    hypernyms = set()
                    for path in synset.hypernym_paths():
                        for h in path:
                            hypernyms.add(h.name().split(".")[0])
                    if "person" in hypernyms or "organism" in hypernyms:
                        return "person"
                    if "event" in hypernyms:
                        return "event"
        except Exception:
            pass

    return "object"
