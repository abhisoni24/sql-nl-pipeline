# Hook-Based Renderer — Action Plan

## Goal

Decouple render-time perturbation logic from `nl_renderer.py` so that adding
or removing a render-time perturbation requires creating/deleting **one file
only** (`src/perturbations/<name>.py`), with **zero edits** to the renderer,
the enum, or the config class.

Post-processing perturbations already achieve this.  This plan brings
render-time perturbations to the same level.

---

## Current Architecture (Before)

```
Strategy.apply()
  → creates PerturbationConfig({PerturbationType.X})
  → creates SQLToNLRenderer(config)
  → calls renderer.render(ast)
  → renderer checks self.config.is_active(PerturbationType.X) at ~20 branch sites
```

**Touchpoints to add a render-time perturbation today**: 3 files
1. Add enum value to `PerturbationType` in `nl_renderer.py`
2. Add `is_active()` branches inside renderer methods
3. Create strategy file in `src/perturbations/`

---

## Target Architecture (After)

```
Strategy.apply()
  → creates SQLToNLRenderer()
  → calls renderer.render(ast, strategy=self)
  → renderer calls self._active_strategy.on_keyword(...) etc. at decision points
  → strategy returns modified/default value
```

**Touchpoints to add a render-time perturbation**: 1 file
1. Create strategy file in `src/perturbations/`, override hooks

`PerturbationType` enum, `PerturbationConfig`, and `is_active()` are eliminated.

---

## Hook Inventory

Analysis of all `is_active()` call sites in `nl_renderer.py` grouped by the
kind of rendering decision they affect:

### Hook 1: `on_keyword(keyword, default) → str`

**What it controls**: Whether SQL structural keywords (SELECT, FROM, WHERE)
appear literally or as NL equivalents, or are omitted entirely.

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 306  | `render_select` | `if is_active(MIXED_SQL_NL): parts.append("SELECT") else: parts.append(intent_verb)` |
| 324  | `render_select` | `if not is_active(OMIT_OBVIOUS): parts.append("FROM" if is_active(MIXED_SQL_NL) else ...)` |
| 336  | `render_select` | `if alias and not is_active(OMIT_OBVIOUS):` → show/hide alias |
| 349  | `render_select` | `if not is_active(OMIT_OBVIOUS): parts.append("WHERE" if is_active(MIXED_SQL_NL) else ...)` |
| 459  | `render_update` | `kw = "WHERE" if is_active(MIXED_SQL_NL) else "where"` |
| 461  | `render_update` | `if is_active(OMIT_OBVIOUS):` controls WHERE prefix |
| 485  | `render_delete` | `kw = "WHERE" if is_active(MIXED_SQL_NL) else "where"` |
| 487  | `render_delete` | `if is_active(OMIT_OBVIOUS):` controls WHERE prefix |

**Perturbations that use it**:
- `MIXED_SQL_NL` → return `keyword` (raw SQL keyword)
- `OMIT_OBVIOUS_CLAUSES` → return `""` for FROM/WHERE

**Default behavior** (no perturbation): return `default` (the NL equivalent)

**Renderer refactor**: At each site, replace the `if/else` with:
```python
kw = self._call_hook_keyword("SELECT", intent_verb)
```

### Hook 2: `on_join(join_node, table_nl, on_str, default_phrase, context) → str`

**What it controls**: How JOIN clauses are phrased — full "joined with X on
Y" vs simplified "and their X" vs incomplete "with X".

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 507  | `_render_join` | `if is_active(INCOMPLETE_JOIN_SPEC): return "and their {table}" / "{choice} {table}"` |

**Perturbations that use it**:
- `INCOMPLETE_JOIN_SPEC` → drop ON clause, use "and their" / "with" / "along with"

**Default behavior**: return `default_phrase` (the full join rendering with ON clause)

### Hook 3: `on_operator(op_key, left, right, default_str, is_temporal, context) → str`

**What it controls**: How comparison operators are phrased — canonical
("greater than") vs varied ("exceeds", "more than") vs temporal-safe
("after", "since").

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 591  | `_render_expression` | `if is_active(OPERATOR_AGGREGATE_VARIATION): return "{left} {op_template} {right}"` (with temporal detection) |

**Perturbations that use it**:
- `OPERATOR_AGGREGATE_VARIATION` → use varied operator phrasing

**Default behavior**: return `default_str` (canonical operator phrasing)

### Hook 4: `on_aggregate(agg_key, inner_text, default_str) → str`

**What it controls**: How aggregate functions are phrased — canonical
("COUNT of") vs varied ("total number of", "how many").

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 575  | `_render_expression` | `if isinstance(expr, AggFunc) and is_active(OPERATOR_AGGREGATE_VARIATION):` |

**Perturbations that use it**:
- `OPERATOR_AGGREGATE_VARIATION` → use varied aggregate phrasing

**Default behavior**: return `default_str` (function of the aggregate type)

**Note**: `OPERATOR_AGGREGATE_VARIATION` controls both operators AND
aggregates.  In the hook model, the strategy would override both
`on_operator` and `on_aggregate`.

### Hook 5: `on_verb(key, baseline_choice) → str`

**What it controls**: Which synonym is used for action verbs (get/show/select
for queries, insert/update/delete for DML).

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 207  | `_choose_word` | `if is_active(SYNONYM_SUBSTITUTION): pick alternative synonym` |

**Perturbations that use it**:
- `SYNONYM_SUBSTITUTION` → force a different synonym than the baseline choice

**Default behavior**: return `baseline_choice`

**Note**: Currently `_choose_word` handles both verbs AND connectors
(where, from, and, etc.).  The `SYNONYM_SUBSTITUTION` perturbation only
targets the verbs.  The hook should be scoped to verb-class words.
Connectors like "where"/"from" are covered by `on_keyword`.

### Hook 6: `on_table_reference(table_name, default_name) → str`

**What it controls**: Whether a table name is replaced with a schema synonym
or substituted with a pronoun.

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 850  | `_render_table` | `if is_active(TABLE_COLUMN_SYNONYMS): return synonym` |

**Perturbations that use it**:
- `TABLE_COLUMN_SYNONYMS` → return synonym from schema dictionary
- `AMBIGUOUS_PRONOUNS` → return pronoun ("it", "that") on second mention

**Default behavior**: return `default_name` (raw table name)

**Ambiguous pronouns design note**: The pronoun logic depends on stateful
mention-tracking (`self._mentions`, `self._recent_mentions`,
`self._ambig_pronoun_count`).  This state lives in the renderer because it
tracks the traversal.  The hook approach for pronouns:
- The renderer still tracks mentions (it must — it controls traversal order)
- The renderer exposes mention state to the hook: `is_repeated`, `mention_count`
- The strategy decides: "if repeated and I haven't pronoun'd yet, return a pronoun"
- This keeps the decision in the strategy while letting the renderer own the state

### Hook 7: `on_column_reference(col_name, table_qualifier, default_name) → str`

**What it controls**: Whether a column name is replaced with a schema synonym
or a pronoun, and how table qualification is handled.

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 903  | `_render_column` | `if is_active(TABLE_COLUMN_SYNONYMS): col_name = synonym` |
| 1001 | `_render_column` | `if not is_active(OMIT_OBVIOUS): return f"{table}.{col_name}"` |

**Perturbations that use it**:
- `TABLE_COLUMN_SYNONYMS` → return synonym from schema dictionary
- `AMBIGUOUS_PRONOUNS` → return pronoun on second mention
- `OMIT_OBVIOUS_CLAUSES` → drop table qualifier

**Default behavior**: return `default_name` (raw column name, possibly qualified)

### Hook 8: `on_temporal(raw_value, default_rendering) → str`

**What it controls**: Whether date literals and datetime expressions are
rendered literally ("2024-01-15") or as relative expressions ("recently",
"within the last 30 days").

**Call sites being replaced**:

| Line | Method | Current code |
|------|--------|-------------|
| 660  | `_render_expression` | `if isinstance(Literal) and is_active(TEMPORAL_EXPRESSION_VARIATION): return temp_choice` |
| 1129 | `_render_sqlite_datetime` | `if is_active(TEMPORAL_EXPRESSION_VARIATION): return rng.choice([...])` |
| 1163 | `_render_datetime_node` | `if is_active(TEMPORAL_EXPRESSION_VARIATION): return rng.choice([...])` |

**Perturbations that use it**:
- `TEMPORAL_EXPRESSION_VARIATION` → replace ISO dates with relative expressions

**Default behavior**: return `default_rendering` (literal date or "X days ago")

---

## Perturbation ↔ Hook Mapping

| Strategy | Hooks it overrides |
|----------|-------------------|
| `mixed_sql_nl` | `on_keyword` |
| `omit_obvious` | `on_keyword`, `on_column_reference` |
| `incomplete_join` | `on_join` |
| `operator_aggregate_variation` | `on_operator`, `on_aggregate` |
| `synonym_substitution` | `on_verb` |
| `table_column_synonyms` | `on_table_reference`, `on_column_reference` |
| `ambiguous_pronouns` | `on_table_reference`, `on_column_reference` |
| `temporal_expression` | `on_temporal` |

---

## Implementation Steps

### Step 1: Add hook methods to `PerturbationStrategy` base class

Add 8 default-pass-through hook methods to `src/perturbations/base.py`.
Each returns the `default` argument unchanged.  Subclasses override only
the hooks they need.

```python
# New methods on PerturbationStrategy:
def on_keyword(self, keyword: str, default: str) -> str:
    return default

def on_join(self, join_node, table_nl: str, on_str: str,
            default_phrase: str, context: dict) -> str:
    return default_phrase

def on_operator(self, op_key: str, left: str, right: str,
                default_str: str, is_temporal: bool,
                context: dict) -> str:
    return default_str

def on_aggregate(self, agg_key: str, inner_text: str,
                 default_str: str) -> str:
    return default_str

def on_verb(self, key: str, baseline: str, rng: random.Random) -> str:
    return baseline

def on_table_reference(self, table_name: str, default: str,
                       is_repeated: bool) -> str:
    return default

def on_column_reference(self, col_name: str, table: str,
                        default: str, is_repeated: bool) -> str:
    return default

def on_temporal(self, raw_value: str, default: str,
                rng: random.Random) -> str:
    return default
```

### Step 2: Update `SQLToNLRenderer` to accept a strategy

Change the renderer constructor and `render()` method:

- **Remove** `config: PerturbationConfig` parameter
- **Add** `strategy: Optional[PerturbationStrategy] = None` parameter
- **Store** `self._active_strategy` (or a no-op default)
- **Remove** all `self.config.is_active(...)` calls
- **Replace** each with the appropriate hook call

Create a private `_NullStrategy` class (or use a sentinel) so the renderer
doesn't need `if self._active_strategy:` guards everywhere:

```python
class _NullStrategy:
    """Default no-op strategy — all hooks return the default value."""
    def on_keyword(self, kw, default): return default
    def on_join(self, ...): return default_phrase
    # ... etc.
```

### Step 3: Refactor each `is_active()` call site in the renderer

For each call site, replace the `if is_active(TYPE)` branch with a hook call
that passes both the "raw" and "default" forms, letting the strategy choose.

**Example — render_select verb**:

Before:
```python
if self.config.is_active(PerturbationType.MIXED_SQL_NL):
    parts.append("SELECT")
else:
    parts.append(intent_verb)
```

After:
```python
parts.append(self._strategy.on_keyword("SELECT", intent_verb))
```

**Example — _render_join**:

Before:
```python
if self.config.is_active(PerturbationType.INCOMPLETE_JOIN_SPEC):
    if has_fk:
        return f"and their {table}", right_table_name
    return f"{choice_incomplete} {table}", right_table_name

# ... full join rendering with ON clause ...
return f"joined with {table}{on_str}", right_table_name
```

After:
```python
# Always compute the full default rendering
default_phrase = f"joined with {table}{on_str}"  # (or LEFT JOIN etc.)
final = self._strategy.on_join(join_node, table, on_str, default_phrase,
                               {"left_table": left_table_name,
                                "right_table": right_table_name,
                                "has_fk": has_fk,
                                "choice_incomplete": choice_incomplete})
return final, right_table_name
```

### Step 4: Move perturbation logic into strategy hook overrides

Migrate each `is_active()` block into the corresponding strategy's hook
override.

**Example — mixed_sql_nl.py**:

Before:
```python
def apply(self, nl_text, ast, rng, context):
    seed = context.get("seed", 42)
    config = PerturbationConfig(active_perturbations={PerturbationType.MIXED_SQL_NL}, seed=seed)
    return SQLToNLRenderer(config, schema_config=context.get("schema_config")).render(ast)
```

After:
```python
def on_keyword(self, keyword, default):
    return keyword  # Emit raw SQL keywords

def apply(self, nl_text, ast, rng, context):
    seed = context.get("seed", 42)
    renderer = SQLToNLRenderer(seed=seed, schema_config=context.get("schema_config"))
    return renderer.render(ast, strategy=self)
```

### Step 5: Move `is_applicable` logic into strategy classes

Currently 8 render-time strategies delegate `is_applicable` to
`renderer.is_applicable(ast, PerturbationType.X)`, which is a large
switch-case in the renderer.

Move each branch into the corresponding strategy's `is_applicable()` method.
Then delete `SQLToNLRenderer.is_applicable()`.

**Example — incomplete_join.py**:

Before:
```python
def is_applicable(self, ast, nl_text, context):
    return SQLToNLRenderer().is_applicable(ast, PerturbationType.INCOMPLETE_JOIN_SPEC)
```

After:
```python
def is_applicable(self, ast, nl_text, context):
    return bool(ast.find(exp.Join))
```

### Step 6: Delete `PerturbationType` enum and `PerturbationConfig`

After all strategies own their logic:
- Delete `class PerturbationType(Enum)`
- Delete `class PerturbationConfig`
- Update `SQLToNLRenderer.__init__` to take `seed: int = 42` directly
- Update `render_template()` to not create a `PerturbationConfig`

### Step 7: Update callers

Update all files that import or reference the removed classes:
- `02_generate_nl_prompts.py` — creates `SQLToNLRenderer(config)` → change
  to `SQLToNLRenderer(seed=seed, schema_config=cfg)`
- `03_generate_systematic_perturbations.py` — no change needed (strategies
  handle renderer creation internally)
- Any test files that construct `PerturbationConfig` directly

### Step 8: Handle the `_choose_word` / RNG sync concern

**Critical**: The current renderer uses "ALWAYS roll" patterns — it consumes
RNG values even when a perturbation isn't active to keep the random sequence
deterministic.  Example:

```python
# ALWAYS roll for incomplete join spec to keep sequence in sync
choice_incomplete = self._rng.choice(['with', 'along with'])

if self.config.is_active(PerturbationType.INCOMPLETE_JOIN_SPEC):
    ...  # use choice_incomplete
# else: choice_incomplete is thrown away, but RNG advanced
```

After refactoring, the renderer must still consume the same RNG values in the
same order regardless of which strategy is active.  The hook approach
naturally handles this: the renderer always computes `default` (consuming RNG
as it goes) and passes it to the hook.  The hook can return something
different, but the RNG sequence stays in sync.

**Verify**: After implementation, run the baseline (no perturbation) rendering
and confirm the output is identical to the pre-refactor output for the same
seed.  The test suite checks this.

### Step 9: Handle ambiguous pronouns state

`AMBIGUOUS_PRONOUNS` is the most complex render-time perturbation because it
requires **stateful tracking** across the AST traversal:
- `self._mentions`: set of (type, name) pairs already mentioned
- `self._recent_mentions`: ordered list for former/latter logic
- `self._ambig_pronoun_count`: cap at 1 pronoun per query
- `self._use_pronouns`: global flag
- `self._is_self_join`: blocks pronouns for self-joins

**Approach**: The renderer continues to track mentions (it must — it controls
traversal order).  The hook receives mention metadata:

```python
# In _render_table:
# ... mention tracking logic stays in renderer ...
result = self._strategy.on_table_reference(
    table_name=table_name,
    default=table_name,  # or synonym if TABLE_COLUMN_SYNONYMS is active
    is_repeated=(entity_key in self._mentions),
    mention_count=self._ambig_pronoun_count,
    is_self_join=self._is_self_join,
    pronoun_options=pronoun_options,
    rng=self._rng,
)
```

The `AmbiguousPronounsPerturbation` strategy:

```python
def on_table_reference(self, table_name, default, is_repeated,
                       mention_count, is_self_join, pronoun_options, rng):
    if is_repeated and not is_self_join and mention_count == 0:
        roll = rng.random()  # This must be pre-consumed by renderer
        if roll < 0.6:
            return rng.choice(pronoun_options)
    return default
```

**BUT**: The RNG roll and pronoun choice are currently consumed by the
renderer regardless of whether AMBIGUOUS_PRONOUNS is active (the "ALWAYS
roll" pattern).  We need to preserve this.  Solution: the renderer always
rolls, always picks a pronoun, and passes both to the hook.  The hook just
decides whether to *use* the pronoun or the default.

```python
# Renderer always does:
roll = self._rng.random()
pronoun = self._rng.choice(pronoun_options)

# Then asks strategy:
result = self._strategy.on_table_reference(
    table_name=table_name,
    default=table_name,
    is_repeated=(entity_key in self._mentions),
    pronoun=pronoun,
    use_pronoun=(roll < 0.6),
    can_pronoun=(self._ambig_pronoun_count == 0 and not self._is_self_join),
)
```

The strategy:
```python
def on_table_reference(self, table_name, default, is_repeated,
                       pronoun, use_pronoun, can_pronoun):
    if is_repeated and can_pronoun and use_pronoun:
        return pronoun
    return default
```

This keeps RNG sync while moving the decision to the strategy.

---

## Files Changed Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `src/perturbations/base.py` | EDIT | Add 8 hook methods with default implementations |
| `src/core/nl_renderer.py` | EDIT | Remove enum+config, add `_NullStrategy`, replace all `is_active()` with hook calls, delete `is_applicable()` method, update constructor signature |
| `src/perturbations/mixed_sql_nl.py` | EDIT | Add `on_keyword()` override, own `is_applicable()`, update `apply()` |
| `src/perturbations/omit_obvious.py` | EDIT | Add `on_keyword()` + `on_column_reference()` overrides, own `is_applicable()`, update `apply()` |
| `src/perturbations/incomplete_join.py` | EDIT | Add `on_join()` override, own `is_applicable()`, update `apply()` |
| `src/perturbations/operator_aggregate_variation.py` | EDIT | Add `on_operator()` + `on_aggregate()` overrides, own `is_applicable()`, update `apply()` |
| `src/perturbations/synonym_substitution.py` | EDIT | Add `on_verb()` override, own `is_applicable()`, update `apply()` |
| `src/perturbations/table_column_synonyms.py` | EDIT | Add `on_table_reference()` + `on_column_reference()` overrides, own `is_applicable()`, update `apply()` |
| `src/perturbations/ambiguous_pronouns.py` | EDIT | Add `on_table_reference()` + `on_column_reference()` overrides, own `is_applicable()`, update `apply()` |
| `src/perturbations/temporal_expression.py` | EDIT | Add `on_temporal()` override, own `is_applicable()`, update `apply()` |
| `02_generate_nl_prompts.py` | EDIT | Update renderer construction (remove PerturbationConfig import) |
| Post-processing strategies (5) | NO CHANGE | `comment_annotations`, `punctuation`, `typos`, `urgency`, `verbosity` are unaffected |

---

## Validation Plan

1. **Baseline output parity**: Generate NL prompts for social_media and bank
   with no perturbation active.  Output must be byte-identical to
   pre-refactor output for the same seed.

2. **Perturbation output parity**: Run `03_generate_systematic_perturbations.py`
   for both schemas.  All 13 strategies must still be discovered, applicable
   counts must match, and perturbed outputs must be identical.

3. **Test suite**: `test_sql_generation.py --schema social_media.yaml`
   (5958/5958) and `cross_schema_test.py schemas/bank.yaml` (1891+/1891+).

4. **Extensibility smoke test**: Create a trivial test perturbation (e.g.,
   "SHOUT" that uppercases all keywords) to verify zero-touchpoint addition.

---

## Risk Mitigation

- **RNG desync**: The biggest risk.  The "ALWAYS roll" pattern must be
  preserved exactly.  Strategy: compute all RNG values in the renderer
  regardless of active strategy, pass pre-computed values to hooks.

- **Regression**: Full test suite run after each step.  Git commit is at a
  known-good state for revert.

- **Complexity**: The hook signatures must be stable.  If a hook needs extra
  context later, add optional `**kwargs` parameters to the base class to
  avoid breaking existing strategies.
