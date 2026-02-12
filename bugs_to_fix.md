### 1. The "Grammar Salad" Bug

**Affected Perturbation:** `temporal_expression_variation`
**The Issue:** The renderer naively concatenates the perturbed operator phrase (e.g., "greater than or equal to") with the original temporal phrase (e.g., "within the last X days"), resulting in nonsensical English.

- **Evidence (ID 2):**
- **SQL:** `...posted_at >= DATETIME('now', '-22 days')` (Recent)
- **Perturbed NL:** "...posted_at **greater than or equal to within the last 22 days**"
- **Critique:** "Greater than within" is not valid English.

- **Evidence (ID 42):**
- **Perturbed NL:** "...signup_date **less than or equal to within the last 27 days**"

---

### 2. The Semantic Inversion Bug

**Affected Perturbation:** `operator_aggregate_variation`
**The Issue:** The perturbation substitutes mathematical operators with natural language phrases that have the **opposite** temporal meaning.

- **SQL Logic:** `> -30 days` means a date _closer to the present_ (Recent).
- **NL Logic:** "Exceeds 30 days" typically means a duration _longer_ than 30 days (Old).
- **Evidence (ID 8):**
- **SQL:** `...posted_at > DATETIME('now', '-30 days')` (Recent posts)
- **Perturbed NL:** "...posted_at **exceeds 30 days ago**"
- **Critique:** "Exceeds 30 days ago" implies the post is _older_ than 30 days. The prompt asks for old items, but the SQL returns recent items.

- **Evidence (ID 2):**
- **Perturbed NL:** "...posted_at **no less than 22 days ago**"
- **Critique:** "No less than 22 days" implies a duration of >= 22 days (Old). The SQL (`>= -22 days`) asks for items _within_ the last 22 days (Recent).

---

### 3. The "Russian Roulette" Reference Bug

**Affected Perturbation:** `anchored_pronoun_references`
**The Issue:** The perturbation replaces a specific column name with a generic placeholder like "the aforementioned column" or "it". However, it does this even when **multiple** columns have been mentioned previously, making it impossible to determine which column is being referenced.

- **Evidence (ID 35):**
- **Context:** The prompt lists `comment's id`, `comment's comment_text`, `comment's post_id`, and `post's user_id`.
- **Perturbed NL:** "...posts FULL JOIN comments on **the aforementioned column** equals the comment's post_id"
- **Critique:** Which of the four previously mentioned columns is "the aforementioned column"? It is statistically impossible to guess.

- **Evidence (ID 12):**
- **Context:** `follower_id` and `followee_id` are both selected.
- **Perturbed NL:** "...ordered by the **aforementioned column**"

---
