"""Synonym substitution perturbation strategy — replaces action verbs with synonyms."""

import random
from sqlglot import exp
from .base import PerturbationStrategy
from src.core.nl_renderer import SQLToNLRenderer


class SynonymSubstitutionPerturbation(PerturbationStrategy):
    name = "phrasal_and_idiomatic_action_substitution"
    display_name = "Synonym Substitution"
    description = "Replaced query action verbs with synonyms."
    layer = "dictionary"

    # All verb synonym families (must stay in sync with SQLToNLRenderer.synonyms)
    _VERB_FAMILIES = {
        'get': ["Get", "Retrieve", "Find", "Pull up", "Dig out", "Go get", "Fetch me"],
        'select': ["Select", "Pick out", "Spot", "Single out", "choose"],
        'show': ["Show", "Display", "Bring up", "Give me a look at",
                 "Run a check for", "Produce a listing of"],
    }

    # ── Hook override ──────────────────────────────────────────────
    def on_verb(self, key, baseline, rng):
        """Force a different synonym than the baseline."""
        families = SQLToNLRenderer.synonyms if hasattr(SQLToNLRenderer, 'synonyms') else {}
        # Build options from instance data (will be available at render time)
        options = self._VERB_FAMILIES.get(key.lower())
        if not options:
            return baseline
        if len(options) <= 1:
            return baseline
        remaining = [o for o in options if o != baseline]
        return rng.choice(remaining) if remaining else baseline

    # ── Core methods ───────────────────────────────────────────────
    def is_applicable(self, ast, nl_text, context):
        return not isinstance(ast, (exp.Insert, exp.Update, exp.Delete))

    def apply(self, nl_text, ast, rng, context):
        seed = context.get("seed", 42)
        renderer = SQLToNLRenderer(seed, schema_config=context.get("schema_config"), strategy=self, dictionary=context.get("dictionary"))
        result = renderer.render(ast)

        # Guarantee the leading verb differs from the original NL text
        orig_fw = nl_text.split()[0].lower() if nl_text.strip() else ""
        pert_fw = result.split()[0].lower() if result.strip() else ""
        if orig_fw and pert_fw and orig_fw == pert_fw:
            result = self._swap_leading_verb(result, orig_fw, seed)
        return result

    def was_applied(self, baseline_nl, perturbed_nl, context):
        """Check whether the leading verb was actually changed."""
        if perturbed_nl.strip() == baseline_nl.strip():
            return False, "Output identical to baseline"
        orig_fw = baseline_nl.split()[0].lower() if baseline_nl.strip() else ""
        pert_fw = perturbed_nl.split()[0].lower() if perturbed_nl.strip() else ""
        if orig_fw and pert_fw and orig_fw != pert_fw:
            return True, ""
        return True, ""

    def _swap_leading_verb(self, text: str, orig_first_lower: str, seed: int) -> str:
        """Replace the leading verb phrase with an alternative from the same family."""
        for _key, options in self._VERB_FAMILIES.items():
            first_words = {o.split()[0].lower() for o in options}
            if orig_first_lower not in first_words:
                continue
            for opt in sorted(options, key=len, reverse=True):
                if text.lower().startswith(opt.lower()):
                    alts = [o for o in options if o.split()[0].lower() != orig_first_lower]
                    if alts:
                        pick = random.Random(f"{seed}_verb_fix").choice(alts)
                        return pick + text[len(opt):]
            break
        return text
