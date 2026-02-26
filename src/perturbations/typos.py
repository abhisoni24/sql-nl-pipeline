"""Typos perturbation strategy — introduces realistic keyboard typos."""

from .base import PerturbationStrategy


class TyposPerturbation(PerturbationStrategy):
    name = "typos"
    display_name = "Keyboard Typos"
    description = "Introduce realistic keyboard typos in the NL prompt."
    layer = "post_processing"

    def is_applicable(self, ast, nl_text, context):
        return True  # Typos can be applied to any NL prompt

    def apply(self, nl_text, ast, rng, context):
        """Introduce typos directly on the original NL text.
        Targets ~15% of words (min 1, max 3) by swapping adjacent chars."""
        words = nl_text.split()
        if not words:
            return nl_text
        num_typos = min(3, max(1, int(len(words) * 0.15)))
        candidates = [i for i, w in enumerate(words) if len(w) >= 3]
        if not candidates:
            return nl_text
        targets = rng.sample(candidates, min(len(candidates), num_typos))
        for idx in targets:
            word = words[idx]
            char_idx = rng.randint(0, len(word) - 2)
            chars = list(word)
            chars[char_idx], chars[char_idx + 1] = chars[char_idx + 1], chars[char_idx]
            words[idx] = "".join(chars)
        return " ".join(words)
