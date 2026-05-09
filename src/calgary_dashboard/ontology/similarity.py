"""String similarity helpers for ontology file-to-class suggestions."""

from __future__ import annotations

import re
from difflib import SequenceMatcher


def normalize_text(value: str) -> str:
    """Lowercase text and remove non-alphanumeric separators."""
    # This normalization intentionally keeps only letters/numbers so that:
    # - "AirQualityDataNearRealtime" and "air quality data near realtime"
    #   become comparable
    # - punctuation-heavy dataset names do not fragment scoring
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def tokenize(value: str) -> set[str]:
    """Tokenize normalized text into a set of words."""
    normalized = normalize_text(value)
    if not normalized:
        return set()
    return set(normalized.split())


def jaccard_similarity(left: str, right: str) -> float:
    """Compute Jaccard similarity over token sets."""
    # Jaccard focuses on semantic overlap ("energy", "consumption", etc.)
    # and is robust to word order differences.
    left_tokens = tokenize(left)
    right_tokens = tokenize(right)
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens.intersection(right_tokens)
    union = left_tokens.union(right_tokens)
    return len(intersection) / len(union)


def edit_similarity(left: str, right: str) -> float:
    """Compute normalized sequence similarity (1 - edit-like distance proxy)."""
    # SequenceMatcher gives a character-level similarity signal that helps
    # when token overlap is weak but strings are still close variants.
    return SequenceMatcher(None, normalize_text(left), normalize_text(right)).ratio()


def combined_similarity(left: str, right: str) -> float:
    """Blend token-overlap and edit similarity for robust ranking."""
    # Heavier weight on Jaccard keeps ranking semantically grounded;
    # edit similarity still contributes for naming variants/abbreviations.
    jac = jaccard_similarity(left, right)
    edt = edit_similarity(left, right)
    return (0.6 * jac) + (0.4 * edt)

