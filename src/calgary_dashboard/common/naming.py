"""Filename normalization helpers."""

from __future__ import annotations

import re


def _to_words(text: str) -> list[str]:
    """Convert text to a list of words, removing special characters and spaces."""
    normalized = re.sub(r"[^A-Za-z0-9_ ]", "", text.strip())
    normalized = normalized.replace(" ", "_")
    return [part for part in normalized.split("_") if part]


def to_camel_case(text: str) -> str:
    """Convert text to CamelCase and remove special characters and spaces."""
    words = _to_words(text)
    camel_case_words = [word.title() for word in words]
    return "".join(camel_case_words)


def standardized_file_name(
    dataset_id: str | None,
    dataset_name: str,
    suffix: str,
) -> str:
    """Build standardized names like <id>_<CamelName>_<suffix>.
    dataset_id is optional and will be prepended to the name if provided."""
    camel_name = to_camel_case(dataset_name)
    if dataset_id:
        return f"{dataset_id}_{camel_name}_{suffix}"
    return f"{camel_name}_{suffix}"

