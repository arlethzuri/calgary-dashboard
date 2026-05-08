"""Ontology package boundaries.

This package intentionally separates concerns:
- io: read/write ontology and mapping assets
- mapping: pure mapping logic and transforms
- build: orchestration/entrypoints that coordinate IO + mapping
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .build import OntologyBuildConfig


def __getattr__(name: str):
    if name == "OntologyBuildConfig":
        from .build import OntologyBuildConfig as _OntologyBuildConfig

        return _OntologyBuildConfig
    raise AttributeError(name)


def run_build(config: "OntologyBuildConfig"):
    """Proxy to ontology build runner with lazy import."""
    from .build import run_build as _run_build

    return _run_build(config)


__all__ = ["OntologyBuildConfig", "run_build"]

