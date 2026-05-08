# Ontology Package Boundaries (Step 1)

This folder defines the target module boundaries for ontology refactoring.

## Modules

- `io.py`
  - JSON/file operations and standardized path resolution.
- `mapping.py`
  - Pure mapping/transformation logic.
- `build.py`
  - Pipeline orchestration + CLI entrypoint.

## Migration Plan

1. Keep `ontology_old` untouched.
2. Move pure mapping functions from `ontology_old/generate_mapping_first_pass.py` into `mapping.py`.
3. Move file/path operations into `io.py` using `common.io` and `config.paths`.
4. Implement orchestration in `build.py`, then add tests.

## Notes

- Current implementations are intentional contracts/stubs for safe incremental migration.
- Any module raising `NotImplementedError` indicates a boundary is defined but migration is pending.

