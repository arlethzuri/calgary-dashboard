# Ontology Standardization Next Steps

Goal: bring `ontology` to the same standard as `catalog`, `cleaners`, `collectors`, `common`, and `config` by removing hardcoded paths, centralizing shared logic, and making modules import-safe/reusable.

## 1) Define Ontology Module Boundaries

- Split ontology code by concern (avoid script-style all-in-one files):
  - `ontology/io.py` for reading/writing ontology and mapping JSON assets
  - `ontology/mapping.py` for class/file mapping transforms
  - `ontology/build.py` for orchestration and pipeline flow
- Keep data files (`calgary-ontology.json`, `class_file_map.json`, etc.) as source-of-truth artifacts only.

## 2) Replace Hardcoded Paths with `config.paths`

- Route all ontology path resolution through shared config values:
  - `PROJECT_ROOT`, `RAW_DATA_ROOT`, `PROCESSED_DATA_ROOT`
- If ontology outputs/inputs need dedicated folders, add explicit constants in `config.paths` (for example `ONTOLOGY_ROOT`) instead of using local `Path(__file__)` math everywhere.

## 3) Reuse `common.io` for JSON and Filesystem Ops

- Replace direct JSON file open/write calls with:
  - `read_json`
  - `write_json`
  - `ensure_dir`
- If pretty/sorted JSON output is needed repeatedly, extend `common.io` once and reuse from ontology and catalog.

## 4) Centralize Shared Constants in `common.definitions`

- Move constants used across ontology/KG/dashboard into `common.definitions`.
- Keep ontology-only constants local unless they are reused elsewhere.
- Use one canonical geometry/source vocabulary across modules to prevent drift.

## 5) Standardize Snapshot/Date Handling

- Reuse helpers from:
  - `common.cleaning` (`is_snapshot_name`, `resolve_snapshot`, etc.)
  - `common.dates` (`snapshot_date`)
- Remove duplicate snapshot parsing and date formatting logic from ontology scripts.

## 6) Standardize Logging and CLI Interface

- Use `config.logging.configure_logger` for ontology jobs.
- Convert script entrypoints to cleaner-style CLI (`argparse`) with consistent flags:
  - `--snapshot-date`
  - `--in-path`
  - `--out-path`
- Keep module imports side-effect free; only run logic inside `main()`.

## 7) Add Lightweight Tests for Refactor Safety

- Add focused tests around ontology contracts:
  - mapping determinism
  - required JSON schema fields
  - expected behavior when files/snapshots are missing
- Keep tests small but sufficient to prevent regressions during standardization.

## 8) Align KG to Use Ontology Interfaces

- Update KG modules (e.g. graph build/load flows) to consume ontology functions, not ad hoc raw file parsing.
- Treat ontology outputs as a stable API/contract layer.

## Recommended Execution Order

1. Refactor one ontology script end-to-end into module + CLI + shared paths/logging.
2. Extract any duplicated utilities found during that refactor into `common`.
3. Apply the same pattern to remaining ontology scripts.
4. Add tests for the first refactored flow, then expand coverage.
5. Update KG callers to use the new ontology interfaces.

## Done Criteria

- No hardcoded absolute paths in active ontology code.
- Ontology entrypoints use `config.paths`, `common.io`, `config.logging`.
- Shared constants and snapshot logic are centralized.
- Ontology modules are import-safe and callable from other packages.
- Basic tests cover core ontology mapping/build behavior.

