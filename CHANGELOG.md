# Changelog

All notable changes to DatasetsEx will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-08

### Added

- **Lineage module** (`DatasetsEx.Lineage`) - LineageIR integration for dataset provenance
  - `artifact_ref/2` - Build `LineageIR.ArtifactRef` structs from datasets
  - `edge/3` - Build `LineageIR.ProvenanceEdge` structs between datasets or artifact refs
  - Delegates `artifact_ref/2` and `lineage_edge/3` from main `DatasetsEx` module
- **`artifact_id` field** on `Dataset` struct with auto-generated UUIDs via Ecto
  - `Dataset.ensure_artifact_id/1` helper for lazy assignment
- Lineage artifact reference example in `examples/usage.exs`
- Lineage test suite (`test/datasets_ex/lineage_test.exs`)

### Changed

- **Dependencies**
  - Added `lineage_ir` (path dependency) for artifact ref and provenance edge structs
  - Added `ecto` ~> 3.11 for UUID generation
  - Upgraded `ex_doc` from ~> 0.31 to ~> 0.40.0
- **Refactored modules for idiomatic Elixir**
  - **Quality**: Extracted `duplicate_key/3`, `accumulate_duplicates/2`, `duplicate_entry/1` private helpers; fixed `median/1` guard clause
  - **Transform**: Extracted `maybe_downcase/2`, `maybe_trim/2`, `maybe_normalize_whitespace/2`, `swap_adjacent/2`, `delete_char/2`, `duplicate_char/1`, `undersample_groups/1`, `oversample_groups/1` private helpers
  - **Stream**: Extracted `stream_csv_with_headers/1` and `stream_csv_row/2` private helpers
  - **Export**: Replaced `Enum.map |> Enum.join` with `Enum.map_join` in CSV writer
  - **Loader**: Simplified `load/3` with clause chaining; replaced `length(list) > 0` with `not Enum.empty?`
- **Tests**: Used `Enum.map_join` and `refute Enum.empty?` for idiomatic assertions
- **ExDoc config**: Added `groups_for_extras` and `groups_for_modules` for organized hexdocs
- **Versioning**: Calls `ensure_artifact_id/1` on create and load for consistent artifact tracking
- `.gitignore`: Added `priv/datasets/` to ignore cached dataset files

## [0.1.0] - 2025-12-06

### Added
- Initial release
- **Dataset** struct for managing ML datasets
- **Loader** for loading datasets from various sources
- **Registry** for dataset catalog management
- **Splitter** for train/test/validation splits
- **Versioning** for dataset version management
- **Quality** for schema validation and profiling
- **Transform** for text preprocessing and augmentation
- **Stream** for memory-efficient processing
- **Export** for JSONL/JSON/CSV export
- Built-in loaders for SciFact and FEVER datasets
