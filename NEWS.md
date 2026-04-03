# calcofi4db 2.5.2

*VIEWs for dependencies, GCS server-side copy, crc32c sync & spatial consolidation*

- **VIEW-based dependency loading** `load_prior_tables()` gains `as_view` parameter — creates VIEWs instead of TABLEs for zero-copy parquet reads. Dependency tables no longer duplicated across ingests.
- **`calcofi.modifies` frontmatter** New YAML field declares which dependency tables an ingest modifies (e.g., `ship`). `parse_qmd_frontmatter()` parses it; `build_release_table_registry()` discovers `_new` delta sidecars from the filesystem.
- **GCS server-side copy for releases** `release_database.qmd` copies parquet from `ingest/` to `releases/` on GCS via `gcloud storage cp` instead of re-uploading from local. Only derived/merged tables exported locally.
- **crc32c hash comparison** `sync_to_gcs()` uses `gcloud storage ls --json` for crc32c hashes; `list_gcs_files()` returns `crc32c` column. Unchanged files skipped entirely.
- **Stale file cleanup** `sync_to_gcs()` gains `delete_stale` parameter to remove orphaned GCS files after partition key or table renames.
- **`export_parquet()`** New helper using DuckDB native `COPY TO PARQUET` — handles GEOMETRY columns (as WKB), preferred over `arrow::write_parquet()`.
- **`build_release_table_registry()`** Auto-discovers table-to-ingest mapping from manifests with canonical source marking for duplicates.
- **Archive listing fix** `get_latest_archive_timestamp()` uses non-recursive `gcloud storage ls` instead of recursive `--json` scan that was hanging on large archives.

# calcofi4db 2.5.1

*Mismatch tracking, supplemental table support, targets integration & bug fix*

- **New mismatch collectors** Added `collect_ship_mismatches()`, `collect_measurement_type_mismatches()`, and `collect_cruise_key_mismatches()` to detect unresolved entities and populate `manifest.json` mismatches section.
- **Supplemental table support** `write_parquet_outputs()` gains `mismatches` and `supplemental` parameters; `load_prior_tables()` and `finalize_ingest()` gain `include_supplemental` to exclude supplemental tables (e.g. wide-format ERDDAP outputs) by default.
- **New spatial manifest** Added `write_spatial_manifest()` to generate `manifest.json` for spatial parquet directories.
- **New ship helper** Added `ensure_interim_ships()` to insert placeholder ship entries for unmatched codes so downstream FK joins can proceed.
- **Targets integration** Added `parse_qmd_frontmatter()` and `build_targets_list()` to build a `targets` pipeline from `calcofi:` YAML frontmatter in `.qmd` workflow files. Added `yaml` to Imports and `targets` to Suggests.
- **Relationships refactor** `build_relationships_json()` now accepts a `rels` list as an alternative to a `dm` object, removing the hard dependency on the `dm` package.
- **Partition change detection** `write_parquet_outputs()` now detects when partition values change and forces a re-write.
- **Bug fix** Fixed `print_csv_change_stats()` using `fields_added` instead of `fields_removed` when counting removed fields.

# calcofi4db 2.5.0

*Simplified provider/dataset naming, taxonomy & workflow improvements*

- **Dataset renaming** Renamed dataset providers from URL-style to short names (e.g., `swfsc.noaa.gov/calcofi-db` -> `swfsc/ichthyo`, `calcofi.org/bottle-database` -> `calcofi/bottle`); moved corresponding `inst/ingest/` config files to match.
- **New taxonomy functions** Added `standardize_species_local()` for fast local species standardization via `spp.duckdb` with optional WoRMS API fallback. Added `build_taxon_hierarchy()` to build taxonomic hierarchies from local `spp.duckdb` using recursive CTEs.
- **New workflow function** Added `finalize_ingest()` high-level function to push parquet tables to Working DuckLake with provenance tracking.
- **New cloud helpers** Added GCS cleanup helpers: `delete_gcs_prefix()`, `cleanup_gcs_obsolete()`.
- **New display helper** Added `dt()` display helper for interactive DataTables with CSV export.
- **New wrangle helpers** Added relationship JSON helpers: `build_relationships_json()`, `merge_relationships_json()`, `read_relationships_json()`. Added `assign_deterministic_uuids_md5()` using DuckDB-native md5.
- **Improved `sync_to_gcs()`** to support recursive/hive-partitioned subdirectories.

# calcofi4db 2.4.0

*Use _uuid over _id, smarter sync with GCS*

* Revert from int `_id` to `_uuid` preferred unique identifiers for SWFSC icthyo db
* Use smarter synchronizing with GCS using md5 hash checks and modified time filenaming

# calcofi4db 2.3.0

*Addition of ship, taxonomy functions*

Added helper functions for processing:

- ships: `fetch_ship_ices()`, `match_ships()`, `add_ship_info()`.
- taxonomy: `build_taxon_table()`, `standardize_species()`

# calcofi4db 2.2.1

*Addition of spatial, parquet, viz helper functions*

- Added functions to help with spatial data processing including: `add_point_geom()`, `assign_grid_key()`.
- Added parquet helper function: `load_gcs_parquet_to_duckdb()`.
- Added ingest workflow helper visualzation of table function: `preview_tables()`.

# calcofi4db 2.2.0

*Improvements to cloud plan functions*

Workflow ingest_swfsc.noaa.gov_calcofi-db.qmd now fully automates ingestion of 
CalCOFI database from SWFSC NOAA archive to parquet files in Google Cloud Storage.
Many new functions added.

# calcofi4db 2.1.0

*Addition of functions for phase 2 of cloud plan*

- Added ducklake and freeze functions. Updated documentation with concepts.

# calcofi4db 1.2.0

*Addition of functions for phase 1 of cloud plan*

# calcofi4db 1.1.0

*Addition of CalCOFI Bottle Database*

# calcofi4db 1.0.0

*Initial production release with NOAA CalCOFI Database*

* Complete NOAA CalCOFI Database ingestion with spatial features
* Add synchronized versioning system for package and database
* Create master ingestion workflow with integrity checks
* Implement comprehensive metadata management

# calcofi4db 0.1.1

* Fix `detect_csv_changes()` to compare CSV files with `read_csv_files()` output.
  * Add type mismatch checks for fields in the CSV files.
* Add `print_csv_change_stats()` functions for textual summary of changes.
* Add `display_csv_changes()` to display changes in a color-coded table and 
    * Ensure compatibility with multiple output formats: interactive DataTable, static kable, or raw tibble.
* Expand documentation for `read_csv_files()` and `detect_csv_changes()`.

