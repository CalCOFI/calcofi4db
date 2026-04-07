# Changelog

## calcofi4db 2.5.4

*Consolidated
[`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
with archive mode, exclude patterns & GCS logging*

- **Unified sync function**
  [`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
  gains `archive`, `exclude`, and `log_to_gcs` parameters. When
  `archive = TRUE`, creates timestamped immutable snapshots (replacing
  [`sync_to_gcs_archive()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs_archive.md)
  internals). When `FALSE` (default), standard mirror mode.
- **Exclude patterns** New `exclude` parameter accepts glob patterns
  (e.g., `c(".DS_Store", "*.tmp")`) to skip files during sync.
- **GCS action logging** `log_to_gcs = TRUE` writes a timestamped JSON
  log to `gs://{bucket}/{prefix}/_logs/sync_YYYY-MM-DD_HHMMSS.json`
  documenting every upload, skip, and delete.
- **Richer results** Sync results tibble now includes `size` and
  `reason` columns (e.g., “checksum match”, “new file”, “crc32c
  changed”).
- **[`sync_to_gcs_archive()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs_archive.md)
  deprecated** Now a thin wrapper calling `sync_to_gcs(archive = TRUE)`.
  Existing callers work unchanged.

## calcofi4db 2.5.3

*DuckDB driver lifecycle, idempotent ingestion & defensive ALTER TABLE*

- **DuckDB driver lifecycle**
  [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md)
  now creates a named driver via `duckdb::duckdb(dbdir=...)` and stores
  it as an attribute;
  [`close_duckdb()`](https://calcofi.io/calcofi4db/reference/close_duckdb.md)
  calls `duckdb_shutdown()` for proper WAL flush. Also sets
  `autoload_known_extensions = "true"` so the spatial extension loads
  during WAL replay.
- **Idempotent DuckLake ingestion**
  [`ingest_to_working()`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md)
  checks `_source_file` before appending — skips if rows from the same
  source already exist, making notebook re-renders safe.
- **Defensive `ADD COLUMN IF NOT EXISTS`** All
  `ALTER TABLE … ADD COLUMN` calls across
  [`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md),
  [`load_gcs_parquet_to_duckdb()`](https://calcofi.io/calcofi4db/reference/load_gcs_parquet_to_duckdb.md),
  [`standardize_species_local()`](https://calcofi.io/calcofi4db/reference/standardize_species_local.md),
  [`standardize_species()`](https://calcofi.io/calcofi4db/reference/standardize_species.md),
  [`finalize_ingest()`](https://calcofi.io/calcofi4db/reference/finalize_ingest.md),
  [`create_cruise_key()`](https://calcofi.io/calcofi4db/reference/create_cruise_key.md),
  [`propagate_natural_key()`](https://calcofi.io/calcofi4db/reference/propagate_natural_key.md),
  [`assign_sequential_ids()`](https://calcofi.io/calcofi4db/reference/assign_sequential_ids.md),
  and
  [`replace_uuid_with_id()`](https://calcofi.io/calcofi4db/reference/replace_uuid_with_id.md)
  now use `IF NOT EXISTS` to prevent errors on re-runs.
- **Better duplicate-key warnings**
  [`create_cruise_key()`](https://calcofi.io/calcofi4db/reference/create_cruise_key.md)
  now shows top-10 examples with counts in the warning message.

## calcofi4db 2.5.2

*VIEWs for dependencies, GCS server-side copy, crc32c sync & spatial
consolidation*

- **VIEW-based dependency loading**
  [`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md)
  gains `as_view` parameter — creates VIEWs instead of TABLEs for
  zero-copy parquet reads. Dependency tables no longer duplicated across
  ingests.
- **`calcofi.modifies` frontmatter** New YAML field declares which
  dependency tables an ingest modifies (e.g., `ship`).
  [`parse_qmd_frontmatter()`](https://calcofi.io/calcofi4db/reference/parse_qmd_frontmatter.md)
  parses it;
  [`build_release_table_registry()`](https://calcofi.io/calcofi4db/reference/build_release_table_registry.md)
  discovers `_new` delta sidecars from the filesystem.
- **GCS server-side copy for releases** `release_database.qmd` copies
  parquet from `ingest/` to `releases/` on GCS via `gcloud storage cp`
  instead of re-uploading from local. Only derived/merged tables
  exported locally.
- **crc32c hash comparison**
  [`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
  uses `gcloud storage ls --json` for crc32c hashes;
  [`list_gcs_files()`](https://calcofi.io/calcofi4db/reference/list_gcs_files.md)
  returns `crc32c` column. Unchanged files skipped entirely.
- **Stale file cleanup**
  [`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
  gains `delete_stale` parameter to remove orphaned GCS files after
  partition key or table renames.
- **[`export_parquet()`](https://calcofi.io/calcofi4db/reference/export_parquet.md)**
  New helper using DuckDB native `COPY TO PARQUET` — handles GEOMETRY
  columns (as WKB), preferred over
  [`arrow::write_parquet()`](https://arrow.apache.org/docs/r/reference/write_parquet.html).
- **[`build_release_table_registry()`](https://calcofi.io/calcofi4db/reference/build_release_table_registry.md)**
  Auto-discovers table-to-ingest mapping from manifests with canonical
  source marking for duplicates.
- **Archive listing fix**
  [`get_latest_archive_timestamp()`](https://calcofi.io/calcofi4db/reference/get_latest_archive_timestamp.md)
  uses non-recursive `gcloud storage ls` instead of recursive `--json`
  scan that was hanging on large archives.

## calcofi4db 2.5.1

*Mismatch tracking, supplemental table support, targets integration &
bug fix*

- **New mismatch collectors** Added
  [`collect_ship_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_ship_mismatches.md),
  [`collect_measurement_type_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_measurement_type_mismatches.md),
  and
  [`collect_cruise_key_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_cruise_key_mismatches.md)
  to detect unresolved entities and populate `manifest.json` mismatches
  section.
- **Supplemental table support**
  [`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md)
  gains `mismatches` and `supplemental` parameters;
  [`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md)
  and
  [`finalize_ingest()`](https://calcofi.io/calcofi4db/reference/finalize_ingest.md)
  gain `include_supplemental` to exclude supplemental tables
  (e.g. wide-format ERDDAP outputs) by default.
- **New spatial manifest** Added
  [`write_spatial_manifest()`](https://calcofi.io/calcofi4db/reference/write_spatial_manifest.md)
  to generate `manifest.json` for spatial parquet directories.
- **New ship helper** Added
  [`ensure_interim_ships()`](https://calcofi.io/calcofi4db/reference/ensure_interim_ships.md)
  to insert placeholder ship entries for unmatched codes so downstream
  FK joins can proceed.
- **Targets integration** Added
  [`parse_qmd_frontmatter()`](https://calcofi.io/calcofi4db/reference/parse_qmd_frontmatter.md)
  and
  [`build_targets_list()`](https://calcofi.io/calcofi4db/reference/build_targets_list.md)
  to build a `targets` pipeline from `calcofi:` YAML frontmatter in
  `.qmd` workflow files. Added `yaml` to Imports and `targets` to
  Suggests.
- **Relationships refactor**
  [`build_relationships_json()`](https://calcofi.io/calcofi4db/reference/build_relationships_json.md)
  now accepts a `rels` list as an alternative to a `dm` object, removing
  the hard dependency on the `dm` package.
- **Partition change detection**
  [`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md)
  now detects when partition values change and forces a re-write.
- **Bug fix** Fixed
  [`print_csv_change_stats()`](https://calcofi.io/calcofi4db/reference/print_csv_change_stats.md)
  using `fields_added` instead of `fields_removed` when counting removed
  fields.

## calcofi4db 2.5.0

*Simplified provider/dataset naming, taxonomy & workflow improvements*

- **Dataset renaming** Renamed dataset providers from URL-style to short
  names (e.g., `swfsc.noaa.gov/calcofi-db` -\> `swfsc/ichthyo`,
  `calcofi.org/bottle-database` -\> `calcofi/bottle`); moved
  corresponding `inst/ingest/` config files to match.
- **New taxonomy functions** Added
  [`standardize_species_local()`](https://calcofi.io/calcofi4db/reference/standardize_species_local.md)
  for fast local species standardization via `spp.duckdb` with optional
  WoRMS API fallback. Added
  [`build_taxon_hierarchy()`](https://calcofi.io/calcofi4db/reference/build_taxon_hierarchy.md)
  to build taxonomic hierarchies from local `spp.duckdb` using recursive
  CTEs.
- **New workflow function** Added
  [`finalize_ingest()`](https://calcofi.io/calcofi4db/reference/finalize_ingest.md)
  high-level function to push parquet tables to Working DuckLake with
  provenance tracking.
- **New cloud helpers** Added GCS cleanup helpers:
  [`delete_gcs_prefix()`](https://calcofi.io/calcofi4db/reference/delete_gcs_prefix.md),
  [`cleanup_gcs_obsolete()`](https://calcofi.io/calcofi4db/reference/cleanup_gcs_obsolete.md).
- **New display helper** Added
  [`dt()`](https://calcofi.io/calcofi4db/reference/dt.md) display helper
  for interactive DataTables with CSV export.
- **New wrangle helpers** Added relationship JSON helpers:
  [`build_relationships_json()`](https://calcofi.io/calcofi4db/reference/build_relationships_json.md),
  [`merge_relationships_json()`](https://calcofi.io/calcofi4db/reference/merge_relationships_json.md),
  [`read_relationships_json()`](https://calcofi.io/calcofi4db/reference/read_relationships_json.md).
  Added
  [`assign_deterministic_uuids_md5()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids_md5.md)
  using DuckDB-native md5.
- **Improved
  [`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)**
  to support recursive/hive-partitioned subdirectories.

## calcofi4db 2.4.0

\*Use \_uuid over \_id, smarter sync with GCS\*

- Revert from int `_id` to `_uuid` preferred unique identifiers for
  SWFSC icthyo db
- Use smarter synchronizing with GCS using md5 hash checks and modified
  time filenaming

## calcofi4db 2.3.0

*Addition of ship, taxonomy functions*

Added helper functions for processing:

- ships:
  [`fetch_ship_ices()`](https://calcofi.io/calcofi4db/reference/fetch_ship_ices.md),
  [`match_ships()`](https://calcofi.io/calcofi4db/reference/match_ships.md),
  `add_ship_info()`.
- taxonomy:
  [`build_taxon_table()`](https://calcofi.io/calcofi4db/reference/build_taxon_table.md),
  [`standardize_species()`](https://calcofi.io/calcofi4db/reference/standardize_species.md)

## calcofi4db 2.2.1

*Addition of spatial, parquet, viz helper functions*

- Added functions to help with spatial data processing including:
  [`add_point_geom()`](https://calcofi.io/calcofi4db/reference/add_point_geom.md),
  [`assign_grid_key()`](https://calcofi.io/calcofi4db/reference/assign_grid_key.md).
- Added parquet helper function:
  [`load_gcs_parquet_to_duckdb()`](https://calcofi.io/calcofi4db/reference/load_gcs_parquet_to_duckdb.md).
- Added ingest workflow helper visualzation of table function:
  [`preview_tables()`](https://calcofi.io/calcofi4db/reference/preview_tables.md).

## calcofi4db 2.2.0

*Improvements to cloud plan functions*

Workflow ingest_swfsc.noaa.gov_calcofi-db.qmd now fully automates
ingestion of CalCOFI database from SWFSC NOAA archive to parquet files
in Google Cloud Storage. Many new functions added.

## calcofi4db 2.1.0

*Addition of functions for phase 2 of cloud plan*

- Added ducklake and freeze functions. Updated documentation with
  concepts.

## calcofi4db 1.2.0

*Addition of functions for phase 1 of cloud plan*

## calcofi4db 1.1.0

*Addition of CalCOFI Bottle Database*

## calcofi4db 1.0.0

*Initial production release with NOAA CalCOFI Database*

- Complete NOAA CalCOFI Database ingestion with spatial features
- Add synchronized versioning system for package and database
- Create master ingestion workflow with integrity checks
- Implement comprehensive metadata management

## calcofi4db 0.1.1

- Fix
  [`detect_csv_changes()`](https://calcofi.io/calcofi4db/reference/detect_csv_changes.md)
  to compare CSV files with
  [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)
  output.
  - Add type mismatch checks for fields in the CSV files.
- Add
  [`print_csv_change_stats()`](https://calcofi.io/calcofi4db/reference/print_csv_change_stats.md)
  functions for textual summary of changes.
- Add
  [`display_csv_changes()`](https://calcofi.io/calcofi4db/reference/display_csv_changes.md)
  to display changes in a color-coded table and
  - Ensure compatibility with multiple output formats: interactive
    DataTable, static kable, or raw tibble.
- Expand documentation for
  [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)
  and
  [`detect_csv_changes()`](https://calcofi.io/calcofi4db/reference/detect_csv_changes.md).
