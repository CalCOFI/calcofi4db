# Package index

## Cloud Storage (GCS)

functions for Google Cloud Storage operations

- [`cleanup_gcs_obsolete()`](https://calcofi.io/calcofi4db/reference/cleanup_gcs_obsolete.md)
  : Clean up obsolete GCS directories from dataset renames
- [`copy_gcs_file()`](https://calcofi.io/calcofi4db/reference/copy_gcs_file.md)
  : Server-side copy between GCS paths
- [`create_gcs_manifest()`](https://calcofi.io/calcofi4db/reference/create_gcs_manifest.md)
  : Create a manifest of current GCS files
- [`delete_gcs_prefix()`](https://calcofi.io/calcofi4db/reference/delete_gcs_prefix.md)
  : Delete all objects under a GCS prefix
- [`get_calcofi_file()`](https://calcofi.io/calcofi4db/reference/get_calcofi_file.md)
  : Get a CalCOFI file from the immutable archive
- [`get_gcs_file()`](https://calcofi.io/calcofi4db/reference/get_gcs_file.md)
  : Get a file from Google Cloud Storage
- [`get_historical_file()`](https://calcofi.io/calcofi4db/reference/get_historical_file.md)
  : Get historical file from a specific date
- [`get_manifest()`](https://calcofi.io/calcofi4db/reference/get_manifest.md)
  : Get manifest for a specific date
- [`list_calcofi_files()`](https://calcofi.io/calcofi4db/reference/list_calcofi_files.md)
  : List CalCOFI files from manifest
- [`list_gcs_files()`](https://calcofi.io/calcofi4db/reference/list_gcs_files.md)
  : List files in a GCS bucket/prefix
- [`list_gcs_versions()`](https://calcofi.io/calcofi4db/reference/list_gcs_versions.md)
  : List versions of a file in GCS archive
- [`put_gcs_file()`](https://calcofi.io/calcofi4db/reference/put_gcs_file.md)
  : Upload a file to Google Cloud Storage
- [`sync_to_gcs()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs.md)
  : Sync local files to GCS, skipping unchanged files

## Parquet

functions for Apache Parquet file operations

- [`add_parquet_metadata()`](https://calcofi.io/calcofi4db/reference/add_parquet_metadata.md)
  : Add metadata to Parquet file
- [`csv_to_parquet()`](https://calcofi.io/calcofi4db/reference/csv_to_parquet.md)
  : Convert CSV file to Parquet format
- [`export_parquet()`](https://calcofi.io/calcofi4db/reference/export_parquet.md)
  : Export a DuckDB Table or Query to Parquet
- [`get_parquet_metadata()`](https://calcofi.io/calcofi4db/reference/get_parquet_metadata.md)
  : Get Parquet file metadata
- [`read_parquet_table()`](https://calcofi.io/calcofi4db/reference/read_parquet_table.md)
  : Read a Parquet table
- [`upload_parquet()`](https://calcofi.io/calcofi4db/reference/upload_parquet.md)
  : Upload Parquet file to GCS
- [`write_parquet_table()`](https://calcofi.io/calcofi4db/reference/write_parquet_table.md)
  : Write data to Parquet format

## DuckDB

basic DuckDB database operations

- [`close_duckdb()`](https://calcofi.io/calcofi4db/reference/close_duckdb.md)
  : Disconnect from DuckDB and shutdown
- [`create_duckdb_from_parquet()`](https://calcofi.io/calcofi4db/reference/create_duckdb_from_parquet.md)
  : Create DuckDB from Parquet files
- [`create_duckdb_views()`](https://calcofi.io/calcofi4db/reference/create_duckdb_views.md)
  : Create views from a manifest
- [`duckdb_to_parquet()`](https://calcofi.io/calcofi4db/reference/duckdb_to_parquet.md)
  : Export DuckDB table to Parquet
- [`get_duckdb_con()`](https://calcofi.io/calcofi4db/reference/get_duckdb_con.md)
  : Get a DuckDB connection
- [`get_duckdb_tables()`](https://calcofi.io/calcofi4db/reference/get_duckdb_tables.md)
  : Get table information from DuckDB
- [`load_duckdb_extension()`](https://calcofi.io/calcofi4db/reference/load_duckdb_extension.md)
  : Install and load DuckDB extension
- [`save_duckdb_to_gcs()`](https://calcofi.io/calcofi4db/reference/save_duckdb_to_gcs.md)
  : Save DuckDB to GCS
- [`set_duckdb_comments()`](https://calcofi.io/calcofi4db/reference/set_duckdb_comments.md)
  : Set table and column comments in DuckDB

## Working DuckLake

functions for the internal Working DuckLake with provenance tracking

- [`add_provenance_columns()`](https://calcofi.io/calcofi4db/reference/add_provenance_columns.md)
  : Add provenance columns to a data frame
- [`get_working_ducklake()`](https://calcofi.io/calcofi4db/reference/get_working_ducklake.md)
  : Get Working DuckLake connection
- [`ingest_dataset()`](https://calcofi.io/calcofi4db/reference/ingest_dataset.md)
  : Ingest Dataset into Working DuckLake
- [`ingest_to_working()`](https://calcofi.io/calcofi4db/reference/ingest_to_working.md)
  : Ingest data to Working DuckLake
- [`list_working_tables()`](https://calcofi.io/calcofi4db/reference/list_working_tables.md)
  : List tables with provenance in Working DuckLake
- [`load_prior_tables()`](https://calcofi.io/calcofi4db/reference/load_prior_tables.md)
  : Load Tables from a Prior Ingest's Parquet Directory
- [`query_at_time()`](https://calcofi.io/calcofi4db/reference/query_at_time.md)
  : Query Working DuckLake at a point in time
- [`save_working_ducklake()`](https://calcofi.io/calcofi4db/reference/save_working_ducklake.md)
  : Save Working DuckLake to GCS
- [`strip_provenance_columns()`](https://calcofi.io/calcofi4db/reference/strip_provenance_columns.md)
  : Strip provenance columns from data

## Frozen Releases

functions for creating and managing frozen DuckLake releases

- [`compare_releases()`](https://calcofi.io/calcofi4db/reference/compare_releases.md)
  : Compare two frozen releases
- [`freeze_release()`](https://calcofi.io/calcofi4db/reference/freeze_release.md)
  : Freeze a release of the DuckLake
- [`get_release_metadata()`](https://calcofi.io/calcofi4db/reference/get_release_metadata.md)
  : Get metadata for a frozen release
- [`list_frozen_releases()`](https://calcofi.io/calcofi4db/reference/list_frozen_releases.md)
  : List available frozen releases
- [`upload_frozen_release()`](https://calcofi.io/calcofi4db/reference/upload_frozen_release.md)
  : Upload Frozen Release to GCS
- [`validate_for_release()`](https://calcofi.io/calcofi4db/reference/validate_for_release.md)
  : Validate Working DuckLake for release

## Read

functions reading data, particularly from CSV files in the Google Drive
CalCOFI data folder

- [`create_redefinition_files()`](https://calcofi.io/calcofi4db/reference/create_redefinition_files.md)
  : Create Redefinition Files for Tables and Fields
- [`determine_field_types()`](https://calcofi.io/calcofi4db/reference/determine_field_types.md)
  : Determine Field Types for Database
- [`read_csv_files()`](https://calcofi.io/calcofi4db/reference/read_csv_files.md)
  : Read CSV Files and Their Metadata
- [`read_csv_metadata()`](https://calcofi.io/calcofi4db/reference/read_csv_metadata.md)
  : Read CSV Files and Extract Metadata

## Transform

functions to transform data, after reading data and before ingesting
into the database

- [`detect_csv_changes()`](https://calcofi.io/calcofi4db/reference/detect_csv_changes.md)
  : Detect Changes in CSV Files
- [`display_csv_changes()`](https://calcofi.io/calcofi4db/reference/display_csv_changes.md)
  : Display CSV Changes in a Formatted Table
- [`print_csv_change_stats()`](https://calcofi.io/calcofi4db/reference/print_csv_change_stats.md)
  : Print CSV Change Statistics
- [`transform_data()`](https://calcofi.io/calcofi4db/reference/transform_data.md)
  : Transform Data for Database Ingestion

## Check

functions for data validation and integrity checking

- [`check_data_integrity()`](https://calcofi.io/calcofi4db/reference/check_data_integrity.md)
  : Check Data Integrity for Ingestion
- [`check_multiple_datasets()`](https://calcofi.io/calcofi4db/reference/check_multiple_datasets.md)
  : Check Multiple Datasets for Integrity
- [`render_integrity_message()`](https://calcofi.io/calcofi4db/reference/render_integrity_message.md)
  : Render Data Integrity Check Message

## Validate

functions for referential integrity validation and flagging invalid rows

- [`delete_flagged_rows()`](https://calcofi.io/calcofi4db/reference/delete_flagged_rows.md)
  : Delete Flagged Rows from Database
- [`flag_invalid_rows()`](https://calcofi.io/calcofi4db/reference/flag_invalid_rows.md)
  : Flag and Export Invalid Rows
- [`validate_dataset()`](https://calcofi.io/calcofi4db/reference/validate_dataset.md)
  : Run All Validations for a Dataset
- [`validate_egg_stages()`](https://calcofi.io/calcofi4db/reference/validate_egg_stages.md)
  : Validate Egg Stage Values
- [`validate_fk_references()`](https://calcofi.io/calcofi4db/reference/validate_fk_references.md)
  : Validate Foreign Key References
- [`validate_lookup_values()`](https://calcofi.io/calcofi4db/reference/validate_lookup_values.md)
  : Validate Lookup Values Exist

## Ingest

functions for ingesting data into the database (PostgreSQL deprecated,
use DuckLake)

- [`ingest_csv_to_db()`](https://calcofi.io/calcofi4db/reference/ingest_csv_to_db.md)
  **\[deprecated\]** : Ingest CSV data to PostgreSQL database
  (DEPRECATED)
- [`ingest_dataset_pg()`](https://calcofi.io/calcofi4db/reference/ingest_dataset_pg.md)
  **\[deprecated\]** : Ingest a Dataset to PostgreSQL (DEPRECATED)

## Version

functions for schema and package versioning

- [`get_schema_versions()`](https://calcofi.io/calcofi4db/reference/get_schema_versions.md)
  : Get Schema Version History
- [`init_schema_version_csv()`](https://calcofi.io/calcofi4db/reference/init_schema_version_csv.md)
  : Initialize Schema Version CSV
- [`record_schema_version()`](https://calcofi.io/calcofi4db/reference/record_schema_version.md)
  : Record Schema Version

## Utilities

utility functions for database operations (PostgreSQL deprecated)

- [`copy_schema()`](https://calcofi.io/calcofi4db/reference/copy_schema.md)
  : Copy Database Schema
- [`get_db_con()`](https://calcofi.io/calcofi4db/reference/get_db_con.md)
  **\[deprecated\]** : Get a database connection to the CalCOFI
  PostgreSQL database (DEPRECATED)

## Wrangle

functions for local DuckDB wrangling (keys, IDs, table consolidation)

- [`apply_data_corrections()`](https://calcofi.io/calcofi4db/reference/apply_data_corrections.md)
  : Apply Data Corrections
- [`assign_deterministic_uuids()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids.md)
  : Assign deterministic UUIDs from composite key columns
- [`assign_deterministic_uuids_md5()`](https://calcofi.io/calcofi4db/reference/assign_deterministic_uuids_md5.md)
  : Assign deterministic UUIDs using DuckDB-native md5
- [`assign_sequential_ids()`](https://calcofi.io/calcofi4db/reference/assign_sequential_ids.md)
  : Assign Sequential IDs with Deterministic Sort Order
- [`build_metadata_json()`](https://calcofi.io/calcofi4db/reference/build_metadata_json.md)
  : Build Metadata JSON for Parquet Outputs
- [`build_relationships_json()`](https://calcofi.io/calcofi4db/reference/build_relationships_json.md)
  : Build Relationships JSON from dm Object
- [`collect_cruise_key_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_cruise_key_mismatches.md)
  : Collect Cruise Key Mismatches
- [`collect_measurement_type_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_measurement_type_mismatches.md)
  : Collect Measurement Type Mismatches
- [`collect_ship_mismatches()`](https://calcofi.io/calcofi4db/reference/collect_ship_mismatches.md)
  : Collect Ship Mismatches
- [`consolidate_ichthyo_tables()`](https://calcofi.io/calcofi4db/reference/consolidate_ichthyo_tables.md)
  : Consolidate Ichthyoplankton Tables into Tidy Format
- [`convert_cruise_key_format()`](https://calcofi.io/calcofi4db/reference/convert_cruise_key_format.md)
  : Convert Old YYMMKK Cruise Key to YYYY-MM-NODC Format
- [`create_cruise_key()`](https://calcofi.io/calcofi4db/reference/create_cruise_key.md)
  : Create Cruise Key from Ship NODC Code and Date
- [`create_lookup_table()`](https://calcofi.io/calcofi4db/reference/create_lookup_table.md)
  : Create Lookup Table from Vocabulary Definitions
- [`derive_measurement_type_datasets()`](https://calcofi.io/calcofi4db/reference/derive_measurement_type_datasets.md)
  : Derive measurement_type → contributing datasets from the data
- [`enforce_column_types()`](https://calcofi.io/calcofi4db/reference/enforce_column_types.md)
  : Enforce Column Types Before Export
- [`ingest_yaml_to_dataset_df()`](https://calcofi.io/calcofi4db/reference/ingest_yaml_to_dataset_df.md)
  : Build the dataset registry table from ingest YAML blocks
- [`merge_metadata_json()`](https://calcofi.io/calcofi4db/reference/merge_metadata_json.md)
  : Merge Per-Ingest metadata.json into a Release-Level Sidecar
- [`merge_relationships_json()`](https://calcofi.io/calcofi4db/reference/merge_relationships_json.md)
  : Merge Multiple Relationships JSON Files
- [`propagate_natural_key()`](https://calcofi.io/calcofi4db/reference/propagate_natural_key.md)
  : Propagate Key from Parent to Child Table
- [`read_calcofi_meta()`](https://calcofi.io/calcofi4db/reference/read_calcofi_meta.md)
  : Read the calcofi YAML block from a single workflow file
- [`read_ingest_yaml()`](https://calcofi.io/calcofi4db/reference/read_ingest_yaml.md)
  : Read calcofi YAML blocks from ingest\_\*.qmd front matter
- [`read_relationships_json()`](https://calcofi.io/calcofi4db/reference/read_relationships_json.md)
  : Read Relationships JSON and Optionally Apply to dm
- [`replace_uuid_with_id()`](https://calcofi.io/calcofi4db/reference/replace_uuid_with_id.md)
  : Replace UUIDs with Integer Foreign Keys
- [`standardize_site_key()`](https://calcofi.io/calcofi4db/reference/standardize_site_key.md)
  : Standardize Site Key from Line and Station Columns
- [`write_parquet_outputs()`](https://calcofi.io/calcofi4db/reference/write_parquet_outputs.md)
  : Write Tables to Parquet Files
- [`write_spatial_manifest()`](https://calcofi.io/calcofi4db/reference/write_spatial_manifest.md)
  : Write Spatial Manifest

## Workflow

functions for orchestrating ingestion workflow steps

- [`build_release_table_registry()`](https://calcofi.io/calcofi4db/reference/build_release_table_registry.md)
  : Build Release Table Registry from Ingest Manifests
- [`build_targets_list()`](https://calcofi.io/calcofi4db/reference/build_targets_list.md)
  : Build Targets List from Quarto Frontmatter
- [`finalize_ingest()`](https://calcofi.io/calcofi4db/reference/finalize_ingest.md)
  : Finalize Ingest — Push Parquet Tables to Working DuckLake
- [`integrate_to_working_ducklake()`](https://calcofi.io/calcofi4db/reference/integrate_to_working_ducklake.md)
  : Integrate Ingest Outputs into Working DuckLake
- [`list_ingest_outputs()`](https://calcofi.io/calcofi4db/reference/list_ingest_outputs.md)
  : List Available Ingest Outputs
- [`parse_qmd_frontmatter()`](https://calcofi.io/calcofi4db/reference/parse_qmd_frontmatter.md)
  : Parse YAML Frontmatter from Quarto Notebooks
- [`read_ingest_manifest()`](https://calcofi.io/calcofi4db/reference/read_ingest_manifest.md)
  : Read Ingest Manifest from GCS
- [`read_ingest_parquet()`](https://calcofi.io/calcofi4db/reference/read_ingest_parquet.md)
  : Read Ingest Parquet Table from GCS
- [`write_ingest_outputs()`](https://calcofi.io/calcofi4db/reference/write_ingest_outputs.md)
  : Write Ingest Workflow Outputs to GCS

## Display

helper functions for workflow display outputs (GitHub links, validation
tables)

- [`dt()`](https://calcofi.io/calcofi4db/reference/dt.md) : Create
  Interactive Data Table with CSV Export
- [`github_file_link()`](https://calcofi.io/calcofi4db/reference/github_file_link.md)
  : Create GitHub File Link
- [`preview_tables()`](https://calcofi.io/calcofi4db/reference/preview_tables.md)
  : Preview Tables with Head and Tail Rows
- [`show_flagged_file()`](https://calcofi.io/calcofi4db/reference/show_flagged_file.md)
  : Show Flagged File Result
- [`show_validation_results()`](https://calcofi.io/calcofi4db/reference/show_validation_results.md)
  : Show Validation Results with GitHub Links

## Archive

functions for archiving and managing historical data snapshots

- [`cleanup_duplicate_archives()`](https://calcofi.io/calcofi4db/reference/cleanup_duplicate_archives.md)
  : Remove duplicate archives from GCS
- [`compare_local_vs_archive()`](https://calcofi.io/calcofi4db/reference/compare_local_vs_archive.md)
  : Compare local files with GCS archive
- [`download_archive()`](https://calcofi.io/calcofi4db/reference/download_archive.md)
  : Download archive to local directory
- [`get_archive_manifest()`](https://calcofi.io/calcofi4db/reference/get_archive_manifest.md)
  : Get archive manifest (file metadata)
- [`get_latest_archive_timestamp()`](https://calcofi.io/calcofi4db/reference/get_latest_archive_timestamp.md)
  : Get latest archive timestamp from GCS
- [`get_local_manifest()`](https://calcofi.io/calcofi4db/reference/get_local_manifest.md)
  : Get local file manifest
- [`sync_to_gcs_archive()`](https://calcofi.io/calcofi4db/reference/sync_to_gcs_archive.md)
  : Sync local files to GCS archive (deprecated wrapper)

## Version Sync

functions for synchronizing schema and package versions

- [`commit_version_and_permalink()`](https://calcofi.io/calcofi4db/reference/commit_version_and_permalink.md)
  : Commit Version Changes and Get Permalink
- [`complete_version_release()`](https://calcofi.io/calcofi4db/reference/complete_version_release.md)
  : Complete Version Release Workflow
- [`get_package_version()`](https://calcofi.io/calcofi4db/reference/get_package_version.md)
  : Get Current Package Version
- [`suggest_next_version()`](https://calcofi.io/calcofi4db/reference/suggest_next_version.md)
  : Suggest Next Version
- [`update_package_version()`](https://calcofi.io/calcofi4db/reference/update_package_version.md)
  : Synchronized Version Management for Package and Database

## Visualize

functions visualizing diagnostic outputs, particularly color coded data
tables

- [`show_fields_redefine()`](https://calcofi.io/calcofi4db/reference/show_fields_redefine.md)
  : Show fields to redefine
- [`show_source_files()`](https://calcofi.io/calcofi4db/reference/show_source_files.md)
  : Show source files
- [`show_tables_redefine()`](https://calcofi.io/calcofi4db/reference/show_tables_redefine.md)
  : Show tables to redefine

## Other

check for other functions or datasets not captured by above categories

- [`add_point_geom()`](https://calcofi.io/calcofi4db/reference/add_point_geom.md)
  : Add Point Geometry Column to a DuckDB Table

- [`append_obs()`](https://calcofi.io/calcofi4db/reference/append_obs.md)
  :

  Append occurrence-headline rows into the core `obs` table

- [`append_obs_attribute()`](https://calcofi.io/calcofi4db/reference/append_obs_attribute.md)
  :

  Append sub-occurrence attribute rows into the core `obs_attribute`
  table

- [`append_sample()`](https://calcofi.io/calcofi4db/reference/append_sample.md)
  :

  Append event rows into the core `sample` dimension

- [`append_sample_measurement()`](https://calcofi.io/calcofi4db/reference/append_sample_measurement.md)
  :

  Append event-level (effort) rows into the core `sample_measurement`
  table

- [`assign_grid_key()`](https://calcofi.io/calcofi4db/reference/assign_grid_key.md)
  : Assign Grid Key via Spatial Join

- [`build_dataset_taxon()`](https://calcofi.io/calcofi4db/reference/build_dataset_taxon.md)
  :

  Build the `dataset_taxon` crosswalk (per-dataset vocabulary -\>
  `taxon`)

- [`build_grid_reference()`](https://calcofi.io/calcofi4db/reference/build_grid_reference.md)
  :

  Build the shared `grid` reference table (deterministic,
  dataset-independent)

- [`build_sample_reference()`](https://calcofi.io/calcofi4db/reference/build_sample_reference.md)
  :

  Build the shared `sample` event dimension from the per-dataset event
  tables

- [`build_taxon_group()`](https://calcofi.io/calcofi4db/reference/build_taxon_group.md)
  :

  Build the `taxon_group` grouping table (many taxa per group)

- [`build_taxon_hierarchy()`](https://calcofi.io/calcofi4db/reference/build_taxon_hierarchy.md)
  : Build Taxon Hierarchy from Local spp.duckdb via Recursive CTEs

- [`build_taxon_reference()`](https://calcofi.io/calcofi4db/reference/build_taxon_reference.md)
  :

  Build the unified `taxon` reference table

- [`build_taxon_table()`](https://calcofi.io/calcofi4db/reference/build_taxon_table.md)
  : Build Taxonomic Hierarchy Table from WoRMS

- [`derive_cruise_key_on_casts()`](https://calcofi.io/calcofi4db/reference/derive_cruise_key_on_casts.md)
  : Derive Cruise Key on Bottle Casts via Ship Matching

- [`emit_core_tables()`](https://calcofi.io/calcofi4db/reference/emit_core_tables.md)
  : Project one dataset into the consolidated core tables

- [`ensure_interim_ships()`](https://calcofi.io/calcofi4db/reference/ensure_interim_ships.md)
  : Ensure Interim Ship Entries for Unmatched Ships

- [`fetch_ship_ices()`](https://calcofi.io/calcofi4db/reference/fetch_ship_ices.md)
  : Fetch Ship Codes from ICES Reference Codes API

- [`load_gcs_parquet_to_duckdb()`](https://calcofi.io/calcofi4db/reference/load_gcs_parquet_to_duckdb.md)
  : Load a GCS Parquet File into DuckDB

- [`match_by_site_datetime()`](https://calcofi.io/calcofi4db/reference/match_by_site_datetime.md)
  : Match Records to a Reference Table by Key + Datetime Window

- [`match_nearest_by_depth()`](https://calcofi.io/calcofi4db/reference/match_nearest_by_depth.md)
  : Match Records to the Nearest Reference Row Along a Continuous Axis

- [`match_ships()`](https://calcofi.io/calcofi4db/reference/match_ships.md)
  : Match Ship Codes Across Datasets Using Multi-Source References

- [`report_ship_matches()`](https://calcofi.io/calcofi4db/reference/report_ship_matches.md)
  : Report Ship Matching Status for a Dataset

- [`standardize_species()`](https://calcofi.io/calcofi4db/reference/standardize_species.md)
  : Standardize Species Identifiers Using WoRMS/ITIS/GBIF APIs

- [`standardize_species_local()`](https://calcofi.io/calcofi4db/reference/standardize_species_local.md)
  : Standardize Species Using Local spp.duckdb Lookups

- [`taxon_key_of()`](https://calcofi.io/calcofi4db/reference/taxon_key_of.md)
  :

  Encode an authority-prefixed `taxon_key`
