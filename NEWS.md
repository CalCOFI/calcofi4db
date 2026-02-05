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

