# calcofi4db 0.1.1

* Fix `detect_csv_changes()` to compare CSV files with `read_csv_files()` output.
  * Add type mismatch checks for fields in the CSV files.
* Add `print_csv_change_stats()` functions for textual summary of changes.
* Add `display_csv_changes()` to display changes in a color-coded table and 
    * Ensure compatibility with multiple output formats: interactive DataTable, static kable, or raw tibble.
* Expand documentation for `read_csv_files()` and `detect_csv_changes()`.

