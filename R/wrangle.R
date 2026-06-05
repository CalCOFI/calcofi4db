# wrangling functions for calcofi data restructuring
# handles intermediate transformations in local duckdb before output

#' Create Cruise Key from Ship NODC Code and Date
#'
#' Creates a natural key for cruises in format YYYY-MM-NODC where:
#' - YYYY = 4-digit year
#' - MM   = 2-digit month
#' - NODC = NODC ship code (from ship table's ship_nodc column)
#'
#' @param con DuckDB connection
#' @param cruise_tbl Name of cruise table (default: "cruise")
#' @param ship_tbl Name of ship table (default: "ship")
#' @param date_col Name of date column in cruise table (default: "date_ym")
#'
#' @return Invisibly returns the connection after adding cruise_key column
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con()
#' create_cruise_key(con)
#' # produces keys like "1998-02-33JD", "2024-01-33UD"
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
create_cruise_key <- function(
    con,
    cruise_tbl = "cruise",
    ship_tbl   = "ship",
    date_col   = "date_ym") {

  # add cruise_key column if it doesn't exist
  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{cruise_tbl}'"))$column_name

  if (!"cruise_key" %in% cols) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {cruise_tbl} ADD COLUMN IF NOT EXISTS cruise_key TEXT"))
  }

  # populate cruise_key as YYYY-MM-NODC (4-digit year + 2-digit month + NODC ship code)
  DBI::dbExecute(con, glue::glue("
    UPDATE {cruise_tbl} AS cr
    SET cruise_key = CONCAT(
      CAST(EXTRACT(YEAR FROM cr.{date_col}) AS VARCHAR),
      '-',
      LPAD(CAST(EXTRACT(MONTH FROM cr.{date_col}) AS VARCHAR), 2, '0'),
      '-',
      s.ship_nodc)
    FROM {ship_tbl} s
    WHERE cr.ship_key = s.ship_key"))

  # verify uniqueness
  dups <- DBI::dbGetQuery(con, glue::glue("
    SELECT cruise_key, COUNT(*) as n
    FROM {cruise_tbl}
    GROUP BY cruise_key
    HAVING COUNT(*) > 1
    ORDER BY cruise_key
    LIMIT 10"))

  if (nrow(dups) > 0) {
    examples <- paste(
      glue::glue("{dups$cruise_key} (n={dups$n})"), collapse = ", ")
    warning(glue::glue(
      "cruise_key is not unique! Found {nrow(dups)} duplicate keys. ",
      "Examples: {examples}"))
  }

  # check for NULLs (ships without NODC codes)
  nulls <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {cruise_tbl} WHERE cruise_key IS NULL"))$n
  if (nulls > 0) {
    warning(glue::glue(
      "{nulls} cruises have NULL cruise_key (ship_key not found in {ship_tbl} or missing ship_nodc)"))
  }

  message(glue::glue("Created cruise_key column in {cruise_tbl}"))
  invisible(con)
}

#' Convert Old YYMMKK Cruise Key to YYYY-MM-NODC Format
#'
#' Converts legacy cruise key format (YYMMKK: 2-digit year + 2-digit month +
#' 2-letter ship_key) to the new format (YYYY-MM-NODC: 4-digit year + 2-digit
#' month + NODC ship code). Requires the ship table for KK → NODC mapping.
#'
#' Century disambiguation: YY >= 49 → 19YY, YY < 49 → 20YY
#' (CalCOFI data spans 1949-present).
#'
#' @param con DuckDB connection
#' @param table Name of table containing old cruise keys
#' @param old_key_col Name of column with old YYMMKK keys (default: "cruise_key")
#' @param ship_tbl Name of ship table (default: "ship")
#'
#' @return Invisibly returns the connection after updating cruise_key column
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' # convert CTD cruise keys extracted from filenames
#' convert_cruise_key_format(con, "ctd_cast", "cruise_key_old")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
convert_cruise_key_format <- function(
    con,
    table,
    old_key_col = "cruise_key",
    ship_tbl    = "ship") {

  # add new column if updating in-place
  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS cruise_key_new TEXT"))

  # convert: extract YY, MM, KK from old format, look up ship_nodc
  DBI::dbExecute(con, glue::glue("
    UPDATE {table} AS t
    SET cruise_key_new = CONCAT(
      CASE
        WHEN CAST(LEFT(t.{old_key_col}, 2) AS INTEGER) >= 49 THEN '19'
        ELSE '20'
      END,
      LEFT(t.{old_key_col}, 2),
      '-',
      SUBSTR(t.{old_key_col}, 3, 2),
      '-',
      s.ship_nodc)
    FROM {ship_tbl} s
    WHERE s.ship_key = RIGHT(t.{old_key_col}, LENGTH(t.{old_key_col}) - 4)"))

  n_converted <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {table} WHERE cruise_key_new IS NOT NULL"))$n
  n_total <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {table} WHERE {old_key_col} IS NOT NULL"))$n

  message(glue::glue(
    "Converted {n_converted}/{n_total} cruise keys from YYMMKK to YYYY-MM-NODC"))

  invisible(con)
}

#' Standardize Site Key from Line and Station Columns
#'
#' Creates a `site_key` column formatted as `NNN.N NNN.N` (0-padded, 1 decimal)
#' from line and station columns. This ensures consistent site identification
#' across all datasets.
#'
#' @param con DuckDB connection
#' @param table Name of table to update
#' @param line_col Name of line column (default: "line")
#' @param station_col Name of station column (default: "station")
#' @param site_key_col Name of site key column to create (default: "site_key")
#'
#' @return Invisibly returns the connection after adding site_key column
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' standardize_site_key(con, "site", "line", "station")
#' # line=90.0, station=62.0 → "090.0 062.0"
#' # line=93.3, station=110.0 → "093.3 110.0"
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
standardize_site_key <- function(
    con,
    table,
    line_col     = "line",
    station_col  = "station",
    site_key_col = "site_key") {

  DBI::dbExecute(con, glue::glue(
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {site_key_col} TEXT"))

  DBI::dbExecute(con, glue::glue("
    UPDATE {table}
    SET {site_key_col} = PRINTF('%05.1f %05.1f', {line_col}, {station_col})"))

  n_set <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {table} WHERE {site_key_col} IS NOT NULL"))$n
  n_null <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {table}
     WHERE {site_key_col} IS NULL AND {line_col} IS NOT NULL"))$n

  message(glue::glue(
    "Standardized {site_key_col} in {table}: {n_set} rows set, {n_null} NULL (with non-NULL line)"))

  invisible(con)
}

#' Propagate Key from Parent to Child Table
#'
#' Copies a key column from a parent table to a child table by joining on
#' a common column (typically UUID). This is used to populate foreign key
#' columns before assigning sequential IDs with deterministic sort order.
#'
#' @param con DuckDB connection
#' @param child_tbl Name of child table to update
#' @param parent_tbl Name of parent table containing the key
#' @param key_col Name of the key column to propagate (e.g., "cruise_key", "site_id")
#' @param join_col Name of the column to join on (must exist in both tables)
#' @param key_type SQL data type for the key ("TEXT" or "INTEGER", default: auto-detect from parent)
#'
#' @return Invisibly returns the connection after adding and populating key column
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' # propagate cruise_key (TEXT) to site table
#' propagate_natural_key(
#'   con        = con,
#'   child_tbl  = "site",
#'   parent_tbl = "cruise",
#'   key_col    = "cruise_key",
#'   join_col   = "cruise_uuid")
#'
#' # propagate site_id (INTEGER) to tow table
#' propagate_natural_key(
#'   con        = con,
#'   child_tbl  = "tow",
#'   parent_tbl = "site",
#'   key_col    = "site_id",
#'   join_col   = "site_uuid")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
propagate_natural_key <- function(
    con,
    child_tbl,
    parent_tbl,
    key_col,
    join_col,
    key_type = NULL) {

  # auto-detect key type from parent table if not specified
  if (is.null(key_type)) {
    parent_cols <- DBI::dbGetQuery(con, glue::glue(
      "SELECT column_name, data_type FROM information_schema.columns
       WHERE table_name = '{parent_tbl}' AND column_name = '{key_col}'"))

    if (nrow(parent_cols) == 0) {
      stop(glue::glue("Column {key_col} not found in {parent_tbl}"))
    }

    # map DuckDB types to SQL types
    dtype <- toupper(parent_cols$data_type)
    if (grepl("INT", dtype)) {
      key_type <- "INTEGER"
    } else {
      key_type <- "TEXT"
    }
  }

  # add key column to child if it doesn't exist
  child_cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{child_tbl}'"))$column_name

  if (!key_col %in% child_cols) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {child_tbl} ADD COLUMN IF NOT EXISTS {key_col} {key_type}"))
  }

  # populate key from parent table via join
  DBI::dbExecute(con, glue::glue("
    UPDATE {child_tbl}
    SET {key_col} = (
      SELECT {parent_tbl}.{key_col}
      FROM {parent_tbl}
      WHERE {parent_tbl}.{join_col} = {child_tbl}.{join_col})"))

  # check for NULLs (orphan join values)
  nulls <- DBI::dbGetQuery(con, glue::glue("
    SELECT COUNT(*) as n
    FROM {child_tbl}
    WHERE {key_col} IS NULL AND {join_col} IS NOT NULL"))$n

  if (nulls > 0) {
    warning(glue::glue(
      "{nulls} rows in {child_tbl} have {join_col} not found in {parent_tbl}"))
  }

  n_updated <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) as n FROM {child_tbl} WHERE {key_col} IS NOT NULL"))$n

  message(glue::glue(
    "Propagated {key_col} ({key_type}) to {child_tbl}: {n_updated} rows updated via {join_col}"))

  invisible(con)
}

#' Assign Sequential IDs with Deterministic Sort Order
#'
#' Assigns sequential integer IDs to a table based on specified sort order.
#' This ensures reproducibility when appending new data - the same data
#' will always get the same IDs.
#'
#' @param con DuckDB connection
#' @param table_name Name of table to assign IDs to
#' @param id_col Name of ID column to create (default: paste0(table_name, "_id"))
#' @param sort_cols Character vector of columns to sort by for ID assignment
#' @param start_id Starting ID value (default: 1)
#'
#' @return Invisibly returns the connection after adding ID column
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con()
#'
#' # assign site_id sorted by cruise_key and orderocc
#' assign_sequential_ids(
#'   con        = con,
#'   table_name = "site",
#'   id_col     = "site_id",
#'   sort_cols  = c("cruise_key", "orderocc"))
#'
#' # assign lookup_id with multi-column sort
#' assign_sequential_ids(
#'   con        = con,
#'   table_name = "lookup",
#'   id_col     = "lookup_id",
#'   sort_cols  = c("lookup_type", "lookup_num"))
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
assign_sequential_ids <- function(
    con,
    table_name,
    id_col    = NULL,
    sort_cols,
    start_id  = 1) {

  if (is.null(id_col)) {
    id_col <- paste0(table_name, "_id")
  }

  # check if column exists
  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{table_name}'"))$column_name

  if (!id_col %in% cols) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {id_col} INTEGER"))
  }

  # build sort clause with COALESCE for NULL handling
  sort_clause <- paste(
    sapply(sort_cols, function(col) {
      glue::glue("COALESCE(CAST({col} AS VARCHAR), '')")
    }),
    collapse = ", ")

  # assign IDs using ROW_NUMBER with deterministic sort
  # use a temp table approach for DuckDB
  temp_tbl <- paste0("_temp_", table_name, "_ids")

  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE {temp_tbl} AS
    SELECT *, ROW_NUMBER() OVER (ORDER BY {sort_clause}) + {start_id - 1} AS _new_id
    FROM {table_name}"))

  # drop original and rename
  DBI::dbExecute(con, glue::glue("DROP TABLE {table_name}"))
  DBI::dbExecute(con, glue::glue("ALTER TABLE {temp_tbl} RENAME TO {table_name}"))

  # update the id column from _new_id
  DBI::dbExecute(con, glue::glue("UPDATE {table_name} SET {id_col} = _new_id"))
  DBI::dbExecute(con, glue::glue("ALTER TABLE {table_name} DROP COLUMN _new_id"))

  n_rows <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) as n FROM {table_name}"))$n

  message(glue::glue(
    "Assigned {id_col} to {n_rows} rows in {table_name} ",
    "(sorted by: {paste(sort_cols, collapse = ', ')})"))

  invisible(con)
}

#' Assign deterministic UUIDs from composite key columns
#'
#' Generates UUID v5 (name-based SHA-1) identifiers from a composite key.
#' The same key values always produce the same UUID, making IDs stable
#' across re-ingestion regardless of row order.
#'
#' For large tables (over `chunk_size` rows), processes in chunks to avoid
#' exceeding R memory limits. Only key columns are read per chunk, and UUIDs
#' are written back via temp table UPDATE joins.
#'
#' @param con DuckDB connection
#' @param table_name Name of table to assign UUIDs to
#' @param id_col Name of UUID column to create
#' @param key_cols Character vector of columns forming the composite key
#' @param namespace_uuid Fixed namespace UUID for deterministic generation
#' @param chunk_size Number of rows per chunk for large tables (default: 5e6)
#'
#' @return Invisibly returns the connection after adding UUID column
#' @export
#' @concept wrangle
#' @importFrom DBI dbGetQuery dbExecute dbWriteTable
#' @importFrom glue glue
#' @importFrom uuid UUIDfromName
assign_deterministic_uuids <- function(
    con,
    table_name,
    id_col,
    key_cols,
    namespace_uuid = "c0f1ca00-ca1c-5000-b000-1c4790000000",
    chunk_size     = 5e6) {

  n_rows <- DBI::dbGetQuery(
    con, glue::glue("SELECT COUNT(*) AS n FROM {table_name}"))$n

  if (n_rows <= chunk_size) {
    # small table: original in-memory approach
    data <- DBI::dbGetQuery(con, glue::glue("SELECT * FROM {table_name}"))

    key_strings <- do.call(paste, c(
      lapply(key_cols, function(col) {
        ifelse(is.na(data[[col]]), "", as.character(data[[col]]))
      }),
      sep = "|"))

    data[[id_col]] <- uuid::UUIDfromName(
      namespace = namespace_uuid,
      name      = key_strings,
      type      = "sha1")

    col_order <- c(id_col, setdiff(names(data), id_col))
    data      <- data[, col_order]

    DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {table_name}"))
    DBI::dbWriteTable(con, table_name, data, overwrite = TRUE)

    n_unique <- length(unique(data[[id_col]]))
  } else {
    # large table: chunked processing to avoid OOM
    message(glue::glue(
      "Large table ({n_rows} rows) — using chunked UUID assignment"))

    # add uuid column and rownum for stable chunking
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {id_col} VARCHAR"))
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS _rownum BIGINT"))
    DBI::dbExecute(con, glue::glue(
      "UPDATE {table_name} SET _rownum = rowid"))

    n_chunks <- ceiling(n_rows / chunk_size)
    key_sel  <- paste(c("_rownum", key_cols), collapse = ", ")

    for (i in seq_len(n_chunks)) {
      offset <- (i - 1) * chunk_size

      # read only key_cols + _rownum for this chunk
      chunk <- DBI::dbGetQuery(con, glue::glue(
        "SELECT {key_sel} FROM {table_name}
         ORDER BY _rownum
         LIMIT {chunk_size} OFFSET {offset}"))

      # generate UUIDs
      key_strings <- do.call(paste, c(
        lapply(key_cols, function(col) {
          ifelse(is.na(chunk[[col]]), "", as.character(chunk[[col]]))
        }),
        sep = "|"))

      chunk$uuid_val <- uuid::UUIDfromName(
        namespace = namespace_uuid,
        name      = key_strings,
        type      = "sha1")

      # write back via temp table + UPDATE join
      DBI::dbWriteTable(
        con, "_uuid_tmp", chunk[, c("_rownum", "uuid_val")], overwrite = TRUE)
      DBI::dbExecute(con, glue::glue("
        UPDATE {table_name} AS t
        SET {id_col} = u.uuid_val
        FROM _uuid_tmp AS u
        WHERE t._rownum = u._rownum"))
      DBI::dbExecute(con, "DROP TABLE IF EXISTS _uuid_tmp")

      message(glue::glue("  chunk {i}/{n_chunks}: {nrow(chunk)} rows"))
      rm(chunk, key_strings)
      gc()
    }

    # cleanup helper column
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} DROP COLUMN _rownum"))

    # reorder columns to put id_col first
    all_cols  <- DBI::dbListFields(con, table_name)
    col_order <- c(id_col, setdiff(all_cols, id_col))
    DBI::dbExecute(con, glue::glue("
      CREATE OR REPLACE TABLE {table_name} AS
      SELECT {paste(col_order, collapse = ', ')} FROM {table_name}"))

    n_unique <- DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(DISTINCT {id_col}) AS n FROM {table_name}"))$n
  }

  message(glue::glue(
    "Assigned {id_col} to {n_rows} rows in {table_name} ",
    "({n_unique} unique UUIDs from: {paste(key_cols, collapse = ', ')})"))

  if (n_unique != n_rows) {
    warning(glue::glue(
      "Non-unique UUIDs detected: {n_rows} rows but {n_unique} unique values. ",
      "Check that key_cols form a true composite key."))
  }

  invisible(con)
}

#' Assign deterministic UUIDs using DuckDB-native md5
#'
#' Generates UUID-style identifiers from a composite key entirely inside
#' DuckDB using `md5()`. Unlike `assign_deterministic_uuids()` which pulls
#' data into R and uses UUID v5 (SHA-1), this runs as a single SQL statement
#' with zero R data transfer — orders of magnitude faster on large tables.
#'
#' The 32-char md5 hex is formatted as 8-4-4-4-12 to resemble a UUID string.
#' These are **not** RFC 4122 UUIDs — they are md5-based internal identifiers.
#' Use this for internal-only IDs on large tables (e.g., ctd_measurement).
#' Use `assign_deterministic_uuids()` when RFC 4122 UUID v5 compatibility
#' is needed (e.g., ichthyo tables shared with external systems).
#'
#' @param con DuckDB connection
#' @param table_name Name of table to assign UUIDs to
#' @param id_col Name of UUID column to create
#' @param key_cols Character vector of columns forming the composite key
#' @param namespace Optional namespace string prepended to keys for domain
#'   separation (default: NULL)
#'
#' @return Invisibly returns the connection after adding UUID column
#' @export
#' @concept wrangle
#' @importFrom DBI dbGetQuery dbExecute dbListFields
#' @importFrom glue glue
assign_deterministic_uuids_md5 <- function(
    con,
    table_name,
    id_col,
    key_cols,
    namespace = NULL) {

  # build COALESCE(CAST(col AS VARCHAR), '') for each key col
  key_expr <- paste(
    sapply(key_cols, function(col)
      glue::glue("COALESCE(CAST({col} AS VARCHAR), '')")),
    collapse = ", ")

  # optionally prepend namespace for domain separation
  if (!is.null(namespace))
    key_expr <- paste0("'", namespace, "', ", key_expr)

  concat_expr <- glue::glue("concat_ws('|', {key_expr})")

  # pre-check: verify composite key uniqueness before generating UUIDs
  key_cols_sql <- paste(key_cols, collapse = ", ")
  pre_stats <- DBI::dbGetQuery(con, glue::glue("
    SELECT COUNT(*) AS n_rows,
           COUNT(DISTINCT {concat_expr}) AS n_unique
    FROM {table_name}"))

  message(glue::glue(
    "Pre-check {table_name}: {format(pre_stats$n_rows, big.mark = ',')} rows, ",
    "{format(pre_stats$n_unique, big.mark = ',')} unique keys ",
    "from ({paste(key_cols, collapse = ', ')})"))

  if (pre_stats$n_unique != pre_stats$n_rows) {
    n_dups <- pre_stats$n_rows - pre_stats$n_unique
    warning(glue::glue(
      "Composite key is NOT unique: {format(n_dups, big.mark = ',')} ",
      "duplicate rows detected in {table_name} for key_cols ",
      "({paste(key_cols, collapse = ', ')}). ",
      "Generated UUIDs will NOT be unique."))
  }

  # generate UUIDs via md5
  md5_expr <- glue::glue("md5({concat_expr})")

  # format as UUID string: 8-4-4-4-12
  uuid_expr <- glue::glue(
    "substr(_h, 1, 8) || '-' || substr(_h, 9, 4) || '-' || ",
    "substr(_h, 13, 4) || '-' || substr(_h, 17, 4) || '-' || ",
    "substr(_h, 21, 12)")

  # get existing columns (exclude id_col if it already exists)
  cols <- DBI::dbListFields(con, table_name)
  other_cols <- setdiff(cols, id_col)
  select_clause <- paste(other_cols, collapse = ", ")

  # single SQL: rebuild table with UUID column first
  DBI::dbExecute(con, glue::glue("
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT
      {uuid_expr} AS {id_col},
      {select_clause}
    FROM (
      SELECT *, {md5_expr} AS _h
      FROM {table_name}
    ) _sub"))

  message(glue::glue(
    "Assigned {id_col} to {format(pre_stats$n_rows, big.mark = ',')} rows ",
    "in {table_name} ({format(pre_stats$n_unique, big.mark = ',')} unique ",
    "from: {paste(key_cols, collapse = ', ')})"))

  invisible(con)
}

#' Create Lookup Table from Vocabulary Definitions
#'
#' Creates a unified lookup table from vocabulary definitions for egg stages,
#' larva stages, and tow types.
#'
#' @param con DuckDB connection
#' @param lookup_tbl Name of lookup table to create (default: "lookup")
#' @param egg_stage_vocab Tibble with egg stage vocabulary (columns: stage_int, stage_description)
#' @param larva_stage_vocab Tibble with larva stage vocabulary (columns: stage_int, stage_txt, stage_description)
#' @param tow_type_vocab Optional tibble with tow type vocabulary (columns: lookup_num, lookup_chr, description)
#'
#' @return Invisibly returns the connection after creating lookup table
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' egg_vocab <- tibble::tibble(
#'   stage_int = 1:11,
#'   stage_description = paste0("egg, stage ", 1:11, " of 11 (Moser & Ahlstrom, 1985)"))
#'
#' larva_vocab <- tibble::tibble(
#'   stage_int = 1:5,
#'   stage_txt = c("YOLK", "PREF", "FLEX", "POST", "TRNS"),
#'   stage_description = c(
#'     "larva, yolk sac",
#'     "larva, preflexion",
#'     "larva, flexion",
#'     "larva, postflexion",
#'     "larva, transformation"))
#'
#' create_lookup_table(con, egg_stage_vocab = egg_vocab, larva_stage_vocab = larva_vocab)
#' }
#' @importFrom DBI dbWriteTable dbExecute
#' @importFrom dplyr bind_rows mutate select arrange
#' @importFrom tibble tibble
#' @importFrom glue glue
create_lookup_table <- function(
    con,
    lookup_tbl        = "lookup",
    egg_stage_vocab   = NULL,
    larva_stage_vocab = NULL,
    tow_type_vocab    = NULL) {

  # build lookup table from vocabularies
  lookup_rows <- list()

  # egg stage vocab
  if (!is.null(egg_stage_vocab)) {
    egg_lookup <- egg_stage_vocab |>
      dplyr::mutate(
        lookup_type = "egg_stage",
        lookup_num = stage_int,
        lookup_chr = NA_character_,
        description = stage_description) |>
      dplyr::select(lookup_type, lookup_num, lookup_chr, description)

    lookup_rows <- append(lookup_rows, list(egg_lookup))
  }

  # larva stage vocab
  if (!is.null(larva_stage_vocab)) {
    larva_lookup <- larva_stage_vocab |>
      dplyr::mutate(
        lookup_type = "larva_stage",
        lookup_num = stage_int,
        lookup_chr = stage_txt,
        description = stage_description) |>
      dplyr::select(lookup_type, lookup_num, lookup_chr, description)

    lookup_rows <- append(lookup_rows, list(larva_lookup))
  }

  # tow type vocab
  if (!is.null(tow_type_vocab)) {
    tow_lookup <- tow_type_vocab |>
      dplyr::mutate(
        lookup_type = "tow_type") |>
      dplyr::select(lookup_type, lookup_num, lookup_chr, description)

    lookup_rows <- append(lookup_rows, list(tow_lookup))
  }

  # combine all lookup rows
  lookup_data <- dplyr::bind_rows(lookup_rows) |>
    dplyr::arrange(lookup_type, lookup_num)

  # add lookup_id and provenance columns
  lookup_data <- lookup_data |>
    dplyr::mutate(
      lookup_id     = dplyr::row_number(),
      `_source_file` = "create_lookup_table()",
      `_source_row`  = dplyr::row_number(),
      `_ingested_at` = as.character(Sys.time()),
      `_source_uuid` = NA_character_) |>
    dplyr::select(
      lookup_id, lookup_type, lookup_num, lookup_chr, description,
      `_source_file`, `_source_row`, `_ingested_at`, `_source_uuid`)

  # write to database
  DBI::dbWriteTable(con, lookup_tbl, lookup_data, overwrite = TRUE)

  message(glue::glue(
    "Created lookup table with {nrow(lookup_data)} entries ",
    "({length(unique(lookup_data$lookup_type))} types)"))

  invisible(con)
}

#' Consolidate Ichthyoplankton Tables into Tidy Format
#'
#' Transforms 5 separate ichthyoplankton tables (egg, eggstage, larva, larvastage,
#' larvasize) into a single tidy table with columns: net_uuid, species_id, life_stage,
#' measurement_type, measurement_value, tally.
#'
#' The output retains `net_uuid` as the foreign key to the net table (source UUIDs
#' are kept as primary identifiers). Use `assign_deterministic_uuids()` to add an
#' `ichthyo_uuid` primary key column derived from the composite natural key.
#'
#' @param con DuckDB connection
#' @param output_tbl Name of output table (default: "ichthyo")
#' @param egg_tbl Name of egg totals table (default: "egg")
#' @param eggstage_tbl Name of egg stage table (default: "egg_stage")
#' @param larva_tbl Name of larva totals table (default: "larva")
#' @param larvastage_tbl Name of larva stage table (default: "larva_stage")
#' @param larvasize_tbl Name of larva size table (default: "larva_size")
#' @param larva_stage_vocab Tibble mapping stage text (YOLK, PREF, etc.) to integers
#' @param net_id_col Name of net identifier column in source tables (default: "net_uuid")
#'
#' @return Invisibly returns the connection after creating ichthyo table
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' larva_vocab <- tibble::tibble(
#'   stage_txt = c("YOLK", "PREF", "FLEX", "POST", "TRNS"),
#'   stage_int = 1:5)
#'
#' consolidate_ichthyo_tables(
#'   con               = con,
#'   larva_stage_vocab = larva_vocab)
#'
#' # assign ichthyo_uuid — deterministic UUID v5 from composite natural key
#' assign_deterministic_uuids(
#'   con        = con,
#'   table_name = "ichthyo",
#'   id_col     = "ichthyo_uuid",
#'   key_cols   = c("net_uuid", "species_id", "life_stage",
#'                  "measurement_type", "measurement_value"))
#' }
#' @importFrom DBI dbExecute dbGetQuery dbWriteTable
#' @importFrom dplyr tbl collect mutate select bind_rows left_join
#' @importFrom glue glue
consolidate_ichthyo_tables <- function(
    con,
    output_tbl        = "ichthyo",
    egg_tbl           = "egg",
    eggstage_tbl      = "egg_stage",
    larva_tbl         = "larva",
    larvastage_tbl    = "larva_stage",
    larvasize_tbl     = "larva_size",
    invert_tbl        = "invert",
    larva_stage_vocab = NULL,
    net_id_col        = "net_uuid") {

  # helper to check if table exists
  table_exists <- function(tbl_name) {
    tbl_name %in% DBI::dbListTables(con)
  }

  ichthyo_rows <- list()

  # provenance columns to carry over from source tables
  prov_cols <- c("_source_file", "_source_row", "_ingested_at", "_source_uuid")

 # helper to select data + provenance columns that exist
  select_with_prov <- function(data, ...) {
    base_cols <- c(...)
    existing_prov <- intersect(prov_cols, names(data))
    dplyr::select(data, dplyr::all_of(c(base_cols, existing_prov)))
  }

  # 1. egg totals (life_stage = 'egg', measurement_type = NULL)
  if (table_exists(egg_tbl)) {
    egg_data <- dplyr::tbl(con, egg_tbl) |>
      dplyr::collect() |>
      dplyr::mutate(
        net_uuid          = .data[[net_id_col]],
        life_stage        = "egg",
        measurement_type  = NA_character_,
        measurement_value = NA_real_) |>
      select_with_prov("net_uuid", "species_id", "life_stage", "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(egg_data))
    message(glue::glue("  Processed {nrow(egg_data)} rows from {egg_tbl}"))
  }

  # 2. egg stages (life_stage = 'egg', measurement_type = 'stage')
  if (table_exists(eggstage_tbl)) {
    eggstage_data <- dplyr::tbl(con, eggstage_tbl) |>
      dplyr::collect() |>
      dplyr::mutate(
        net_uuid          = .data[[net_id_col]],
        life_stage        = "egg",
        measurement_type  = "stage",
        measurement_value = as.double(stage)) |>
      select_with_prov("net_uuid", "species_id", "life_stage", "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(eggstage_data))
    message(glue::glue("  Processed {nrow(eggstage_data)} rows from {eggstage_tbl}"))
  }

  # 3. larva totals (life_stage = 'larva', measurement_type = NULL)
  if (table_exists(larva_tbl)) {
    larva_data <- dplyr::tbl(con, larva_tbl) |>
      dplyr::collect() |>
      dplyr::mutate(
        net_uuid          = .data[[net_id_col]],
        life_stage        = "larva",
        measurement_type  = NA_character_,
        measurement_value = NA_real_) |>
      select_with_prov("net_uuid", "species_id", "life_stage", "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(larva_data))
    message(glue::glue("  Processed {nrow(larva_data)} rows from {larva_tbl}"))
  }

  # 4. larva stages (life_stage = 'larva', measurement_type = 'stage')
  if (table_exists(larvastage_tbl)) {
    larvastage_data <- dplyr::tbl(con, larvastage_tbl) |>
      dplyr::collect()

    # convert stage text to integer if vocab provided
    if (!is.null(larva_stage_vocab)) {
      larvastage_data <- larvastage_data |>
        dplyr::left_join(
          larva_stage_vocab |> dplyr::select(stage_txt, stage_int),
          by = c("stage" = "stage_txt")) |>
        dplyr::mutate(
          measurement_value = as.double(stage_int))
    } else {
      # try to use stage directly as numeric
      larvastage_data <- larvastage_data |>
        dplyr::mutate(
          measurement_value = as.double(stage))
    }

    larvastage_data <- larvastage_data |>
      dplyr::mutate(
        net_uuid         = .data[[net_id_col]],
        life_stage       = "larva",
        measurement_type = "stage") |>
      select_with_prov("net_uuid", "species_id", "life_stage", "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(larvastage_data))
    message(glue::glue("  Processed {nrow(larvastage_data)} rows from {larvastage_tbl}"))
  }

  # 5. larva sizes (life_stage = 'larva', measurement_type = 'size')
  if (table_exists(larvasize_tbl)) {
    larvasize_data <- dplyr::tbl(con, larvasize_tbl) |>
      dplyr::collect() |>
      dplyr::mutate(
        net_uuid          = .data[[net_id_col]],
        life_stage        = "larva",
        measurement_type  = "size",
        measurement_value = as.double(length_mm)) |>
      select_with_prov("net_uuid", "species_id", "life_stage", "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(larvasize_data))
    message(glue::glue("  Processed {nrow(larvasize_data)} rows from {larvasize_tbl}"))
  }

  # 6. invert totals (life_stage = 'invert', measurement_type = NULL)
  if (table_exists(invert_tbl)) {
    invert_data <- dplyr::tbl(con, invert_tbl) |>
      dplyr::collect() |>
      dplyr::mutate(
        net_uuid          = .data[[net_id_col]],
        life_stage        = "invert",
        measurement_type  = NA_character_,
        measurement_value = NA_real_) |>
      select_with_prov(
        "net_uuid", "species_id", "life_stage",
        "measurement_type", "measurement_value", "tally")

    ichthyo_rows <- append(ichthyo_rows, list(invert_data))
    message(glue::glue("  Processed {nrow(invert_data)} rows from {invert_tbl}"))
  }

  # combine all rows
  ichthyo_data <- dplyr::bind_rows(ichthyo_rows)

  # write to database
  DBI::dbWriteTable(con, output_tbl, ichthyo_data, overwrite = TRUE)

  message(glue::glue(
    "Created {output_tbl} table with {nrow(ichthyo_data)} rows ",
    "(from {length(ichthyo_rows)} source tables)"))

  invisible(con)
}

#' Replace UUIDs with Integer Foreign Keys
#'
#' Creates an integer ID column and populates it based on UUID lookups,
#' then optionally drops the UUID column.
#'
#' @param con DuckDB connection
#' @param table_name Table to update
#' @param uuid_col Name of UUID column to replace
#' @param new_id_col Name of new integer ID column
#' @param ref_table Reference table containing UUID-to-ID mapping
#' @param ref_uuid_col UUID column in reference table
#' @param ref_id_col ID column in reference table
#' @param drop_uuid Whether to drop the UUID column after replacement (default: TRUE)
#'
#' @return Invisibly returns the connection
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' # replace site_uuid with site_id FK
#' replace_uuid_with_id(
#'   con         = con,
#'   table_name  = "tow",
#'   uuid_col    = "site_uuid",
#'   new_id_col  = "site_id",
#'   ref_table   = "site",
#'   ref_uuid_col = "site_uuid",
#'   ref_id_col  = "site_id")
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
replace_uuid_with_id <- function(
    con,
    table_name,
    uuid_col,
    new_id_col,
    ref_table,
    ref_uuid_col,
    ref_id_col,
    drop_uuid = TRUE) {

  # check if new column exists
  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{table_name}'"))$column_name

  if (!new_id_col %in% cols) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {new_id_col} INTEGER"))
  }

  # populate new ID column from reference table
  DBI::dbExecute(con, glue::glue("
    UPDATE {table_name}
    SET {new_id_col} = (
      SELECT {ref_id_col}
      FROM {ref_table}
      WHERE {ref_table}.{ref_uuid_col} = {table_name}.{uuid_col})"))

  # check for NULLs (orphan UUIDs)
  nulls <- DBI::dbGetQuery(con, glue::glue("
    SELECT COUNT(*) as n
    FROM {table_name}
    WHERE {new_id_col} IS NULL AND {uuid_col} IS NOT NULL"))$n

  if (nulls > 0) {
    warning(glue::glue(
      "{nulls} rows in {table_name} have {uuid_col} values not found in {ref_table}"))
  }

  # optionally drop UUID column
  if (drop_uuid) {
    DBI::dbExecute(con, glue::glue(
      "ALTER TABLE {table_name} DROP COLUMN {uuid_col}"))
    message(glue::glue(
      "Replaced {uuid_col} with {new_id_col} in {table_name} (dropped UUID column)"))
  } else {
    message(glue::glue(
      "Added {new_id_col} to {table_name} (kept {uuid_col} column)"))
  }

  invisible(con)
}

#' Apply Data Corrections
#'
#' Applies known data corrections to the database. This function should be
#' updated as new corrections are identified by data managers.
#'
#' @param con DuckDB connection
#' @param verbose Print correction messages (default: TRUE)
#'
#' @return Invisibly returns the connection
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con()
#' apply_data_corrections(con)
#' }
#' @importFrom DBI dbExecute dbGetQuery
#' @importFrom glue glue
apply_data_corrections <- function(con, verbose = TRUE) {

  corrections_applied <- 0

 # correction 1: fix missing ship_nodc for BOLD HORIZON
  if ("ship" %in% DBI::dbListTables(con)) {
    result <- DBI::dbExecute(con, "
      UPDATE ship
      SET ship_nodc = '39C2'
      WHERE ship_name = 'BOLD HORIZON' AND (ship_nodc IS NULL OR ship_nodc = '')")

    if (result > 0) {
      if (verbose) message("Correction: Set ship.ship_nodc = '39C2' for ship_name = 'BOLD HORIZON'")
      corrections_applied <- corrections_applied + 1
    }
  }

  # correction 2: add missing invert species not in Ed Weber's species.csv
  # these species exist in inverts.csv but have no matching species_id row.
  # names and IDs sourced from ERDDAP erdCalCOFIinvcnt; worms_id from WoRMS.
  if ("species" %in% DBI::dbListTables(con)) {
    missing_species <- data.frame(
      species_id      = c(9542L,  9562L,  9597L,  9607L,  9787L,  9790L),
      scientific_name = c("Doryteuthis opalescens", "Abraliopsis sp A",
                          "Onychoteuthis sp B", "Onykia robusta",
                          "Octopodid type E", "Octopodid type H"),
      common_name     = c("Market squid", NA, NA, "Robust clubhook squid", NA, NA),
      itis_id         = c(82371L, 82398L, 82439L, 82438L, 82590L, 82590L),
      worms_id        = c(574540L, NA, NA, 342397L, NA, NA),
      stringsAsFactors = FALSE)

    existing <- DBI::dbGetQuery(con, paste0(
      "SELECT species_id FROM species WHERE species_id IN (",
      paste(missing_species$species_id, collapse = ","), ")"))

    to_add <- missing_species[!missing_species$species_id %in% existing$species_id, ]

    # only keep columns that exist in the target table
    if (nrow(to_add) > 0) {
      tbl_cols <- DBI::dbListFields(con, "species")
      to_add   <- to_add[, intersect(names(to_add), tbl_cols), drop = FALSE]
      DBI::dbWriteTable(con, "species", to_add, append = TRUE)
      if (verbose) message(glue::glue(
        "Correction: Added {nrow(to_add)} missing invert species to species table: ",
        "{paste(to_add$scientific_name, collapse = ', ')}"))
      corrections_applied <- corrections_applied + nrow(to_add)
    }
  }

  if (verbose) {
    message(glue::glue("Applied {corrections_applied} data correction(s)"))
  }

  invisible(con)
}

#' Enforce Column Types Before Export
#'
#' Runs `ALTER TABLE ... ALTER COLUMN ... TYPE` for every column whose current
#' DuckDB type differs from the target type. Intended to run right before
#' `write_parquet_outputs()` so that parquet files carry correct integer/decimal
#' types instead of DOUBLE from R's default numeric mapping.
#'
#' Target types come from two sources:
#' 1. `d_flds_rd` (field redefinition data frame) — keyed by `tbl_new.fld_new`
#'    with target in `type_new`
#' 2. `type_overrides` — named list for derived-table columns not in the
#'    redefinition file, e.g. `list(ichthyo.species_id = "SMALLINT")`
#'
#' @param con DuckDB connection
#' @param d_flds_rd Field redefinition data frame with columns `tbl_new`,
#'   `fld_new`, `type_new` (optional; NULL skips this source)
#' @param type_overrides Named list of `table.column = "SQL_TYPE"` overrides
#'   for derived-table columns (optional; NULL skips)
#' @param tables Character vector of tables to enforce. If NULL, uses all
#'   tables from `DBI::dbListTables(con)`.
#' @param verbose Print messages for each change (default: TRUE)
#'
#' @return Tibble of changes applied (table, column, from_type, to_type, success)
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' changes <- enforce_column_types(
#'   con            = con,
#'   d_flds_rd      = d$d_flds_rd,
#'   type_overrides = list(
#'     ichthyo.species_id = "SMALLINT",
#'     ichthyo.tally      = "INTEGER"),
#'   verbose        = TRUE)
#' }
#' @importFrom DBI dbGetQuery dbExecute dbListTables
#' @importFrom glue glue
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows
enforce_column_types <- function(
    con,
    d_flds_rd      = NULL,
    type_overrides = NULL,
    tables         = NULL,
    verbose        = TRUE) {

  # get table list
  if (is.null(tables)) {
    tables <- DBI::dbListTables(con)
  }

  # query current types for all columns in target tables
  tbl_list <- paste0("'", tables, "'", collapse = ", ")
  current_cols <- DBI::dbGetQuery(con, glue::glue("
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
      AND table_name IN ({tbl_list})
    ORDER BY table_name, ordinal_position"))

  # map csv type names to DuckDB SQL types
  type_map <- c(
    smallint  = "SMALLINT",
    integer   = "INTEGER",
    bigint    = "BIGINT",
    decimal   = "DOUBLE",
    varchar   = "VARCHAR",
    uuid      = "UUID",
    date      = "DATE",
    timestamp = "TIMESTAMP",
    text      = "VARCHAR",
    boolean   = "BOOLEAN")

  # build target type lookup: key = "table.column", value = DuckDB SQL type

  targets <- list()

  # source 1: field redefinition file
  if (!is.null(d_flds_rd)) {
    required_cols <- c("tbl_new", "fld_new", "type_new")
    missing_cols  <- setdiff(required_cols, names(d_flds_rd))
    if (length(missing_cols) > 0) {
      stop(
        "d_flds_rd is missing required column(s): ",
        paste(missing_cols, collapse = ", "),
        "\n  Found columns: ", paste(names(d_flds_rd), collapse = ", "),
        call. = FALSE)
    }

    for (i in seq_len(nrow(d_flds_rd))) {
      tbl_new  <- d_flds_rd$tbl_new[i]
      fld_new  <- d_flds_rd$fld_new[i]
      type_new <- d_flds_rd$type_new[i]

      if (is.na(tbl_new) || is.na(fld_new) || is.na(type_new)) next

      # map to DuckDB type
      type_lower <- tolower(trimws(type_new))
      duckdb_type <- type_map[type_lower]
      if (!is.na(duckdb_type)) {
        targets[[paste0(tbl_new, ".", fld_new)]] <- unname(duckdb_type)
      }
    }
  }

  # source 2: explicit overrides (take precedence)
  if (!is.null(type_overrides)) {
    for (key in names(type_overrides)) {
      targets[[key]] <- toupper(type_overrides[[key]])
    }
  }

  # compare current vs target and alter where needed
  changes <- list()

  for (i in seq_len(nrow(current_cols))) {
    tbl      <- current_cols$table_name[i]
    col      <- current_cols$column_name[i]
    cur_type <- toupper(current_cols$data_type[i])
    key      <- paste0(tbl, ".", col)

    target_type <- targets[[key]]
    if (is.null(target_type)) next

    # normalize for comparison (DuckDB reports some types differently)
    normalize <- function(t) {
      t <- toupper(t)
      # DuckDB reports SMALLINT as SMALLINT, INTEGER as INTEGER, etc.
      # but DOUBLE can show as DOUBLE or FLOAT
      t
    }

    if (normalize(cur_type) == normalize(target_type)) next

    # attempt the alter
    sql <- glue::glue(
      'ALTER TABLE "{tbl}" ALTER COLUMN "{col}" TYPE {target_type}')

    success <- tryCatch({
      DBI::dbExecute(con, sql)
      if (verbose) {
        message(glue::glue(
          "  {tbl}.{col}: {cur_type} -> {target_type}"))
      }
      TRUE
    }, error = function(e) {
      warning(glue::glue(
        "Failed to alter {tbl}.{col} from {cur_type} to {target_type}: {e$message}"))
      FALSE
    })

    changes <- append(changes, list(tibble::tibble(
      table     = tbl,
      column    = col,
      from_type = cur_type,
      to_type   = target_type,
      success   = success)))
  }

  result <- if (length(changes) > 0) {
    dplyr::bind_rows(changes)
  } else {
    tibble::tibble(
      table     = character(0),
      column    = character(0),
      from_type = character(0),
      to_type   = character(0),
      success   = logical(0))
  }

  if (verbose) {
    n_ok   <- sum(result$success)
    n_fail <- sum(!result$success)
    message(glue::glue(
      "Enforced column types: {n_ok} changed, {n_fail} failed, ",
      "{nrow(current_cols) - nrow(result)} already correct"))
  }

  result
}

#' Write Tables to Parquet Files
#'
#' Exports all tables from a DuckDB connection to parquet files.
#' Optionally strips provenance columns for public releases.
#'
#' @param con DuckDB connection
#' @param output_dir Directory for parquet files
#' @param tables Optional vector of table names to export. If NULL, exports all tables.
#' @param partition_by Named list mapping table names to partition column(s),
#'   e.g. `list(ctd_measurement = "cruise_key")`. Creates hive-partitioned
#'   subdirectories.
#' @param sort_by Named list mapping table names to sort column(s) for
#'   optimized row group statistics. For Hilbert-curve spatial sorting,
#'   use `"hilbert:lon_col,lat_col"`. Example:
#'   `list(ctd_measurement = c("measurement_type", "depth_m"),
#'         site = "hilbert:longitude,latitude")`.
#' @param strip_provenance Remove provenance columns (_source_*, _ingested_at) (default: TRUE)
#' @param compression Parquet compression codec (default: "snappy")
#' @param mismatches Optional named list of mismatch tibbles to include in
#'   manifest.json. Expected names: `ships`, `measurement_types`, `cruise_keys`.
#'   Each element should be a tibble (or data frame) describing unresolved
#'   entities. Zero-row tibbles are recorded as empty arrays.
#' @param supplemental Character vector of table names that are supplemental
#'   outputs (e.g. wide-format tables for ERDDAP). These are written to parquet
#'   and listed in manifest.json under `"supplemental"`, but are excluded by
#'   default from downstream database loading via [load_prior_tables()].
#'
#' @return Tibble with export statistics (table, rows, file_size, path)
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con("calcofi.duckdb")
#' stats <- write_parquet_outputs(con, "output/parquet")
#' }
#' @importFrom DBI dbListTables dbGetQuery dbExecute
#' @importFrom glue glue
#' @importFrom tibble tibble
#' @importFrom purrr map_dfr
write_parquet_outputs <- function(
    con,
    output_dir,
    tables           = NULL,
    partition_by     = NULL,
    sort_by          = NULL,
    strip_provenance = TRUE,
    compression      = "snappy",
    mismatches       = NULL,
    supplemental     = NULL) {

  # ensure output directory exists
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  # get table list
  if (is.null(tables)) {
    tables <- DBI::dbListTables(con)
    # filter out system tables
    tables <- tables[!grepl("^_", tables)]
  }

  # normalize partition_by to named list: table -> partition column(s)
  if (is.null(partition_by)) {
    part_map <- list()
  } else if (is.character(partition_by) && is.null(names(partition_by))) {
    # unnamed character vector: applies to all tables that have that column
    part_map <- stats::setNames(
      rep(list(partition_by), length(tables)), tables)
  } else {
    part_map <- as.list(partition_by)
  }

  # normalize sort_by to named list: table -> sort column(s)
  sort_map <- if (is.null(sort_by)) list() else as.list(sort_by)

  # provenance columns to strip
  prov_cols <- c("_source_file", "_source_row", "_source_uuid", "_ingested_at")

  # export each table
  stats <- purrr::map_dfr(tables, function(tbl) {

    # get column list
    cols <- DBI::dbGetQuery(con, glue::glue(
      "SELECT column_name FROM information_schema.columns
       WHERE table_name = '{tbl}'"))$column_name

    # determine columns to export
    if (strip_provenance) {
      export_cols <- setdiff(cols, prov_cols)
    } else {
      export_cols <- cols
    }

    select_clause <- paste(export_cols, collapse = ", ")

    # check if this table should be partitioned
    part_cols <- part_map[[tbl]]
    is_partitioned <- !is.null(part_cols) && all(part_cols %in% cols)

    # get row count
    n_rows <- DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) AS n FROM {tbl}"))$n

    # determine output path
    if (is_partitioned) {
      output_path <- file.path(output_dir, tbl)
    } else {
      output_path <- file.path(output_dir, paste0(tbl, ".parquet"))
    }

    # check if we can skip this table (output exists with same row count)
    skip <- FALSE
    if (is_partitioned && dir.exists(output_path)) {
      existing_n <- tryCatch(
        DBI::dbGetQuery(con, glue::glue(
          "SELECT COUNT(*) AS n
           FROM read_parquet('{output_path}/**/*.parquet')"))$n,
        error = function(e) -1)
      skip <- (existing_n == n_rows)
    } else if (!is_partitioned && file.exists(output_path)) {
      existing_n <- tryCatch(
        DBI::dbGetQuery(con, glue::glue(
          "SELECT COUNT(*) AS n FROM read_parquet('{output_path}')"))$n,
        error = function(e) -1)
      skip <- (existing_n == n_rows)
    }

    # for partitioned tables, also check partition directory names match
    if (skip && is_partitioned && dir.exists(output_path)) {
      existing_parts <- list.dirs(output_path, recursive = FALSE,
                                  full.names = FALSE)
      existing_parts <- existing_parts[grepl("=", existing_parts)]
      existing_vals  <- sort(gsub("^[^=]+=", "", existing_parts))
      current_vals   <- sort(as.character(DBI::dbGetQuery(con, glue::glue(
        "SELECT DISTINCT {part_cols[1]} FROM {tbl}"))[[1]]))
      if (!identical(existing_vals, current_vals)) {
        skip <- FALSE
        message(glue::glue(
          "Partition values changed for {tbl} — forcing re-write"))
      }
    }

    if (skip) {
      file_size <- if (is_partitioned) {
        part_files <- list.files(output_path, "\\.parquet$",
                                 recursive = TRUE, full.names = TRUE)
        sum(file.info(part_files)$size)
      } else {
        file.info(output_path)$size
      }
      message(glue::glue(
        "Skipped {tbl}: {format(n_rows, big.mark = ',')} rows unchanged"))
      return(tibble::tibble(
        table = tbl, rows = n_rows, file_size = file_size,
        path = output_path, partitioned = is_partitioned))
    }

    # build ORDER BY clause from sort_by
    sort_cols <- sort_map[[tbl]]
    order_clause <- ""
    if (!is.null(sort_cols)) {
      # handle hilbert spatial sort: "hilbert:lon_col,lat_col"
      # uses ST_Hilbert(x, y, bounds) from DuckDB spatial extension
      order_parts <- vapply(sort_cols, function(sc) {
        if (grepl("^hilbert:", sc)) {
          coords <- trimws(strsplit(sub("^hilbert:", "", sc), ",")[[1]])
          paste0(
            "ST_Hilbert(", coords[1], ", ", coords[2],
            ", {min_x: -180, min_y: -90, max_x: 180, max_y: 90}::BOX_2D)")
        } else {
          sc
        }
      }, character(1))
      order_clause <- paste(" ORDER BY", paste(order_parts, collapse = ", "))
    }

    if (is_partitioned) {
      # hive-partitioned output to subdirectory
      if (dir.exists(output_path)) unlink(output_path, recursive = TRUE)
      part_clause <- paste(part_cols, collapse = ", ")

      DBI::dbExecute(con, paste0(
        "COPY (SELECT ", select_clause, " FROM ", tbl, order_clause, ")",
        " TO '", output_path, "'",
        " (FORMAT PARQUET, PARTITION_BY (", part_clause, "),",
        " COMPRESSION '", compression, "', OVERWRITE_OR_IGNORE)"))

      # sum file sizes across partition files
      part_files <- list.files(output_path, pattern = "\\.parquet$",
                               recursive = TRUE, full.names = TRUE)
      file_size <- sum(file.info(part_files)$size)
    } else {
      # single parquet file
      DBI::dbExecute(con, paste0(
        "COPY (SELECT ", select_clause, " FROM ", tbl, order_clause, ")",
        " TO '", output_path, "'",
        " (FORMAT PARQUET, COMPRESSION '", compression, "')"))

      file_size <- file.info(output_path)$size
    }

    size_label <- if (file_size > 1024^3) {
      paste0(round(file_size / 1024^3, 2), " GB")
    } else {
      paste0(round(file_size / 1024^2, 2), " MB")
    }
    part_label <- if (is_partitioned) " (partitioned)" else ""
    sort_label <- if (!is.null(sort_cols))
      paste0(" sorted:", paste(sort_cols, collapse = ",")) else ""
    message(glue::glue(
      "Exported {tbl}: {n_rows} rows, {size_label}{part_label}{sort_label}"))

    tibble::tibble(
      table       = tbl,
      rows        = n_rows,
      file_size   = file_size,
      path        = output_path,
      partitioned = is_partitioned)
  })

  # write manifest
  manifest <- list(
    tables           = stats$table,
    total_rows       = sum(stats$rows),
    total_size_bytes = sum(stats$file_size),
    metadata_file    = "metadata.json",
    partitioned      = stats$table[stats$partitioned],
    supplemental     = if (!is.null(supplemental))
      intersect(supplemental, stats$table) else list(),
    sort_by          = if (length(sort_map) > 0) sort_map else list(),
    files            = stats |> dplyr::select(-partitioned) |> as.list())

  # append mismatches if provided

  if (!is.null(mismatches)) {
    # convert tibbles to list-of-lists for JSON
    manifest$mismatches <- lapply(mismatches, function(mm) {
      if (is.data.frame(mm) && nrow(mm) > 0) {
        lapply(seq_len(nrow(mm)), function(i) as.list(mm[i, ]))
      } else {
        list()
      }
    })
    n_issues <- sum(vapply(manifest$mismatches, length, integer(1)))
    if (n_issues > 0) {
      message(glue::glue("Manifest includes {n_issues} mismatch(es) for resolution"))
    }
  }

  manifest_path <- file.path(output_dir, "manifest.json")
  jsonlite::write_json(manifest, manifest_path, auto_unbox = TRUE, pretty = TRUE)
  message(glue::glue("Wrote manifest to {manifest_path}"))

  stats
}

#' Build Metadata JSON for Parquet Outputs
#'
#' Creates a sidecar `metadata.json` file alongside parquet outputs that
#' documents every table and column. DuckDB `COMMENT ON` does not propagate
#' to parquet via `COPY TO`, so this provides the metadata externally.
#'
#' Metadata is assembled from three sources:
#' 1. Table/field redefinition files (`d_tbls_rd`, `d_flds_rd`)
#' 2. A derived metadata CSV for workflow-created tables/columns
#' 3. Auto-generated stubs for any remaining undocumented columns
#'
#' Optionally sets DuckDB `COMMENT ON` for tables and columns.
#'
#' @param con DuckDB connection
#' @param d_tbls_rd Table redefinition data frame (with `tbl_new`, `tbl_description`)
#' @param d_flds_rd Field redefinition data frame (with `tbl_new`, `fld_new`,
#'   `fld_description`, `units`)
#' @param metadata_derived_csv Path to CSV with derived table/column metadata
#'   (columns: table, column, name_long, units, description_md)
#' @param output_dir Directory to write `metadata.json`
#' @param tables Character vector of table names to include. If NULL, uses all
#'   tables from DuckDB.
#' @param set_comments If TRUE, also sets DuckDB `COMMENT ON` for tables/columns
#' @param provider Data provider identifier (e.g. "swfsc")
#' @param dataset Dataset identifier (e.g. "ichthyo")
#' @param workflow_url URL to the rendered workflow page
#' @param tables_owned Optional list describing the tables this ingest owns,
#'   as parsed from the `calcofi$tables_owned` YAML block: a list of entries
#'   each with `table` and optional `shared` (logical) / `note`. When supplied,
#'   a `contributions` block (per-table row counts) is emitted for these tables
#'   only, so reference tables loaded from prior ingests are not mis-attributed.
#'
#' @return Path to the created `metadata.json` file
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' build_metadata_json(
#'   con                  = con,
#'   d_tbls_rd            = d$d_tbls_rd,
#'   d_flds_rd            = d$d_flds_rd,
#'   metadata_derived_csv = "metadata/swfsc/ichthyo/metadata_derived.csv",
#'   output_dir           = "data/parquet/swfsc_ichthyo",
#'   tables               = DBI::dbListTables(con),
#'   provider             = "swfsc",
#'   dataset              = "ichthyo",
#'   workflow_url         = "https://calcofi.io/workflows/ingest_swfsc_ichthyo.html")
#' }
#' @importFrom DBI dbGetQuery dbListTables
#' @importFrom glue glue
#' @importFrom jsonlite write_json
#' @importFrom readr read_csv
#' @importFrom stringr str_replace_all str_to_title
build_metadata_json <- function(
    con,
    d_tbls_rd,
    d_flds_rd,
    metadata_derived_csv = NULL,
    output_dir,
    tables        = NULL,
    set_comments  = TRUE,
    provider      = NULL,
    dataset       = NULL,
    workflow_url  = NULL,
    tables_owned  = NULL) {

  # get tables from duckdb if not specified
  if (is.null(tables)) {
    tables <- DBI::dbListTables(con)
  }

  # get all columns from information_schema
  all_cols <- DBI::dbGetQuery(con, "
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
    ORDER BY table_name, ordinal_position")
  all_cols <- all_cols[all_cols$table_name %in% tables, ]

  # helper: convert snake_case to Title Case
  snake_to_title <- function(x) {
    x |>
      stringr::str_replace_all("_", " ") |>
      stringr::str_to_title()
  }

  # --- build table metadata ---
  tables_meta <- list()
  for (tbl in tables) {
    # look up description from redefinition file
    rd_row <- d_tbls_rd[d_tbls_rd$tbl_new == tbl, ]
    if (nrow(rd_row) > 0 && !is.na(rd_row$tbl_description[1]) && rd_row$tbl_description[1] != "") {
      desc <- rd_row$tbl_description[1]
    } else {
      desc <- ""
    }
    tables_meta[[tbl]] <- list(
      name_long      = snake_to_title(tbl),
      description_md = desc)
  }

  # --- build column metadata ---
  columns_meta <- list()
  for (i in seq_len(nrow(all_cols))) {
    tbl <- all_cols$table_name[i]
    col <- all_cols$column_name[i]
    key <- paste0(tbl, ".", col)

    # look up from field redefinition
    rd_row <- d_flds_rd[d_flds_rd$tbl_new == tbl & d_flds_rd$fld_new == col, ]

    if (nrow(rd_row) > 0) {
      desc  <- if (!is.null(rd_row$fld_description[1]) && !is.na(rd_row$fld_description[1]))
        rd_row$fld_description[1] else ""
      units <- if ("units" %in% names(rd_row) && !is.null(rd_row$units[1]) && !is.na(rd_row$units[1]) && rd_row$units[1] != "")
        rd_row$units[1] else NULL
    } else {
      desc  <- ""
      units <- NULL
    }

    columns_meta[[key]] <- list(
      name_long      = snake_to_title(col),
      units          = units,
      description_md = desc)
  }

  # --- overlay with metadata_derived.csv ---
  if (!is.null(metadata_derived_csv) && file.exists(metadata_derived_csv)) {
    d_derived <- readr::read_csv(metadata_derived_csv, show_col_types = FALSE)

    for (i in seq_len(nrow(d_derived))) {
      row <- d_derived[i, ]
      tbl <- row$table

      if (is.na(row$column) || row$column == "") {
        # table-level metadata
        if (tbl %in% names(tables_meta)) {
          if (!is.na(row$name_long) && row$name_long != "")
            tables_meta[[tbl]]$name_long <- row$name_long
          if (!is.na(row$description_md) && row$description_md != "")
            tables_meta[[tbl]]$description_md <- row$description_md
        }
      } else {
        # column-level metadata
        key <- paste0(tbl, ".", row$column)
        if (key %in% names(columns_meta) || paste0(tbl, ".", row$column) %in%
            paste0(all_cols$table_name, ".", all_cols$column_name)) {
          entry <- columns_meta[[key]]
          if (is.null(entry)) entry <- list()

          if (!is.na(row$name_long) && row$name_long != "")
            entry$name_long <- row$name_long
          if (!is.na(row$units) && row$units != "")
            entry$units <- row$units
          else if (is.null(entry$units))
            entry$units <- NULL
          if (!is.na(row$description_md) && row$description_md != "")
            entry$description_md <- row$description_md

          columns_meta[[key]] <- entry
        }
      }
    }
  }

  # --- fill stubs for any missing columns ---
  for (i in seq_len(nrow(all_cols))) {
    key <- paste0(all_cols$table_name[i], ".", all_cols$column_name[i])
    if (!key %in% names(columns_meta)) {
      columns_meta[[key]] <- list(
        name_long      = snake_to_title(all_cols$column_name[i]),
        units          = NULL,
        description_md = "")
    }
    # ensure name_long is populated
    if (is.null(columns_meta[[key]]$name_long) || columns_meta[[key]]$name_long == "") {
      columns_meta[[key]]$name_long <- snake_to_title(all_cols$column_name[i])
    }
  }

  # --- set duckdb comments ---
  if (set_comments) {
    for (tbl in names(tables_meta)) {
      desc <- tables_meta[[tbl]]$description_md
      if (!is.null(desc) && desc != "") {
        tryCatch(
          set_duckdb_comments(con, table = tbl, table_comment = desc),
          error = function(e) {
            warning(glue::glue("Failed to set comment on table {tbl}: {e$message}"))
          })
      }

      # column comments for this table
      col_keys <- grep(paste0("^", tbl, "\\."), names(columns_meta), value = TRUE)
      col_comments <- list()
      for (ck in col_keys) {
        col_name <- sub(paste0("^", tbl, "\\."), "", ck)
        col_desc <- columns_meta[[ck]]$description_md
        if (!is.null(col_desc) && col_desc != "") {
          col_comments[[col_name]] <- col_desc
        }
      }
      if (length(col_comments) > 0) {
        tryCatch(
          set_duckdb_comments(con, table = tbl, column_comments = col_comments),
          error = function(e) {
            warning(glue::glue("Failed to set column comments on {tbl}: {e$message}"))
          })
      }
    }
    message("Set DuckDB COMMENT ON for tables and columns")
  }

  # --- per-dataset row contributions (owned tables only) ---
  # each ingest's DuckDB holds only its own rows, so COUNT(*) per owned table
  # is this dataset's clean contribution — even for shared registry tables
  # (e.g. measurement_type). reference tables loaded from prior ingests are
  # excluded because they are not listed in tables_owned.
  contributions <- NULL
  if (!is.null(tables_owned) && length(tables_owned) > 0) {
    contributions <- list()
    db_tbls <- DBI::dbListTables(con)
    for (ent in tables_owned) {
      tbl <- ent$table
      if (is.null(tbl) || !(tbl %in% db_tbls)) {
        if (!is.null(tbl)) {
          warning(glue::glue(
            "build_metadata_json: tables_owned lists '{tbl}' but it is not ",
            "in the connection — skipping its contribution"))
        }
        next
      }
      n <- DBI::dbGetQuery(con, glue::glue(
        "SELECT COUNT(*) AS n FROM {DBI::dbQuoteIdentifier(con, tbl)}"))$n
      contributions[[tbl]] <- list(
        rows   = as.integer(n),
        owned  = TRUE,
        shared = isTRUE(ent$shared))
    }
  }

  # --- assemble metadata json ---
  metadata <- list(
    schema_version = "1.1",
    provider       = provider,
    dataset        = dataset,
    workflow       = workflow_url,
    tables         = tables_meta,
    columns        = columns_meta)
  if (!is.null(contributions)) metadata$contributions <- contributions

  # write json
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  metadata_path <- file.path(output_dir, "metadata.json")
  jsonlite::write_json(metadata, metadata_path, auto_unbox = TRUE, pretty = TRUE, null = "null")

  message(glue::glue(
    "Wrote metadata.json: {length(tables_meta)} tables, {length(columns_meta)} columns"))

  metadata_path
}

#' Build Relationships JSON from dm Object
#'
#' Extracts primary keys and foreign keys from a \code{dm} object and writes
#' a \code{relationships.json} sidecar file alongside parquet outputs. Since
#' parquet files cannot store table relationships natively, this provides a
#' machine-readable source of truth for PKs and FKs.
#'
#' @param dm A \code{dm} object with PKs and FKs defined. Deprecated in
#'   favour of \code{rels}; kept for backward compatibility.
#' @param rels A list with \code{primary_keys} (named list: table → column)
#'   and \code{foreign_keys} (list of lists with \code{table}, \code{column},
#'   \code{ref_table}, \code{ref_column}). Takes precedence over \code{dm}.
#' @param output_dir Directory to write \code{relationships.json}
#' @param provider Data provider identifier (e.g. "swfsc")
#' @param dataset Dataset identifier (e.g. "ichthyo")
#'
#' @return Path to the created \code{relationships.json} file
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' # preferred: pass relationships as a list
#' rels <- list(
#'   primary_keys = list(cruise = "cruise_key", ship = "ship_key"),
#'   foreign_keys = list(
#'     list(table = "cruise", column = "ship_key",
#'          ref_table = "ship", ref_column = "ship_key")))
#' build_relationships_json(
#'   rels       = rels,
#'   output_dir = "data/parquet/swfsc_ichthyo",
#'   provider   = "swfsc",
#'   dataset    = "ichthyo")
#' }
#' @importFrom jsonlite write_json
#' @importFrom glue glue
build_relationships_json <- function(
    dm         = NULL,
    rels       = NULL,
    output_dir,
    provider   = NULL,
    dataset    = NULL) {

  if (!is.null(rels)) {
    # list-based path (preferred)
    primary_keys <- rels$primary_keys %||% list()
    foreign_keys <- rels$foreign_keys %||% list()
  } else if (!is.null(dm)) {
    # dm-based path (backward compat)
    pks_df <- dm::dm_get_all_pks(dm)
    primary_keys <- list()
    for (i in seq_len(nrow(pks_df))) {
      tbl <- as.character(pks_df$table[i])
      col <- as.character(pks_df$pk_col[[i]])
      primary_keys[[tbl]] <- col
    }

    fks_df <- dm::dm_get_all_fks(dm)
    foreign_keys <- list()
    for (i in seq_len(nrow(fks_df))) {
      foreign_keys[[i]] <- list(
        table      = as.character(fks_df$child_table[i]),
        column     = as.character(fks_df$child_fk_cols[[i]]),
        ref_table  = as.character(fks_df$parent_table[i]),
        ref_column = as.character(fks_df$parent_key_cols[[i]]))
    }
  } else {
    stop("Either 'dm' or 'rels' must be provided")
  }

  # assemble json structure
  relationships <- list(
    schema_version = "1.0",
    provider       = provider,
    dataset        = dataset,
    primary_keys   = primary_keys,
    foreign_keys   = foreign_keys)

  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  out_path <- file.path(output_dir, "relationships.json")
  jsonlite::write_json(
    relationships, out_path,
    auto_unbox = TRUE, pretty = TRUE, null = "null")

  message(glue::glue(
    "Wrote relationships.json: {length(primary_keys)} PKs, ",
    "{length(foreign_keys)} FKs"))

  out_path
}

#' Read Relationships JSON and Optionally Apply to dm
#'
#' Reads a \code{relationships.json} file. If a \code{dm} object is provided,
#' applies the PKs and FKs from the JSON to it. Otherwise returns the parsed
#' list.
#'
#' @param path Path to \code{relationships.json} file
#' @param dm Optional \code{dm} object to apply relationships to
#'
#' @return If \code{dm} is provided, returns the dm with PKs/FKs applied.
#'   Otherwise returns the parsed JSON as a list.
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' # read and apply to dm
#' d <- dm::dm_from_con(con, learn_keys = FALSE)
#' d <- read_relationships_json("relationships.json", dm = d)
#' dm::dm_draw(d)
#'
#' # read as raw list
#' rels <- read_relationships_json("relationships.json")
#' }
#' @importFrom jsonlite fromJSON
#' @importFrom dm dm_add_pk dm_add_fk
read_relationships_json <- function(path, dm = NULL) {
  rels <- jsonlite::fromJSON(path, simplifyVector = FALSE)

  if (is.null(dm)) {
    return(rels)
  }

  # get tables present in dm

  dm_tables <- dm::dm_get_tables(dm) |> names()

  # apply primary keys
  for (tbl in names(rels$primary_keys)) {
    if (tbl %in% dm_tables) {
      col <- rels$primary_keys[[tbl]]
      dm <- tryCatch(
        dm::dm_add_pk(dm, !!tbl, !!col),
        error = function(e) dm)
    }
  }

  # apply foreign keys
  for (fk in rels$foreign_keys) {
    if (fk$table %in% dm_tables && fk$ref_table %in% dm_tables) {
      dm <- tryCatch(
        dm::dm_add_fk(dm, !!fk$table, !!fk$column, !!fk$ref_table, !!fk$ref_column),
        error = function(e) dm)
    }
  }

  dm
}

#' Merge Multiple Relationships JSON Files
#'
#' Combines primary keys and foreign keys from multiple per-dataset
#' \code{relationships.json} files into a single merged file. PKs use
#' last-writer-wins for shared tables; FKs are concatenated and deduplicated.
#'
#' @param paths Character vector of paths to \code{relationships.json} files
#' @param output_path Path for the merged output file
#'
#' @return Path to the merged \code{relationships.json} file
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' merge_relationships_json(
#'   paths = c(
#'     "data/parquet/swfsc_ichthyo/relationships.json",
#'     "data/parquet/calcofi_bottle/relationships.json"),
#'   output_path = "data/releases/v2026.03/relationships.json")
#' }
#' @importFrom jsonlite fromJSON write_json
merge_relationships_json <- function(paths, output_path) {
  merged_pks <- list()
  all_fks    <- list()

  for (path in paths) {
    rels <- jsonlite::fromJSON(path, simplifyVector = FALSE)

    # merge PKs (last-writer-wins for shared tables)
    for (tbl in names(rels$primary_keys)) {
      merged_pks[[tbl]] <- rels$primary_keys[[tbl]]
    }

    # collect FKs
    for (fk in rels$foreign_keys) {
      all_fks <- append(all_fks, list(fk))
    }
  }

  # deduplicate FKs by (table, column, ref_table, ref_column)
  fk_keys <- vapply(all_fks, function(fk) {
    paste(fk$table, fk$column, fk$ref_table, fk$ref_column, sep = "|")
  }, character(1))
  all_fks <- all_fks[!duplicated(fk_keys)]

  merged <- list(
    schema_version = "1.0",
    provider       = "calcofi",
    dataset        = "calcofi-db-release",
    primary_keys   = merged_pks,
    foreign_keys   = all_fks)

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(
    merged, output_path,
    auto_unbox = TRUE, pretty = TRUE, null = "null")

  message(glue::glue(
    "Merged relationships.json: {length(merged_pks)} PKs, ",
    "{length(all_fks)} FKs from {length(paths)} files"))

  output_path
}

# extract the YAML front-matter block (between the leading `---` fences) of a
# .qmd/.Rmd/.md file and parse it. returns an empty list when absent.
.read_yaml_front_matter <- function(path) {
  lines <- readLines(path, warn = FALSE)
  fences <- which(grepl("^---\\s*$", lines))
  # front matter must open within the first few lines
  if (length(fences) < 2 || fences[1] > 3) return(list())
  body <- lines[(fences[1] + 1):(fences[2] - 1)]
  tryCatch(
    yaml::yaml.load(paste(body, collapse = "\n")) %||% list(),
    error = function(e) {
      warning(glue::glue("Failed to parse YAML front matter in {path}: {e$message}"))
      list()
    })
}

#' Read calcofi YAML blocks from ingest_*.qmd front matter
#'
#' Reads the \code{calcofi:} block from the YAML front matter of each
#' \code{ingest_*.qmd} workflow and returns it keyed by
#' \code{"\{provider\}_\{dataset\}"}. This is the authoritative source for
#' dataset-level metadata, table ownership, workflow links, and ERD colors
#' consumed by [merge_metadata_json()] and \code{release_database.qmd}.
#'
#' @param workflow_dir Directory containing the \code{ingest_*.qmd} files.
#' @param pattern Regular expression matching ingest filenames
#'   (default \code{"^ingest_.*\\\\.qmd$"}).
#'
#' @return A named list keyed by \code{provider_dataset}. Each element is the
#'   parsed \code{calcofi} block, augmented with \code{provider_dataset} and
#'   \code{qmd} (the source file path). Ingests whose block lacks
#'   \code{provider}/\code{dataset} are skipped with a warning.
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' ingest_yaml <- read_ingest_yaml("workflows")
#' names(ingest_yaml)            # "swfsc_ichthyo" "calcofi_bottle" ...
#' ingest_yaml$calcofi_bottle$tables_owned
#' }
read_ingest_yaml <- function(workflow_dir, pattern = "^ingest_.*\\.qmd$") {
  stopifnot(dir.exists(workflow_dir))
  qmds <- list.files(workflow_dir, pattern = pattern, full.names = TRUE)
  out <- list()
  for (qmd in qmds) {
    cc <- read_calcofi_meta(qmd)
    if (is.null(cc) || is.null(cc$provider) || is.null(cc$dataset)) {
      next
    }
    out[[cc$provider_dataset]] <- cc
  }
  out
}

#' Read the calcofi YAML block from a single workflow file
#'
#' @param qmd_path Path to one \code{ingest_*.qmd} (or any .qmd with a
#'   \code{calcofi:} YAML block).
#'
#' @return The parsed \code{calcofi} block as a list, augmented with
#'   \code{provider_dataset} and \code{qmd}, or \code{NULL} if absent. Use this
#'   in an ingest's setup chunk to read its own \code{provider}/\code{dataset}/
#'   \code{tables_owned} from the authoritative YAML rather than hard-coding.
#' @export
#' @concept wrangle
read_calcofi_meta <- function(qmd_path) {
  stopifnot(file.exists(qmd_path))
  cc <- .read_yaml_front_matter(qmd_path)$calcofi
  if (is.null(cc)) return(NULL)
  if (!is.null(cc$provider) && !is.null(cc$dataset)) {
    cc$provider_dataset <- paste0(cc$provider, "_", cc$dataset)
  }
  cc$qmd <- qmd_path
  cc
}

#' Build the dataset registry table from ingest YAML blocks
#'
#' Assembles a data frame mirroring the legacy \code{metadata/dataset.csv}
#' (the authoritative \code{dataset} registry table written into each ingest's
#' database) from the \code{dataset_meta}/\code{tables_owned} YAML blocks
#' returned by [read_ingest_yaml()]. Replaces reading \code{dataset.csv}.
#'
#' @param ingest_yaml Named list from [read_ingest_yaml()].
#'
#' @return A [tibble][tibble::tibble] with one row per dataset (including any
#'   \code{additional_datasets} folded into an ingest) and the columns of the
#'   legacy \code{dataset.csv}.
#' @export
#' @concept wrangle
#' @importFrom tibble tibble
ingest_yaml_to_dataset_df <- function(ingest_yaml) {
  join_semi <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    paste(unlist(x), collapse = ";")
  }
  s <- function(x) if (is.null(x)) NA_character_ else as.character(x)

  rows <- list()
  add_row <- function(provider, dataset, m, tables_owned = NULL) {
    m <- m %||% list()
    tbls <- if (!is.null(tables_owned)) {
      vapply(tables_owned, function(e) e$table %||% NA_character_, character(1))
    } else {
      m$tables
    }
    rows[[length(rows) + 1]] <<- tibble::tibble(
      provider          = s(provider),
      dataset           = s(dataset),
      dataset_name      = s(m$dataset_name),
      description       = s(m$description),
      citation_main     = s(m$citation_main),
      citation_others   = join_semi(m$citation_others),
      link_calcofi_org  = s(m$link_calcofi_org),
      link_data_source  = s(m$link_data_source),
      link_others       = join_semi(m$link_others),
      tables            = join_semi(tbls),
      coverage_temporal = s(m$coverage_temporal),
      coverage_spatial  = s(m$coverage_spatial),
      license           = s(m$license),
      pi_names          = s(m$pi_names))
  }

  for (key in names(ingest_yaml)) {
    cc <- ingest_yaml[[key]]
    add_row(cc$provider, cc$dataset, cc$dataset_meta, cc$tables_owned)
    for (ad in cc$additional_datasets %||% list()) {
      add_row(ad$provider, ad$dataset, ad, ad$tables_owned)
    }
  }
  do.call(rbind, rows)
}

# build a standard datasets[] entry from a metadata block (list), dropping
# empty/NA fields. shared by the CSV and YAML code paths in merge_metadata_json.
.dataset_entry <- function(provider, dataset, m) {
  m <- m %||% list()
  pick <- function(k) {
    v <- m[[k]]
    if (is.null(v)) return(NULL)
    if (is.character(v) && length(v) == 1 && (is.na(v) || v == "")) return(NULL)
    v
  }
  list(
    provider          = provider,
    dataset           = dataset,
    dataset_name      = pick("dataset_name"),
    description       = pick("description"),
    citation_main     = pick("citation_main"),
    link_calcofi_org  = pick("link_calcofi_org"),
    link_data_source  = pick("link_data_source"),
    coverage_temporal = pick("coverage_temporal"),
    coverage_spatial  = pick("coverage_spatial"),
    license           = pick("license"),
    pi_names          = pick("pi_names"))
}

#' Merge Per-Ingest metadata.json into a Release-Level Sidecar
#'
#' Combines per-ingest \code{metadata.json} files (produced by
#' [build_metadata_json()]) into a single release-level \code{metadata.json}.
#' Adds release-only tables and columns from CSV registries plus optional
#' \code{dataset.csv} and \code{measurement_type.csv} blocks. Emits schema
#' version \code{"1.1"} alongside \code{catalog.json} and
#' \code{relationships.json} in a frozen release directory.
#'
#' Conflict rule: when the same \code{table} or \code{table.column} key
#' appears in multiple per-ingest files, the last path wins, but a warning
#' lists the duplicates so genuine drift between ingests is surfaced.
#'
#' @param paths Character vector of paths to per-ingest
#'   \code{metadata.json} files.
#' @param output_path Path for the merged output file.
#' @param release_version Optional release version string (e.g.
#'   \code{"v2026.05.14"}) written to the top-level \code{release_version}
#'   field.
#' @param release_tables_csv Optional path to a CSV with columns
#'   \code{table, name_long, description_md, provider, dataset} describing
#'   tables built inside \code{release_database.qmd} that have no per-ingest
#'   metadata.json (e.g. \code{cruise_summary}, \code{_spatial}).
#' @param release_columns_csv Optional path to a CSV with columns
#'   \code{table, column, name_long, units, description_md} for release-only
#'   columns.
#' @param measurement_type_csv Optional path to
#'   \code{metadata/measurement_type.csv}. When supplied, populates the
#'   \code{measurement_types} block with one entry per canonical type.
#' @param dataset_csv Optional path to \code{metadata/dataset.csv}. Deprecated
#'   fallback for the \code{datasets} block; superseded by \code{ingest_yaml}.
#'   When both are supplied, \code{ingest_yaml} wins.
#' @param ingest_yaml Optional named list from [read_ingest_yaml()] (keyed by
#'   \code{provider_dataset}). When supplied, the \code{datasets} block and the
#'   \code{erd_legend} are built from each ingest's \code{calcofi} YAML
#'   (authoritative source) rather than \code{dataset_csv}.
#' @param table_rows Optional named numeric vector (table name → release-final
#'   row count, e.g. from freeze stats). Used as the denominator when computing
#'   per-dataset contribution percentages.
#'
#' @return Path to the merged \code{metadata.json} file (invisibly returns
#'   \code{output_path}).
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' merge_metadata_json(
#'   paths = c(
#'     "data/parquet/swfsc_ichthyo/metadata.json",
#'     "data/parquet/calcofi_bottle/metadata.json",
#'     "data/parquet/calcofi_ctd-cast/metadata.json",
#'     "data/parquet/calcofi_dic/metadata.json"),
#'   output_path          = "data/releases/v2026.05.14/metadata.json",
#'   release_version      = "v2026.05.14",
#'   release_tables_csv   = "metadata/release_tables.csv",
#'   release_columns_csv  = "metadata/release_columns.csv",
#'   measurement_type_csv = "metadata/measurement_type.csv",
#'   dataset_csv          = "metadata/dataset.csv")
#' }
#' @importFrom jsonlite fromJSON write_json
#' @importFrom readr read_csv
#' @importFrom glue glue
merge_metadata_json <- function(
    paths,
    output_path,
    release_version      = NULL,
    release_tables_csv   = NULL,
    release_columns_csv  = NULL,
    measurement_type_csv = NULL,
    dataset_csv          = NULL,
    ingest_yaml          = NULL,
    table_rows           = NULL) {

  tables_meta  <- list()
  columns_meta <- list()
  datasets     <- list()

  dup_tables  <- character()
  dup_columns <- character()

  # per-ingest contributions, pivoted to table -> list of contributor records
  contrib_raw <- list()

  for (path in paths) {
    if (!file.exists(path)) {
      warning(glue::glue("merge_metadata_json: skipping missing path {path}"))
      next
    }
    meta <- jsonlite::fromJSON(path, simplifyVector = FALSE)

    prov <- meta$provider %||% NA_character_
    dset <- meta$dataset  %||% NA_character_
    wf   <- meta$workflow %||% NA_character_
    pd   <- paste0(prov, "_", dset)

    # record per-ingest tables (tagged with provider/dataset + workflow link)
    for (tbl in names(meta$tables)) {
      if (tbl %in% names(tables_meta)) dup_tables <- c(dup_tables, tbl)
      entry <- meta$tables[[tbl]]
      entry$provider <- prov
      entry$dataset  <- dset
      entry$workflow <- wf
      tables_meta[[tbl]] <- entry
    }

    # record per-ingest columns
    for (key in names(meta$columns)) {
      if (key %in% names(columns_meta)) dup_columns <- c(dup_columns, key)
      columns_meta[[key]] <- meta$columns[[key]]
    }

    # collect per-ingest row contributions (only tables the ingest owns)
    if (!is.null(meta$contributions)) {
      for (tbl in names(meta$contributions)) {
        c1 <- meta$contributions[[tbl]]
        contrib_raw[[tbl]] <- c(contrib_raw[[tbl]], list(list(
          provider_dataset = pd,
          rows             = c1$rows %||% 0,
          workflow         = wf)))
      }
    }
  }

  if (length(dup_tables) > 0) {
    warning(glue::glue(
      "merge_metadata_json: duplicate table keys across ingests ",
      "(last-writer-wins): {paste(unique(dup_tables), collapse = ', ')}"))
  }
  if (length(dup_columns) > 0) {
    warning(glue::glue(
      "merge_metadata_json: {length(unique(dup_columns))} duplicate ",
      "table.column keys across ingests (last-writer-wins). First few: ",
      "{paste(utils::head(unique(dup_columns), 5), collapse = ', ')}"))
  }

  # overlay release_tables.csv (release-only tables — supplement only, do not
  # override per-ingest metadata; only fill fields that are missing/empty)
  fill_if_empty <- function(entry, key, val) {
    if (is.na(val) || val == "") return(entry)
    cur <- entry[[key]]
    if (is.null(cur) || (is.character(cur) && (is.na(cur) || cur == ""))) {
      entry[[key]] <- val
    }
    entry
  }
  if (!is.null(release_tables_csv) && file.exists(release_tables_csv)) {
    d_rt <- readr::read_csv(release_tables_csv, show_col_types = FALSE)
    for (i in seq_len(nrow(d_rt))) {
      tbl <- d_rt$table[i]
      if (is.na(tbl) || tbl == "") next
      entry <- tables_meta[[tbl]] %||% list()
      if ("name_long"      %in% names(d_rt)) entry <- fill_if_empty(entry, "name_long",      d_rt$name_long[i])
      if ("description_md" %in% names(d_rt)) entry <- fill_if_empty(entry, "description_md", d_rt$description_md[i])
      if ("provider"       %in% names(d_rt)) entry <- fill_if_empty(entry, "provider",       d_rt$provider[i])
      if ("dataset"        %in% names(d_rt)) entry <- fill_if_empty(entry, "dataset",        d_rt$dataset[i])
      tables_meta[[tbl]] <- entry
    }
  }

  # overlay release_columns.csv (release-only columns — same gap-fill semantics)
  if (!is.null(release_columns_csv) && file.exists(release_columns_csv)) {
    d_rc <- readr::read_csv(release_columns_csv, show_col_types = FALSE)
    for (i in seq_len(nrow(d_rc))) {
      tbl <- d_rc$table[i]
      col <- d_rc$column[i]
      if (is.na(tbl) || is.na(col) || tbl == "" || col == "") next
      key <- paste0(tbl, ".", col)
      entry <- columns_meta[[key]] %||% list(name_long = NULL, units = NULL, description_md = "")
      if ("name_long"      %in% names(d_rc)) entry <- fill_if_empty(entry, "name_long",      d_rc$name_long[i])
      if ("units"          %in% names(d_rc)) entry <- fill_if_empty(entry, "units",          d_rc$units[i])
      if ("description_md" %in% names(d_rc)) entry <- fill_if_empty(entry, "description_md", d_rc$description_md[i])
      columns_meta[[key]] <- entry
    }
  }

  # measurement_types block
  measurement_types <- list()
  if (!is.null(measurement_type_csv) && file.exists(measurement_type_csv)) {
    d_mt <- readr::read_csv(measurement_type_csv, show_col_types = FALSE)
    for (i in seq_len(nrow(d_mt))) {
      mt <- d_mt$measurement_type[i]
      if (is.na(mt) || mt == "") next
      ds_vec <- NULL
      if ("_source_datasets" %in% names(d_mt) && !is.na(d_mt$`_source_datasets`[i]) &&
          nzchar(d_mt$`_source_datasets`[i])) {
        ds_vec <- trimws(strsplit(d_mt$`_source_datasets`[i], ";")[[1]])
        ds_vec <- ds_vec[nzchar(ds_vec)]
      }
      measurement_types[[mt]] <- list(
        description  = if (!is.na(d_mt$description[i]))   d_mt$description[i]   else "",
        units        = if (!is.na(d_mt$units[i]))         d_mt$units[i]         else NULL,
        is_canonical = if ("is_canonical" %in% names(d_mt)) isTRUE(d_mt$is_canonical[i]) else NA,
        datasets     = ds_vec)
    }
  }

  # datasets block + erd_legend — authoritative source is ingest_yaml; fall
  # back to the deprecated dataset.csv when YAML is not supplied
  erd_legend <- list()
  owned_tbls <- function(tables_owned) {
    if (is.null(tables_owned) || length(tables_owned) == 0) return(NULL)
    unname(vapply(tables_owned, function(e) e$table %||% NA_character_, character(1)))
  }
  if (!is.null(ingest_yaml)) {
    for (key in names(ingest_yaml)) {
      cc <- ingest_yaml[[key]]
      entry <- .dataset_entry(cc$provider, cc$dataset, cc$dataset_meta)
      entry$tables <- owned_tbls(cc$tables_owned)
      datasets[[key]] <- entry
      # datasets folded into one ingest (e.g. swfsc_invert inside ichthyo)
      for (ad in cc$additional_datasets %||% list()) {
        k2 <- paste0(ad$provider, "_", ad$dataset)
        e2 <- .dataset_entry(ad$provider, ad$dataset, ad)
        e2$tables <- owned_tbls(ad$tables_owned)
        datasets[[k2]] <- e2
      }
      # ERD legend swatch (provider_dataset → color)
      col <- cc$erd$color
      if (!is.null(col)) {
        erd_legend[[length(erd_legend) + 1]] <- list(
          provider_dataset = key, color = col)
      }
    }
  } else if (!is.null(dataset_csv) && file.exists(dataset_csv)) {
    d_ds <- readr::read_csv(dataset_csv, show_col_types = FALSE)
    csv_field <- function(i, k) {
      if (k %in% names(d_ds) && !is.na(d_ds[[k]][i])) d_ds[[k]][i] else NULL
    }
    for (i in seq_len(nrow(d_ds))) {
      prov <- d_ds$provider[i]
      dset <- d_ds$dataset[i]
      if (is.na(prov) || is.na(dset)) next
      key <- paste0(prov, "_", dset)
      m <- list(
        dataset_name      = csv_field(i, "dataset_name"),
        description       = csv_field(i, "description"),
        citation_main     = csv_field(i, "citation_main"),
        link_calcofi_org  = csv_field(i, "link_calcofi_org"),
        link_data_source  = csv_field(i, "link_data_source"),
        coverage_temporal = csv_field(i, "coverage_temporal"),
        coverage_spatial  = csv_field(i, "coverage_spatial"),
        license           = csv_field(i, "license"),
        pi_names          = csv_field(i, "pi_names"))
      datasets[[key]] <- .dataset_entry(prov, dset, m)
    }
  }

  # contributions block — pivot per-ingest row counts to table -> by_dataset
  # with percentages against the release-final row count
  contributions <- list()
  for (tbl in names(contrib_raw)) {
    conts    <- contrib_raw[[tbl]]
    sum_rows <- sum(vapply(conts, function(x) as.numeric(x$rows %||% 0), numeric(1)))
    total    <- if (!is.null(table_rows) && tbl %in% names(table_rows)) {
      as.numeric(table_rows[[tbl]])
    } else {
      sum_rows
    }
    denom <- if (total > 0) total else sum_rows
    by_dataset <- lapply(conts, function(x) {
      r <- as.numeric(x$rows %||% 0)
      list(
        provider_dataset = x$provider_dataset,
        rows             = as.integer(r),
        pct              = if (denom > 0) round(r / denom * 100, 1) else 0,
        workflow         = x$workflow %||% NA_character_)
    })
    contributions[[tbl]] <- list(
      total_rows      = as.integer(total),
      over_attributed = sum_rows > total,
      by_dataset      = by_dataset)
  }

  merged <- list(
    schema_version    = "1.2",
    release_version   = release_version,
    release_date      = as.character(Sys.Date()),
    datasets          = datasets,
    tables            = tables_meta,
    columns           = columns_meta,
    measurement_types = measurement_types,
    contributions     = contributions,
    erd_legend        = erd_legend)

  dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(
    merged, output_path,
    auto_unbox = TRUE, pretty = TRUE, null = "null")

  message(glue::glue(
    "Merged metadata.json: {length(tables_meta)} tables, ",
    "{length(columns_meta)} columns, {length(datasets)} datasets, ",
    "{length(measurement_types)} measurement types"))

  invisible(output_path)
}

# mismatch collection & spatial manifest ----

#' Collect Ship Mismatches
#'
#' Finds ships in a table with placeholder `ship_nodc` values (containing `"?"`)
#' or NULL ship_key, indicating unresolved matches. Used to populate the
#' `mismatches$ships` section of `manifest.json`.
#'
#' @param con DBI connection to DuckDB
#' @param table Table to check for ship mismatches
#' @param ship_tbl Ship reference table (default: `"ship"`)
#'
#' @return Tibble with columns: ship_key, ship_nodc, ship_name, n_rows
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' collect_ship_mismatches(con, "ctd_cast")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
collect_ship_mismatches <- function(con, table, ship_tbl = "ship") {

  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{table}'"))$column_name

  if (!"ship_key" %in% cols) {
    return(tibble::tibble(
      ship_key  = character(),
      ship_nodc = character(),
      ship_name = character(),
      n_rows    = integer()))
  }

  DBI::dbGetQuery(con, glue::glue(
    "SELECT t.ship_key, s.ship_nodc, s.ship_name, COUNT(*) AS n_rows
     FROM {table} t
     LEFT JOIN {ship_tbl} s ON t.ship_key = s.ship_key
     WHERE t.ship_key IS NULL
        OR s.ship_nodc LIKE '%?%'
     GROUP BY t.ship_key, s.ship_nodc, s.ship_name
     ORDER BY n_rows DESC"))
}

#' Collect Measurement Type Mismatches
#'
#' Finds measurement types present in the DuckDB `measurement_type` table but
#' not registered in the central `measurement_type.csv` registry. Used to
#' populate the `mismatches$measurement_types` section of `manifest.json`.
#'
#' @param con DBI connection to DuckDB with a `measurement_type` table
#' @param measurement_type_csv Path to `metadata/measurement_type.csv`
#'
#' @return Tibble with columns: measurement_type, source (value: "db_only")
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' collect_measurement_type_mismatches(con, "metadata/measurement_type.csv")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom readr read_csv
collect_measurement_type_mismatches <- function(con, measurement_type_csv) {

  tbls <- DBI::dbListTables(con)
  if (!"measurement_type" %in% tbls) {
    return(tibble::tibble(
      measurement_type = character(),
      source           = character()))
  }

  db_types <- DBI::dbGetQuery(con,
    "SELECT DISTINCT measurement_type FROM measurement_type")$measurement_type

  csv_types <- readr::read_csv(
    measurement_type_csv, show_col_types = FALSE)$measurement_type

  new_types <- setdiff(db_types, csv_types)

  if (length(new_types) == 0) {
    return(tibble::tibble(
      measurement_type = character(),
      source           = character()))
  }

  message(glue::glue(
    "{length(new_types)} measurement type(s) in DB but not in CSV: ",
    "{paste(new_types, collapse = ', ')}"))

  tibble::tibble(
    measurement_type = new_types,
    source           = "db_only")
}

#' Collect Cruise Key Mismatches
#'
#' Finds cruise keys that are malformed (do not match `YYYY-MM-NODC` pattern)
#' or contain placeholder `"?"` characters from interim ship entries. Used to
#' populate the `mismatches$cruise_keys` section of `manifest.json`.
#'
#' @param con DBI connection to DuckDB
#' @param table Table to check for cruise_key mismatches
#'
#' @return Tibble with columns: cruise_key, status, n_rows
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' collect_cruise_key_mismatches(con, "ctd_cast")
#' }
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
collect_cruise_key_mismatches <- function(con, table) {

  cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{table}'"))$column_name

  if (!"cruise_key" %in% cols) {
    return(tibble::tibble(
      cruise_key = character(),
      status     = character(),
      n_rows     = integer()))
  }

  DBI::dbGetQuery(con, glue::glue(
    "SELECT cruise_key,
            CASE
              WHEN cruise_key IS NULL THEN 'null'
              WHEN cruise_key LIKE '%?%' THEN 'interim_ship'
              WHEN NOT regexp_matches(cruise_key, '^\\d{{4}}-\\d{{2}}-.+$')
                THEN 'malformed'
              ELSE 'ok'
            END AS status,
            COUNT(*) AS n_rows
     FROM {table}
     WHERE cruise_key IS NULL
        OR cruise_key LIKE '%?%'
        OR NOT regexp_matches(cruise_key, '^\\d{{4}}-\\d{{2}}-.+$')
     GROUP BY cruise_key, status
     ORDER BY n_rows DESC"))
}

#' Write Spatial Manifest
#'
#' Generates a `manifest.json` for spatial parquet outputs that do not use
#' `write_parquet_outputs()`. Inventories all `.parquet` files in a directory,
#' reads row counts via a transient DuckDB connection, and writes a manifest
#' in the same format as [write_parquet_outputs()].
#'
#' @param parquet_dir Directory containing `.parquet` files
#'
#' @return Invisible path to the written `manifest.json`
#' @export
#' @concept wrangle
#'
#' @examples
#' \dontrun{
#' write_spatial_manifest("data/parquet/spatial")
#' }
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom tibble tibble
#' @importFrom jsonlite write_json
#' @importFrom glue glue
write_spatial_manifest <- function(parquet_dir) {

  pq_files <- list.files(parquet_dir, pattern = "\\.parquet$",
                          full.names = TRUE)

  if (length(pq_files) == 0) {
    warning("No .parquet files found in ", parquet_dir)
    return(invisible(NULL))
  }

  # transient DuckDB for row counts
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  stats <- tibble::tibble(
    table     = tools::file_path_sans_ext(basename(pq_files)),
    rows      = vapply(pq_files, function(f) {
      DBI::dbGetQuery(con, glue::glue(
        "SELECT COUNT(*) AS n FROM read_parquet('{f}')"))$n
    }, numeric(1)),
    file_size = file.info(pq_files)$size,
    path      = pq_files)

  manifest <- list(
    tables           = stats$table,
    total_rows       = sum(stats$rows),
    total_size_bytes = sum(stats$file_size),
    metadata_file    = "metadata.json",
    partitioned      = character(0),
    files            = as.list(stats))

  manifest_path <- file.path(parquet_dir, "manifest.json")
  jsonlite::write_json(
    manifest, manifest_path,
    auto_unbox = TRUE, pretty = TRUE)
  message(glue::glue(
    "Wrote spatial manifest: {nrow(stats)} tables, ",
    "{format(sum(stats$rows), big.mark = ',')} rows -> {manifest_path}"))

  invisible(manifest_path)
}
