# wrangling functions for calcofi data restructuring
# handles intermediate transformations in local duckdb before output

#' Create Cruise Key from Ship Key and Date
#'
#' Creates a natural key for cruises in format YYMMKK where:
#' - YY = 2-digit year
#' - MM = 2-digit month
#' - KK = 2-letter ship key
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
      "ALTER TABLE {cruise_tbl} ADD COLUMN cruise_key TEXT"))
  }

  # populate cruise_key as YYMMKK (2-digit year + 2-digit month + ship_key)
  DBI::dbExecute(con, glue::glue("
    UPDATE {cruise_tbl}
    SET cruise_key = CONCAT(
      LPAD(CAST(EXTRACT(YEAR FROM {date_col}) % 100 AS VARCHAR), 2, '0'),
      LPAD(CAST(EXTRACT(MONTH FROM {date_col}) AS VARCHAR), 2, '0'),
      ship_key)"))

  # verify uniqueness
  dups <- DBI::dbGetQuery(con, glue::glue("
    SELECT cruise_key, COUNT(*) as n
    FROM {cruise_tbl}
    GROUP BY cruise_key
    HAVING COUNT(*) > 1"))

  if (nrow(dups) > 0) {
    warning(glue::glue(
      "cruise_key is not unique! Found {nrow(dups)} duplicate keys"))
  }

  message(glue::glue("Created cruise_key column in {cruise_tbl}"))
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
      "ALTER TABLE {child_tbl} ADD COLUMN {key_col} {key_type}"))
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
#' # assign ichthyo_id with multi-column sort
#' assign_sequential_ids(
#'   con        = con,
#'   table_name = "ichthyo",
#'   id_col     = "ichthyo_id",
#'   sort_cols  = c("net_id", "species_id", "life_stage",
#'                  "measurement_type", "measurement_value"))
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
      "ALTER TABLE {table_name} ADD COLUMN {id_col} INTEGER"))
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
#' After calling this function, use `replace_uuid_with_id()` to convert net_uuid
#' to net_id, then `assign_sequential_ids()` to add ichthyo_id.
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
#' # then convert net_uuid to net_id
#' replace_uuid_with_id(
#'   con          = con,
#'   table_name   = "ichthyo",
#'   uuid_col     = "net_uuid",
#'   new_id_col   = "net_id",
#'   ref_table    = "net",
#'   ref_uuid_col = "net_uuid",
#'   ref_id_col   = "net_id")
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
      "ALTER TABLE {table_name} ADD COLUMN {new_id_col} INTEGER"))
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

  # add additional corrections here as they are identified
  # correction 2: example placeholder
  # if ("some_table" %in% DBI::dbListTables(con)) {
  #   result <- DBI::dbExecute(con, "UPDATE some_table SET ... WHERE ...")
  #   if (result > 0) {
  #     if (verbose) message("Correction: ...")
  #     corrections_applied <- corrections_applied + 1
  #   }
  # }

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
#' @param strip_provenance Remove provenance columns (_source_*, _ingested_at) (default: TRUE)
#' @param compression Parquet compression codec (default: "snappy")
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
    strip_provenance = TRUE,
    compression      = "snappy") {

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

  # provenance columns to strip
  prov_cols <- c("_source_file", "_source_row", "_source_uuid", "_ingested_at")

  # export each table
  stats <- purrr::map_dfr(tables, function(tbl) {
    output_path <- file.path(output_dir, paste0(tbl, ".parquet"))

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

    # build select clause
    select_clause <- paste(export_cols, collapse = ", ")

    # export to parquet
    DBI::dbExecute(con, glue::glue("
      COPY (SELECT {select_clause} FROM {tbl})
      TO '{output_path}'
      (FORMAT PARQUET, COMPRESSION '{compression}')"))

    # get file size
    file_size <- file.info(output_path)$size

    # get row count
    n_rows <- DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) as n FROM {tbl}"))$n

    message(glue::glue("Exported {tbl}: {n_rows} rows, {round(file_size/1024/1024, 2)} MB"))

    tibble::tibble(
      table     = tbl,
      rows      = n_rows,
      file_size = file_size,
      path      = output_path)
  })

  # write manifest
  manifest <- list(
    created_at       = as.character(Sys.time()),
    tables           = stats$table,
    total_rows       = sum(stats$rows),
    total_size_bytes = sum(stats$file_size),
    metadata_file    = "metadata.json",
    files            = stats |> as.list())

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
#' @param provider Data provider identifier (e.g. "swfsc.noaa.gov")
#' @param dataset Dataset identifier (e.g. "calcofi-db")
#' @param workflow_url URL to the rendered workflow page
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
#'   metadata_derived_csv = "metadata/swfsc.noaa.gov/calcofi-db/metadata_derived.csv",
#'   output_dir           = "data/parquet/swfsc.noaa.gov_calcofi-db",
#'   tables               = DBI::dbListTables(con),
#'   provider             = "swfsc.noaa.gov",
#'   dataset              = "calcofi-db",
#'   workflow_url         = "https://calcofi.io/workflows/ingest_swfsc.noaa.gov_calcofi-db.html")
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
    workflow_url  = NULL) {

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

  # --- assemble metadata json ---
  metadata <- list(
    schema_version = "1.0",
    created_at     = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    provider       = provider,
    dataset        = dataset,
    workflow       = workflow_url,
    tables         = tables_meta,
    columns        = columns_meta)

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
