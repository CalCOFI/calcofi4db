# bundle.R — one dataset as zips a person can open ------------------------------------
#
# `publish_to-zip.qmd` (CalCOFI/workflows) writes, for every dataset in a promoted release,
# one zip per format — CSV for everyone, an Excel workbook where every table fits, Parquet for
# people who know what that is — each holding the dataset's own rows of every table it needs
# to be used alone, and the dataset's README, citation, licence and column dictionary. These
# functions are that publisher's rules, kept here so a test can assert them:
#
#   bundle_plan()           which tables, filtered how, and what is left out and why
#   bundle_build_tables()   the tables themselves, in a DuckDB connection, readable without joins
#   bundle_format_decision() which formats are offered, and in plain words why one is not
#   bundle_readme(), bundle_citation(), bundle_license(), bundle_columns()   the documents
#   write_bundle()          the zips, byte-for-byte reproducible
#   bundle_manifest_entry(), fetch_bundle_manifest()   what the website reads
#
# The rows are never reshaped beyond adding readable columns beside keys; a bundle is the
# release's own tables, cut to one dataset.

#' @keywords internal
CC_EXPLORE_URL <- "https://calcofi.io/explore/"

#' @keywords internal
CC_BUNDLE_BASE_URL <- "https://storage.googleapis.com/calcofi-db/publish/zip"

#' @keywords internal
CC_BUNDLE_MANIFEST_URL <- "https://storage.googleapis.com/calcofi-db/publish/zip/manifest.json"

# the calcofi.io site's own addresses (CalCOFI.github.io _config.yml, Decisions 22-23):
# registration is encouraged, never required
#' @keywords internal
CC_REGISTER_USE_URL <- "https://github.com/CalCOFI/workflows/issues/new?labels=derived-product&title=Registered+use%3A+"
#' @keywords internal
CC_STAY_INFORMED_URL <- "mailto:data@calcofi.io?subject=CalCOFI%20release%20updates"
#' @keywords internal
CC_DATA_CONTACT <- "data@calcofi.io"

#' Limits a bundle is held to
#'
#' * `EXCEL_MAX_ROWS` — Excel's own sheet limit, 1,048,576 rows including the header.
#' * `BUNDLE_XLSX_MAX_ROWS` — the largest table an Excel workbook is offered for: Excel's own
#'   limit, 1,048,575 rows of data below the header row (owner's decision, 2026-11-16: any
#'   lower cap would be arbitrary). Above it the CSV zip is the route.
#' * `BUNDLE_MAX_ZIP_BYTES` — the largest zip [write_bundle()] will leave for a browser
#'   download, 2 GiB; over it the build stops and says so rather than dropping the file.
#'
#' @format Integers / a number.
#' @export
#' @concept bundle
EXCEL_MAX_ROWS <- 1048576L

#' @rdname EXCEL_MAX_ROWS
#' @export
BUNDLE_XLSX_MAX_ROWS <- EXCEL_MAX_ROWS - 1L

#' @rdname EXCEL_MAX_ROWS
#' @export
BUNDLE_MAX_ZIP_BYTES <- 2 * 1024^3

# the shared tables every bundle carries its rows of, when the release has them, so the
# download is usable alone (samples, cruises, the taxa and measurement types it names)
.BUNDLE_COMPANIONS <- c("sample", "cruise", "taxon", "measurement_type")

# a table without a dataset_key column is cut to the rows a bundle reaches through the first
# of these key columns it carries (the *_key naming rule): a vocabulary row another dataset
# added never enters this dataset's download. A table with none of them is carried whole.
.BUNDLE_LINK_COLS <- c("sample_key", "taxon_key", "measurement_type", "cruise_key", "grid_key", "ship_key")

# column types a bundle leaves out: geometry (latitude/longitude are kept beside it) and
# unsigned 64-bit H3 cell ids, which no spreadsheet holds exactly (Excel keeps 15 digits)
.BUNDLE_DROP_TYPES <- "^(GEOMETRY|BLOB|UBIGINT|UHUGEINT)"

# plain-language one-liners for the tables a newcomer meets most; any other table is
# described by the first sentence of the release's own metadata.json description
.BUNDLE_TABLE_LABELS <- c(
  obs                = "The observations: one measured value per row: what was measured, its value and units, where, when, at what depth and, for biological data, which taxon. Start here.",
  sample             = "The sampling events the observations were made at (a cast, a tow, a net, a bottle, an underway record...): where, when, how deep, and which event each belongs to.",
  sample_measurement = "Measurements of a sampling event itself, such as the volume of water a net filtered.",
  obs_attribute      = "Detail on an observation: counts by size or developmental stage, or a behaviour.",
  measurement_type   = "What each measurement type means and the units its values are in.",
  taxon              = "The taxa named in this dataset: scientific and common names, WoRMS and ITIS identifiers, classification.",
  dataset_taxon      = "The names and codes this dataset's provider uses for each taxon, beside the taxon they were matched to.",
  taxon_group        = "Groups of taxa the provider reports together.",
  cruise             = "The cruises the samples were taken on: ship and dates.",
  ship               = "The ships.",
  grid               = "The CalCOFI grid cells the samples fall in (one cell per official station).")

# the readable columns bundle_build_tables() adds beside keys
.BUNDLE_ADDED_COLUMNS <- list(
  site_key        = list(units = "", description = "The station the provider recorded for the sampling event (CalCOFI line and station, e.g. `090.0 060.0`), copied from `sample.site_key` so a row reads without a join. Empty where the provider names no station (underway, transect and survey data)."),
  line            = list(units = "", description = "CalCOFI line number, read from `site_key` (its first number). Empty where `site_key` is not a line and station."),
  station         = list(units = "", description = "CalCOFI station number along the line, read from `site_key` (its second number). Empty where `site_key` is not a line and station."),
  scientific_name = list(units = "", description = "The taxon's accepted scientific name, copied from `taxon.scientific_name` so a row reads without a join."),
  common_name     = list(units = "", description = "The taxon's common name where one is recorded, copied from `taxon.common_name`."),
  units           = list(units = "", description = "The units of this row's value, copied from `measurement_type.units`."))

#' The download formats a dataset bundle can be written in
#'
#' `csv` (every dataset), `xlsx` (only when every table fits; see
#' [bundle_format_decision()]) and `parquet` (every dataset), in the order a page offers them.
#'
#' @return Character vector.
#' @export
#' @concept bundle
bundle_formats <- function() c("csv", "xlsx", "parquet")

#' The file name of a dataset's bundle
#'
#' `calcofi_{dataset_key}_{version}_{format}.zip`, the `calcofi_` prefix omitted when the
#' key already starts with it (`calcofi_bottle_v2026.11.14_csv.zip`, not
#' `calcofi_calcofi_bottle_…`), so the name says what it is when it lands in Downloads.
#'
#' @param dataset_key the dataset
#' @param version the release the bundle was built from
#' @param format one of [bundle_formats()]
#' @param ext the extension; `""` gives the stem (the folder inside the zip)
#' @return Character.
#' @export
#' @concept bundle
bundle_file_name <- function(dataset_key, version, format, ext = ".zip") {
  stopifnot(nzchar(dataset_key), nzchar(version), format %in% bundle_formats())
  stem <- if (startsWith(dataset_key, "calcofi_")) dataset_key else paste0("calcofi_", dataset_key)
  paste0(stem, "_", version, "_", format, ext)
}

# plan ---------------------------------------------------------------------------------

#' Plan a dataset's bundle: which tables, cut how, and what is left out
#'
#' The tables are the record's own (`datasets.json` `tables[]`) plus the shared tables a
#' download needs to be used alone — `sample`, `cruise`, `taxon`, `measurement_type` —
#' wherever the release has them. Each table is cut to this dataset by one rule:
#'
#' * `dataset_key` — the table carries a `dataset_key` column: this dataset's rows;
#' * `dataset_key_or_referenced` — `sample` only: this dataset's rows, plus the samples its
#'   other tables point at (a derived product's observations sit on another dataset's
#'   casts) and their parents;
#' * `linked` — a shared table without `dataset_key`: the rows the bundle reaches through
#'   `link`, the first of `sample_key`, `taxon_key`, `measurement_type`, `cruise_key`,
#'   `grid_key`, `ship_key` the table carries;
#' * `whole` — a small table with none of those (e.g. `region`, `lookup`);
#' * `excluded` — a supplemental table (the full-resolution `obs_ctd_full`,
#'   `obs_mets_full`): hundreds of millions of rows partitioned by cruise, left out and
#'   named in the README with where to get them;
#' * `absent` — named by the record but not in this release.
#'
#' A companion table that ends up with no rows is dropped by [bundle_build_tables()].
#'
#' @param record one dataset record from `datasets.json` (a list with `dataset_key`, `tables`)
#' @param table_columns named list, table -> its column names in this release
#' @param supplemental table names the catalog marks `supplemental`
#' @param supplemental_rows optional named numeric, table -> rows, quoted in the reason
#' @return A data frame, one row per table in presentation order (`obs`, `sample`, the
#'   dataset's other tables, then the shared vocabularies, then exclusions): `table`,
#'   `included`, `rule`, `link`, `role`, `source` (`record` | `companion`), `reason`.
#' @export
#' @concept bundle
#' @seealso [bundle_build_tables()]
bundle_plan <- function(record, table_columns, supplemental = character(), supplemental_rows = NULL) {
  stopifnot(is.list(record), nzchar(.s(record[["dataset_key"]])), is.list(table_columns))
  rec_tables <- unique(as.character(unlist(record[["tables"]])))
  comp <- setdiff(intersect(.BUNDLE_COMPANIONS, names(table_columns)), rec_tables)
  cand <- c(rec_tables, comp)
  rows <- lapply(cand, function(tb) {
    src  <- if (tb %in% rec_tables) "record" else "companion"
    cols <- table_columns[[tb]]
    out  <- list(table = tb, included = TRUE, rule = NA_character_, link = NA_character_,
                 role = "reference", source = src, reason = "")
    if (tb %in% supplemental) {
      n <- if (!is.null(supplemental_rows) && tb %in% names(supplemental_rows))
        sprintf("%s rows, ", format(supplemental_rows[[tb]], big.mark = ",", scientific = FALSE)) else ""
      out$included <- FALSE; out$rule <- "excluded"; out$role <- "supplemental"
      out$reason <- sprintf(paste0(
        "full-resolution supplemental table (%spartitioned by cruise, not by dataset): too large ",
        "for a download; the observations at the release's grain are in obs"), n)
      return(out)
    }
    if (is.null(cols)) {
      out$included <- FALSE; out$rule <- "absent"; out$reason <- "not in this release"
      return(out)
    }
    if ("dataset_key" %in% cols) {
      out$rule <- if (tb == "sample") "dataset_key_or_referenced" else "dataset_key"
      out$role <- if (tb == "obs") "observations" else if (tb == "sample") "samples" else "dataset"
      out$reason <- if (tb == "sample") "this dataset's samples and the samples its rows point at" else
        "this dataset's rows"
      return(out)
    }
    lk <- intersect(.BUNDLE_LINK_COLS, cols)
    if (length(lk)) {
      out$rule <- "linked"; out$link <- lk[1]
      if (tb == "sample") out$role <- "samples"
      out$reason <- sprintf("the rows this dataset reaches through `%s`", lk[1])
    } else {
      out$rule <- "whole"; out$reason <- "small shared table, carried whole"
    }
    out
  })
  plan <- do.call(rbind, lapply(rows, as.data.frame, stringsAsFactors = FALSE))
  rank <- ifelse(plan$table == "obs", 1,
          ifelse(plan$table == "sample", 2,
          ifelse(plan$rule == "dataset_key", 3,
          ifelse(plan$rule == "linked", 4 + match(plan$link, .BUNDLE_LINK_COLS) / 10,
          ifelse(plan$rule == "whole", 5, 6)))))
  plan <- plan[order(rank, plan$table), , drop = FALSE]
  rownames(plan) <- NULL
  plan
}

# build order: the dataset's own rows first, then sample (it needs their sample_keys), then
# the linked tables by key (fish before taxon before cruise before ship), then the whole ones
.bundle_build_order <- function(plan) {
  p <- plan[plan$included, , drop = FALSE]
  rank <- ifelse(p$rule == "dataset_key", 1,
          ifelse(p$rule == "dataset_key_or_referenced", 2,
          ifelse(p$rule == "linked", 3 + match(p$link, .BUNDLE_LINK_COLS) / 10, 4)))
  p$table[order(rank, p$table)]
}

# build --------------------------------------------------------------------------------

.q <- function(x) paste0('"', gsub('"', '""', x), '"')
.lit <- function(x) paste0("'", gsub("'", "''", x), "'")

#' Build a dataset's bundle tables in a DuckDB connection
#'
#' For each table [bundle_plan()] includes, one table `{prefix}{table}` in `con`: the
#' dataset's rows cut by the plan's rule, geometry and H3 cell columns dropped (latitude and
#' longitude stay), and readable columns added beside keys so a row reads without a join:
#'
#' * beside `sample_key`: `site_key`, `line`, `station` (from `sample`; `line` and `station`
#'   parsed from a `site_key` of the form `090.0 060.0`, empty otherwise);
#' * beside `site_key` on `sample` itself: `line`, `station`;
#' * beside `taxon_key`: `scientific_name`, `common_name` (from `taxon`);
#' * beside `measurement_type`: `units` (from `measurement_type`).
#'
#' A column is only added to a table that does not already carry one by that name, and never
#' to the table it comes from. Rows are stored under `ORDER BY ALL`, so a table's order is a
#' function of its content (the zips are hashed). Intermediate tables are dropped.
#'
#' @param con a DuckDB connection
#' @param plan the data frame from [bundle_plan()]
#' @param dataset_key the dataset
#' @param from_sql named character, table -> a `FROM` expression for the release table
#'   (`read_parquet('…')`, or a parenthesised view such as the catalog's `obs`)
#' @param prefix name prefix of the tables written
#' @return A data frame, one row per table written (presentation order): `table`, `rows`,
#'   `n_columns`, `dropped` (comma-separated columns left out), `added` (comma-separated
#'   columns added). A companion table with no rows is dropped and not listed.
#' @export
#' @concept bundle
#' @importFrom DBI dbExecute dbGetQuery
#' @seealso [bundle_plan()], [write_bundle()]
bundle_build_tables <- function(con, plan, dataset_key, from_sql, prefix = "bundle_") {
  stopifnot(is.data.frame(plan), nzchar(dataset_key))
  order <- .bundle_build_order(plan)
  miss <- setdiff(order, names(from_sql))
  if (length(miss)) stop("no FROM expression for: ", paste(miss, collapse = ", "), call. = FALSE)
  base <- function(tb) paste0("_bundle_base_", tb)
  cols_of <- function(tb) DBI::dbGetQuery(con, sprintf("DESCRIBE %s", .q(base(tb))))$column_name
  key <- .lit(dataset_key)
  built <- character(); dropped <- list()

  for (tb in order) {
    r <- plan[plan$table == tb, , drop = FALSE]
    desc <- DBI::dbGetQuery(con, sprintf("DESCRIBE SELECT * FROM %s", from_sql[[tb]]))
    drop <- desc$column_name[grepl(.BUNDLE_DROP_TYPES, toupper(desc$column_type))]
    keep <- setdiff(desc$column_name, drop)
    dropped[[tb]] <- drop
    sel <- paste(.q(keep), collapse = ", ")
    # the values a key column takes across the tables already built
    reached <- function(col) {
      have <- Filter(function(b) col %in% cols_of(b), built)
      if (!length(have)) return(NULL)
      paste(sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL", .q(col), .q(base(have)), .q(col)),
            collapse = " UNION ")
    }
    where <- switch(r$rule,
      dataset_key = sprintf("WHERE dataset_key = %s", key),
      dataset_key_or_referenced = {
        ref <- reached("sample_key")
        if (is.null(ref)) sprintf("WHERE dataset_key = %s", key) else
          sprintf("WHERE dataset_key = %s OR sample_key IN (%s)", key, ref)
      },
      linked = {
        ref <- reached(r$link)
        if (is.null(ref)) "WHERE FALSE" else sprintf("WHERE %s IN (%s)", .q(r$link), ref)
      },
      whole = "",
      stop("unknown rule `", r$rule, "` for ", tb, call. = FALSE))
    DBI::dbExecute(con, sprintf("CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s %s",
                                .q(base(tb)), sel, from_sql[[tb]], where))
    # a sample a row points at brings the events it belongs to (a bottle its cast)
    if (r$rule == "dataset_key_or_referenced" &&
        all(c("parent_sample_key", "root_sample_key") %in% keep)) {
      for (i in 1:10) {
        n <- DBI::dbExecute(con, sprintf(
          "INSERT INTO %1$s SELECT %2$s FROM %3$s WHERE sample_key IN (
             SELECT parent_sample_key FROM %1$s UNION SELECT root_sample_key FROM %1$s)
           AND sample_key NOT IN (SELECT sample_key FROM %1$s)",
          .q(base(tb)), sel, from_sql[[tb]]))
        if (n == 0) break
      }
    }
    built <- c(built, tb)
  }

  # a companion that reaches nothing is not part of the bundle
  n_base <- vapply(built, function(tb)
    as.numeric(DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) n FROM %s", .q(base(tb))))$n), 1)
  comp <- plan$table[plan$source == "companion"]
  empty <- built[built %in% comp & n_base == 0]
  for (tb in empty) DBI::dbExecute(con, sprintf("DROP TABLE %s", .q(base(tb))))
  built <- setdiff(built, empty)

  # readable columns beside keys ----
  has <- function(tb, col) tb %in% built && col %in% cols_of(tb)
  site_ok <- has("sample", "site_key")
  line_sql <- function(x, part) sprintf(
    "CASE WHEN regexp_full_match(%1$s, '\\s*-?[0-9]+(\\.[0-9]+)?\\s+-?[0-9]+(\\.[0-9]+)?\\s*')
       THEN TRY_CAST(regexp_split_to_array(trim(%1$s), '\\s+')[%2$d] AS DOUBLE) END", x, part)
  out <- list()
  for (tb in plan$table[plan$table %in% built]) {
    cols <- cols_of(tb)
    sel <- character(); joins <- character(); added <- character()
    for (cn in cols) {
      sel <- c(sel, paste0("t.", .q(cn)))
      if (cn == "sample_key" && tb != "sample" && site_ok && !"site_key" %in% cols) {
        joins <- c(joins, sprintf(
          "LEFT JOIN (SELECT sample_key, site_key FROM %s) s_ ON t.sample_key = s_.sample_key",
          .q(base("sample"))))
        sel <- c(sel, "s_.site_key AS site_key", paste(line_sql("s_.site_key", 1), "AS line"),
                 paste(line_sql("s_.site_key", 2), "AS station"))
        added <- c(added, "site_key", "line", "station")
      }
      if (cn == "site_key" && !any(c("line", "station") %in% cols)) {
        sel <- c(sel, paste(line_sql("t.site_key", 1), "AS line"), paste(line_sql("t.site_key", 2), "AS station"))
        added <- c(added, "line", "station")
      }
      if (cn == "taxon_key" && tb != "taxon" && has("taxon", "scientific_name") &&
          !any(c("scientific_name", "common_name") %in% cols)) {
        joins <- c(joins, sprintf(
          "LEFT JOIN (SELECT taxon_key, scientific_name, common_name FROM %s) x_ ON t.taxon_key = x_.taxon_key",
          .q(base("taxon"))))
        sel <- c(sel, "x_.scientific_name AS scientific_name", "x_.common_name AS common_name")
        added <- c(added, "scientific_name", "common_name")
      }
      if (cn == "measurement_type" && tb != "measurement_type" && has("measurement_type", "units") &&
          !"units" %in% cols) {
        joins <- c(joins, sprintf(
          "LEFT JOIN (SELECT measurement_type, units FROM %s) m_ ON t.measurement_type = m_.measurement_type",
          .q(base("measurement_type"))))
        sel <- c(sel, "m_.units AS units")
        added <- c(added, "units")
      }
    }
    DBI::dbExecute(con, sprintf("CREATE OR REPLACE TABLE %s AS SELECT %s FROM %s t %s ORDER BY ALL",
                                .q(paste0(prefix, tb)), paste(sel, collapse = ", "), .q(base(tb)),
                                paste(joins, collapse = " ")))
    n <- as.numeric(DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) n FROM %s", .q(paste0(prefix, tb))))$n)
    out[[tb]] <- data.frame(table = tb, rows = n, n_columns = length(sel),
                            dropped = paste(dropped[[tb]], collapse = ","),
                            added = paste(added, collapse = ","), stringsAsFactors = FALSE)
  }
  for (tb in built) DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", .q(base(tb))))
  res <- do.call(rbind, unname(out))
  rownames(res) <- NULL
  res
}

#' Row signatures of a dataset's bundle tables, for its fingerprint
#'
#' The bundle is a function of exactly the rows written, so these — not the release objects
#' they were cut from — are its data inputs: a vocabulary row another dataset added, or a
#' release that left this dataset alone, changes nothing here. Each table is signed by the
#' release's own row-signature rule (order-independent per-column hash sums, the identity
#' `freeze_plan()` uses).
#'
#' @param con the connection [bundle_build_tables()] wrote into
#' @param tables the table names (without prefix)
#' @param prefix the prefix [bundle_build_tables()] used
#' @return A named character vector, `data:{table}` -> md5, for [publish_fingerprint()].
#' @export
#' @concept bundle
bundle_table_signatures <- function(con, tables, prefix = "bundle_") {
  tables <- sort(unique(as.character(tables)))
  v <- vapply(tables, function(tb)
    digest::digest(.s(.table_content_hash(con, paste0(prefix, tb))), algo = "md5", serialize = FALSE), "")
  stats::setNames(v, paste0("data:", tables))
}

# formats ------------------------------------------------------------------------------

#' Which formats a dataset's bundle is offered in, and why one is not
#'
#' * `csv` — always. `excel_openable` only when every table is within Excel's sheet
#'   limit ([EXCEL_MAX_ROWS], header included); otherwise `note` says which table is too
#'   long to open in a spreadsheet.
#' * `xlsx` — only when every table has at most `xlsx_max_rows` rows of data
#'   ([BUNDLE_XLSX_MAX_ROWS], so every sheet fits Excel's 1,048,576 rows with its header);
#'   otherwise not written, and `reason` says why in plain words.
#' * `parquet` — always.
#'
#' @param rows named numeric, table -> rows in the bundle
#' @param xlsx_max_rows the largest table an Excel workbook is offered for
#' @return A data frame, one row per [bundle_formats()]: `format`, `write`,
#'   `excel_openable`, `reason` (why a format is not offered, `""` otherwise) and `note`
#'   (a caveat on a format that is offered, `""` otherwise).
#' @export
#' @concept bundle
bundle_format_decision <- function(rows, xlsx_max_rows = BUNDLE_XLSX_MAX_ROWS) {
  stopifnot(length(rows) > 0, !is.null(names(rows)))
  rows <- stats::setNames(as.numeric(rows), names(rows))
  big  <- names(rows)[which.max(rows)]
  fmt  <- function(n) format(n, big.mark = ",", scientific = FALSE, trim = TRUE)
  csv_ok <- all(rows <= EXCEL_MAX_ROWS - 1)
  xlsx_ok <- all(rows <= xlsx_max_rows)
  over_excel <- names(rows)[rows > EXCEL_MAX_ROWS - 1]
  data.frame(
    format = bundle_formats(),
    write  = c(TRUE, xlsx_ok, TRUE),
    excel_openable = c(csv_ok, xlsx_ok, FALSE),
    reason = c("",
               if (xlsx_ok) "" else if (xlsx_max_rows >= EXCEL_MAX_ROWS - 1) sprintf(paste0(
                 "Not offered as an Excel workbook: its largest table, %s, has %s rows, more than ",
                 "an Excel sheet holds (%s rows including the header row). Use the CSV download, ",
                 "or take a subset from the Explorer."), big, fmt(rows[[big]]), fmt(EXCEL_MAX_ROWS)) else
                 sprintf(paste0(
                 "Not offered as an Excel workbook: its largest table, %s, has %s rows, and a ",
                 "workbook is made only when every table has at most %s rows. Use the CSV download."),
                 big, fmt(rows[[big]]), fmt(xlsx_max_rows)),
               ""),
    note = c(if (csv_ok) "" else sprintf(paste0(
               "%s has more rows than a spreadsheet can open (Excel stops at %s): open it with R, ",
               "Python, a database, or a text editor, or take a subset from the Explorer."),
               paste(sprintf("%s (%s rows)", over_excel, fmt(rows[over_excel])), collapse = " and "),
               fmt(EXCEL_MAX_ROWS)),
             "", ""),
    stringsAsFactors = FALSE)
}

# documents ----------------------------------------------------------------------------

.bundle_attr <- function(record) record[["attribution"]] %||% list()

.bundle_contact <- function(record) {
  ct <- .s(.bundle_attr(record)[["contact"]])
  ct <- sub("^mailto:", "", ct)
  if (nzchar(ct)) ct else NULL
}

.bundle_year <- function(x, fallback) {
  m <- regmatches(x, regexpr("\\b(19|20)[0-9]{2}\\b", x))
  if (length(m)) m[1] else fallback
}

#' The licence a dataset's bundle carries, in words
#'
#' Never invents a licence (workflows `CLAUDE.md`, Attribution):
#'
#' * a licence id with a bundled legal text (`inst/licenses/{id}.txt`: CC-BY-4.0,
#'   CC-BY-NC-4.0, CC-BY-SA-4.0, CC0-1.0) — `standard`: a short header, the provider's own
#'   statement URL where it differs from the licence's, then the full legal code;
#' * `US-PD` — `public-domain`: the 17 U.S.C. § 105 statement;
#' * `custom` — `custom`: the provider's terms URL, with no paraphrase of them;
#' * anything else, empty, `unknown` or an id with no text — `unstated`: says plainly that
#'   no licence has been established and whom to ask.
#'
#' @param record one dataset record from `datasets.json`
#' @param licenses_dir the directory holding the legal texts
#' @return A list: `kind` (`standard` | `public-domain` | `custom` | `unstated`), `id`
#'   (the record's licence id, or NA), `summary` (one line for the README) and `text`
#'   (the whole `LICENSE.txt`).
#' @export
#' @concept bundle
bundle_license <- function(record, licenses_dir = system.file("licenses", package = "calcofi4db")) {
  a    <- .bundle_attr(record)
  id   <- .s(a[["license"]])
  name <- .s(a[["license_name"]])
  url  <- .s(a[["license_url"]])
  nm   <- .s(record[["dataset_name"]]) %|e|% .s(record[["dataset_key"]])
  who  <- c(.bundle_contact(record), CC_DATA_CONTACT)
  ask  <- paste(unique(who), collapse = " or ")
  legal <- if (nzchar(id) && nzchar(licenses_dir)) file.path(licenses_dir, paste0(id, ".txt")) else ""
  if (nzchar(legal) && file.exists(legal)) {
    canon <- .CC_LICENSE_URLS[[id]] %||% url
    head <- c(
      sprintf("%s: %s", nm, id),
      sprintf("Licence: %s", name %|e|% id),
      sprintf("Licence page: %s", canon),
      if (nzchar(url) && !identical(sub("/$", "", url), sub("/$", "", canon)))
        sprintf("The provider's own statement of these terms: %s", url),
      "",
      "This licence applies to every data file in this download. Cite the dataset and the",
      "CalCOFI release as CITATION.md describes.",
      "",
      strrep("=", 78), "")
    return(list(kind = "standard", id = id,
                summary = sprintf("%s (%s); the full text is in LICENSE.txt", name %|e|% id, id),
                text = paste(c(head, readLines(legal, warn = FALSE, encoding = "UTF-8")), collapse = "\n")))
  }
  if (identical(id, "US-PD")) {
    txt <- c(
      sprintf("%s: US-PD", nm), "",
      "These data are a work of the United States Government and are not subject to copyright",
      "in the United States (17 U.S.C. § 105). They may be used without restriction; credit the",
      "provider and the CalCOFI release as CITATION.md describes.",
      if (nzchar(url)) c("", sprintf("Statement: %s", url)) else "https://www.usa.gov/government-works")
    return(list(kind = "public-domain", id = id,
                summary = "a work of the US Government, in the public domain in the United States; see LICENSE.txt",
                text = paste(txt, collapse = "\n")))
  }
  if (identical(id, "custom")) {
    txt <- c(
      sprintf("%s: terms set by the provider", nm), "",
      "This dataset is not distributed under a standard licence. Its provider states its own",
      "terms of use, and they apply to every data file in this download:", "",
      if (nzchar(url)) paste0("  ", url) else
        "  (the record names no address for them: ask before reusing these data)",
      "",
      "Read them before you reuse or redistribute these data. CalCOFI does not restate them",
      "here, so that nothing in this download can disagree with the provider's own words.",
      sprintf("Questions about these terms: %s.", ask))
    return(list(kind = "custom", id = id,
                summary = sprintf("custom terms set by the provider%s; see LICENSE.txt",
                                  if (nzchar(url)) paste0(" (", url, ")") else ""),
                text = paste(txt, collapse = "\n")))
  }
  txt <- c(
    sprintf("%s: no licence established", nm), "",
    "No licence has been established for this dataset yet. Its provider has not stated the",
    "terms under which these data may be reused, and CalCOFI does not assign a licence on a",
    "provider's behalf.", "",
    sprintf("To ask about reusing or redistributing these data, write to %s.", ask),
    "Whatever the terms turn out to be, cite the dataset and the CalCOFI release as",
    "CITATION.md describes.")
  list(kind = "unstated", id = if (nzchar(id)) id else NA_character_,
       summary = "no licence has been established yet; see LICENSE.txt for whom to ask",
       text = paste(txt, collapse = "\n"))
}

# canonical licence pages (metadata/license.csv's url column)
.CC_LICENSE_URLS <- list(
  `CC-BY-4.0`    = "https://creativecommons.org/licenses/by/4.0/",
  `CC-BY-NC-4.0` = "https://creativecommons.org/licenses/by-nc/4.0/",
  `CC-BY-SA-4.0` = "https://creativecommons.org/licenses/by-sa/4.0/",
  `CC0-1.0`      = "https://creativecommons.org/publicdomain/zero/1.0/")

`%|e|%` <- function(x, y) if (is.null(x) || !length(x) || is.na(x[1]) || !nzchar(x[1])) y else x

# a BibTeX @misc entry, `=` aligned, empty fields dropped (calcofi4r::cc_cite()'s shape)
.bib_entry <- function(key, fields) {
  fields <- fields[!vapply(fields, function(v) is.null(v) || is.na(v) || !nzchar(v), TRUE)]
  w <- max(nchar(names(fields)))
  body <- paste(sprintf("  %-*s = {%s}", w, names(fields), unlist(fields, use.names = FALSE)),
                collapse = ",\n")
  sprintf("@misc{%s,\n%s\n}", key, body)
}

#' The citation a dataset's bundle carries
#'
#' Cite both: the dataset (its provider's `citation_main`, with its DOI and any
#' `citation_others`) and the CalCOFI release it was downloaded from (`catalog.json`'s
#' `citation`, with the version DOI and the concept DOI for all versions). Where the provider
#' gave no citation, `CITATION.md` says so and no dataset entry is invented: the `.bib`
#' carries a comment instead. The BibTeX entries take calcofi4r::cc_cite()'s shape.
#'
#' @param record one dataset record from `datasets.json`
#' @param release a list: `version`, `release_date`, `citation`, `doi`, `concept_doi`
#'   (from the release's `catalog.json`)
#' @return A list: `md` (the `CITATION.md` text), `bib` (the `CITATION.bib` text) and
#'   `has_provider_citation` (logical).
#' @export
#' @concept bundle
bundle_citation <- function(record, release) {
  a    <- .bundle_attr(record)
  key  <- .s(record[["dataset_key"]])
  nm   <- .s(record[["dataset_name"]]) %|e|% key
  prov <- .s(record[["provider"]][["name"]]) %|e|% .s(record[["provider"]][["key"]])
  cit  <- trimws(gsub("[\r\n]+", " ", .s(a[["citation_main"]])))
  doi  <- .s(a[["doi"]])
  others <- trimws(as.character(unlist(a[["citation_others"]])))
  others <- others[nzchar(others)]
  ack  <- trimws(.s(a[["acknowledgement"]]))
  v    <- .s(release[["version"]]); rdoi <- .s(release[["doi"]]); cdoi <- .s(release[["concept_doi"]])
  rcit <- .s(release[["citation"]]) %|e|% release_citation(v, .s(release[["release_date"]]) %|e|% NULL,
                                                           doi = rdoi %|e|% NULL)
  page <- paste0(CC_DATASET_PAGE_BASE, key, "/")

  md <- c(
    sprintf("# How to cite %s", nm), "",
    "Please cite both the dataset and the CalCOFI database release you downloaded it from.", "",
    "## The dataset", "",
    if (nzchar(cit)) c(cit, "",
      if (nzchar(doi) && !grepl(doi, cit, fixed = TRUE)) c(sprintf("DOI: https://doi.org/%s", doi), "")) else
      c(sprintf(paste0("The provider (%s) has not supplied a citation for this dataset, and CalCOFI does ",
                       "not write one on its behalf. Until there is one, cite the release below and name ",
                       "the dataset, \"%s\", and its page, %s, in your text."), prov %|e|% "the provider", nm, page), ""),
    if (length(others)) c("Also cite:", "", paste0("- ", others), ""),
    "## The release", "",
    rcit, "",
    if (nzchar(rdoi)) sprintf("This release: https://doi.org/%s", rdoi),
    if (nzchar(cdoi)) sprintf("Every release (concept DOI): https://doi.org/%s", cdoi),
    "",
    if (nzchar(ack)) c("## Acknowledgement", "", ack, ""),
    "## Tell us how you use it", "",
    "Registering your use is encouraged and never required. It tells the people who collect",
    "these data how their work is used.", "",
    sprintf("- Register your use: %s", CC_REGISTER_USE_URL),
    sprintf("- Stay informed about new releases: %s", CC_STAY_INFORMED_URL), "",
    "`CITATION.bib` holds the same citations for a reference manager.")

  rel_key <- paste0("calcofi_release_", gsub("[^A-Za-z0-9]+", "_", v))
  rel_bib <- .bib_entry(rel_key, list(
    title = sprintf("CalCOFI Integrated Database, release %s", v), author = "{CalCOFI}",
    year = .bundle_year(.s(release[["release_date"]]), substr(v, 2, 5)),
    publisher = CC_RELEASE_PUBLISHER, doi = rdoi %|e|% NA_character_,
    url = if (nzchar(rdoi)) paste0("https://doi.org/", rdoi) else sprintf("%s?v=%s", CC_DB_SCHEMA_URL, v)))
  ds_bib <- if (nzchar(cit)) .bib_entry(gsub("[^A-Za-z0-9_]+", "_", key), list(
    title = nm, howpublished = cit, year = .bundle_year(cit, NA_character_),
    doi = doi %|e|% NA_character_,
    url = if (nzchar(doi)) paste0("https://doi.org/", doi) else page)) else
    sprintf("%% %s: the provider has not supplied a citation for this dataset; cite the release.", key)
  list(md = paste(md, collapse = "\n"),
       bib = paste(c(ds_bib, "", rel_bib, ""), collapse = "\n"),
       has_provider_citation = nzchar(cit))
}

#' The column dictionary of a dataset's bundle
#'
#' One row per column of every table in the bundle: the type it is written with, its units
#' and its meaning, from the release's own `metadata.json` (`columns{"table.column"}`); the
#' columns [bundle_build_tables()] adds are described by this package.
#'
#' @param con the connection [bundle_build_tables()] wrote into
#' @param tables the bundle's table names, in presentation order
#' @param meta_columns `metadata.json`'s `columns` block (a named list)
#' @param prefix the prefix [bundle_build_tables()] used
#' @return A data frame: `table`, `column`, `type`, `units`, `description`.
#' @export
#' @concept bundle
bundle_columns <- function(con, tables, meta_columns = list(), prefix = "bundle_") {
  rows <- lapply(tables, function(tb) {
    d <- DBI::dbGetQuery(con, sprintf("DESCRIBE %s", .q(paste0(prefix, tb))))
    meta <- lapply(d$column_name, function(cn) meta_columns[[paste0(tb, ".", cn)]] %||%
                     .BUNDLE_ADDED_COLUMNS[[cn]] %||% list())
    desc <- vapply(meta, function(m) {
      x <- .s(m[["description_md"]]) %|e|% .s(m[["description"]]) %|e|% .s(m[["name_long"]])
      trimws(gsub("[\r\n]+", " ", x))
    }, "")
    units <- vapply(meta, function(m) .s(m[["units"]]), "")
    units[d$column_name == "measurement_value" & !nzchar(units) & "units" %in% d$column_name] <-
      "see the units column"
    data.frame(table = tb, column = d$column_name, type = d$column_type, units = units,
               description = desc, stringsAsFactors = FALSE)
  })
  do.call(rbind, rows)
}

#' The README of a dataset's bundle
#'
#' Plain language: what the dataset is and who collected it, the release version and date,
#' what each file holds and how many rows, how the tables join, what was left out and where
#' to get it, how to take a subset by place and time in the Explorer, the dataset's page,
#' and the register-your-use and stay-informed links.
#'
#' @param record one dataset record from `datasets.json`
#' @param release a list with `version` and `release_date` (the release the bundle was
#'   built from)
#' @param format one of [bundle_formats()]
#' @param tables the data frame from [bundle_build_tables()]
#' @param plan the data frame from [bundle_plan()] (for the exclusions)
#' @param decision the data frame from [bundle_format_decision()]
#' @param license the list from [bundle_license()]
#' @param has_citation logical, from [bundle_citation()]
#' @param meta_tables `metadata.json`'s `tables` block, for a table this package has no
#'   label for
#' @return Character, the Markdown text.
#' @export
#' @concept bundle
bundle_readme <- function(record, release, format, tables, plan, decision, license,
                          has_citation = TRUE, meta_tables = list()) {
  stopifnot(format %in% bundle_formats())
  key  <- .s(record[["dataset_key"]])
  nm   <- .s(record[["dataset_name"]]) %|e|% key
  a    <- .bundle_attr(record)
  prov <- record[["provider"]] %||% list()
  v    <- .s(release[["version"]]); rd <- .s(release[["release_date"]])
  fmt_n <- function(n) format(n, big.mark = ",", scientific = FALSE, trim = TRUE)
  # the creators as the provider lists them, else the principal investigators
  people <- vapply(.rows(a[["creators"]]), function(cr) .s(cr[["name"]]), "")
  if (!any(nzchar(people))) people <- trimws(unlist(strsplit(as.character(unlist(a[["pi_names"]])), ";")))
  people <- unique(people[nzchar(people)])
  ext  <- c(csv = ".csv", xlsx = "", parquet = ".parquet")[[format]]
  what <- c(csv = "CSV tables", xlsx = "an Excel workbook", parquet = "Parquet tables")[[format]]
  label <- function(tb) {
    l <- .BUNDLE_TABLE_LABELS[tb]
    if (!is.na(l)) return(unname(l))
    .first_sentence(meta_tables[[tb]][["description_md"]]) %||% ""
  }
  desc <- trimws(.s(record[["description_md"]]))

  files <- if (format == "xlsx") c(
    "| file | what it holds | rows |", "|---|---|---:|",
    sprintf("| `%s` | one sheet per table below, after a README sheet and before a `columns` sheet | |",
            bundle_file_name(key, v, "xlsx", ".xlsx")),
    sprintf("| sheet `%s` | %s | %s |", tables$table, vapply(tables$table, label, ""), fmt_n(tables$rows))) else c(
    "| file | what it holds | rows |", "|---|---|---:|",
    sprintf("| `%s%s` | %s | %s |", tables$table, ext, vapply(tables$table, label, ""), fmt_n(tables$rows)))
  files <- c(files,
    "| `columns.csv` | every column of every table: its type, units and meaning | |",
    "| `CITATION.md`, `CITATION.bib` | how to cite the dataset and the release | |",
    "| `LICENSE.txt` | the terms these data may be reused under | |")

  how_read <- switch(format,
    csv = c("Each table is a UTF-8 CSV file with one header row. An empty cell means no value. Dates",
            "and times are ISO 8601 (`2021-07-14`, `2021-07-14 08:30:00`), in UTC. Numbers use a",
            "full stop for the decimal point and are never rounded for display."),
    xlsx = c("Each table is one sheet with a header row; dates and times are Excel dates (UTC). The",
             "same tables are in the CSV download."),
    parquet = c("Each table is an Apache Parquet file (typed columns, compressed), readable with R",
                "(`arrow::read_parquet()`), Python (`pandas.read_parquet()`), DuckDB or QGIS. The same",
                "tables are in the CSV download."))
  note <- decision$note[decision$format == format]
  if (length(note) && nzchar(note)) how_read <- c(how_read, "", paste("**Note:**", note))

  present <- tables$table
  joins <- c(
    if ("sample" %in% present)
      "- `sample_key` → `sample`: the sampling event a row belongs to. A sample's `parent_sample_key` is the event it sits in (a bottle in its cast, a net in its tow) and `root_sample_key` the top of that chain.",
    if ("taxon" %in% present) "- `taxon_key` → `taxon`: the taxon, with its names, identifiers and classification.",
    if ("measurement_type" %in% present) "- `measurement_type` → `measurement_type`: what was measured and in which units.",
    if ("cruise" %in% present) "- `cruise_key` → `cruise`: the cruise (`YYYY-MM` and the ship's NODC code) and its ship and dates.",
    if ("ship" %in% present) "- `ship_key` → `ship`: the ship.",
    if ("grid" %in% present) "- `grid_key` → `grid`: the CalCOFI grid cell the position falls in.",
    if ("dataset_taxon" %in% present) "- `dataset_taxon` pairs the provider's own taxon names and codes with `taxon_key`.",
    if ("obs_attribute" %in% present) "- `obs_attribute` rows belong to an observation by `sample_key`, `taxon_key` and `life_stage`.")
  added <- unique(unlist(strsplit(tables$added[nzchar(tables$added)], ",")))
  dropped <- unique(unlist(strsplit(tables$dropped[nzchar(tables$dropped)], ",")))
  excl <- plan[plan$rule == "excluded", , drop = FALSE]
  skipped <- decision[!decision$write, , drop = FALSE]

  lic_line <- license$summary
  txt <- c(
    sprintf("# %s", nm), "",
    sprintf("CalCOFI integrated database, release **%s**%s: %s.", v,
            if (nzchar(rd)) sprintf(" (%s)", rd) else "", what), "",
    if (nzchar(desc)) c(desc, ""),
    if (nzchar(.s(prov[["name"]]))) sprintf("- **Provided by:** %s%s", .s(prov[["name"]]),
                                            if (nzchar(.s(prov[["url"]]))) sprintf(" (%s)", .s(prov[["url"]])) else ""),
    if (length(people)) sprintf("- **People:** %s", paste(people, collapse = "; ")),
    sprintf("- **Dataset page:** %s%s/", CC_DATASET_PAGE_BASE, key),
    sprintf("- **Licence:** %s", lic_line),
    sprintf("- **How to cite:** see `CITATION.md`%s.",
            if (has_citation) "" else " (the provider has not supplied a dataset citation; cite the release)"),
    "",
    "## What is in this download", "",
    files, "",
    how_read, "",
    "## Start here", "",
    if ("obs" %in% present) c(
      "`obs` is the data: one measured value per row, with what was measured (`measurement_type`,",
      "and its `units`), the value (`measurement_value`), where (`latitude`, `longitude`, and `line`",
      "and `station` where the provider names a CalCOFI station), when (`datetime`), at what depth",
      "(`depth_min_m`, `depth_max_m`, metres) and, for biological data, which taxon",
      "(`scientific_name`). It can be used on its own; the other tables add detail.") else
      "This dataset has no observations in this release: `sample` lists its sampling events.",
    "",
    if (length(joins)) c("## How the tables join", "", joins, ""),
    if (length(added)) c(
      "## Columns added so a row reads on its own", "",
      sprintf(paste("Beside their keys, the tables carry %s, copied from the table the key points at",
                    "(described in `columns.csv`)."), paste(sprintf("`%s`", added), collapse = ", ")), ""),
    "## Left out of this download", "",
    if (length(dropped)) c(
      sprintf(paste("- The columns %s (map geometry and H3 cell ids) are left out: `latitude` and",
                    "`longitude` carry the position, and the release's own Parquet keeps them."),
              paste(sprintf("`%s`", dropped), collapse = ", "))),
    if (nrow(excl)) sprintf("- `%s`: %s. It is in the release's Parquet files (dataset page, *Get the data*).",
                            excl$table, excl$reason),
    if (nrow(skipped)) sprintf("- %s: %s", toupper(skipped$format), skipped$reason),
    if (!length(dropped) && !nrow(excl) && !nrow(skipped)) "Nothing: every row of this dataset in the release is here.",
    "",
    "## Only need part of it?", "",
    sprintf("The CalCOFI Explorer, %s, maps every dataset; choose a place and a time window there", CC_EXPLORE_URL),
    "and download just those observations.", "",
    "## Tell us how you use it", "",
    "Registering your use is encouraged and never required.", "",
    sprintf("- Register your use: %s", CC_REGISTER_USE_URL),
    sprintf("- Stay informed about new releases: %s", CC_STAY_INFORMED_URL),
    sprintf("- Questions: %s%s", CC_DATA_CONTACT,
            if (!is.null(.bundle_contact(record))) sprintf(" (this dataset: %s)", .bundle_contact(record)) else ""),
    "")
  paste(txt, collapse = "\n")
}

# write --------------------------------------------------------------------------------

# a fixed instant for every member of a zip: the release's own date, else a constant
.bundle_stamp <- function(release_date) {
  d <- tryCatch(as.Date(.s(release_date)), error = function(e) NA)
  if (is.na(d)) d <- as.Date("2026-01-01")
  as.POSIXct(paste(format(d), "00:00:00"), tz = "UTC")
}

#' Zip files so the same content always gives the same bytes
#'
#' A zip entry records each file's modification time and permissions, and the time in local
#' time: two builds of identical files a minute apart, or on machines in different time zones,
#' gave different bytes, and every hash-compared upload saw a change. Every member gets one
#' fixed instant and mode `644`, the archive is written with `TZ=UTC`, and the members go in
#' the order given.
#'
#' @param root the directory the member paths are relative to
#' @param files member paths relative to `root`, in archive order
#' @param zip_path the archive to write (replaced)
#' @param stamp a POSIXct every member is dated to
#' @param compression_level 0–9
#' @return `zip_path`, invisibly.
#' @export
#' @concept bundle
#' @importFrom zip zip
bundle_zip <- function(root, files, zip_path, stamp, compression_level = 6L) {
  paths <- file.path(root, files)
  stopifnot(all(file.exists(paths)))
  Sys.chmod(paths[!dir.exists(paths)], "644")
  Sys.setFileTime(paths, rep(stamp, length(paths)))
  old_tz <- Sys.getenv("TZ", unset = NA)
  Sys.setenv(TZ = "UTC")
  on.exit(if (is.na(old_tz)) Sys.unsetenv("TZ") else Sys.setenv(TZ = old_tz), add = TRUE)
  zip_path <- normalizePath(zip_path, mustWork = FALSE)
  if (file.exists(zip_path)) unlink(zip_path)
  zip::zip(zip_path, files = files, root = root, mode = "mirror",
           compression_level = compression_level, include_directories = FALSE)
  invisible(zip_path)
}

# libxlsxwriter stamps docProps/core.xml with the wall clock: rewrite it to the fixed instant
# and re-zip the workbook member by member in its own order, so a workbook is reproducible too
.xlsx_fix_time <- function(path, stamp) {
  tmp <- tempfile("xlsx_"); dir.create(tmp)
  on.exit(unlink(tmp, recursive = TRUE), add = TRUE)
  members <- zip::zip_list(path)$filename
  zip::unzip(path, exdir = tmp)
  core <- file.path(tmp, "docProps", "core.xml")
  if (file.exists(core)) {
    x <- readLines(core, warn = FALSE, encoding = "UTF-8")
    x <- gsub("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
              format(stamp, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"), x)
    writeLines(x, core, useBytes = TRUE)
  }
  bundle_zip(tmp, members, path, stamp)
}

.write_text <- function(text, path) {
  con <- file(path, open = "wb")
  on.exit(close(con))
  writeBin(charToRaw(enc2utf8(paste0(sub("\n+$", "", text), "\n"))), con)
}

#' Write a dataset's bundle zips
#'
#' For each format [bundle_format_decision()] offers, a folder named like the zip
#' ([bundle_file_name()] without `.zip`) holding the tables and `README.md`,
#' `CITATION.md`, `CITATION.bib`, `LICENSE.txt` and `columns.csv`, zipped with
#' [bundle_zip()] (fixed order and timestamps, so unchanged content gives the same sha256)
#' and then removed. CSV and Parquet come straight from the tables in `con`
#' (`COPY … TO`); the Parquet uses the release's pinned writer settings on one thread; the
#' Excel workbook is written with `writexl` (a `README` sheet first, a `columns` sheet last)
#' and its internal timestamp fixed.
#'
#' A zip over `max_bytes` stops the build with its size: a file too large for a browser
#' download is a decision for a person, never dropped silently.
#'
#' @param con the connection [bundle_build_tables()] wrote into
#' @param record one dataset record from `datasets.json`
#' @param release a list: `version`, `release_date`, `citation`, `doi`, `concept_doi`
#' @param tables the data frame from [bundle_build_tables()]
#' @param plan the data frame from [bundle_plan()]
#' @param out_dir where the zips are written (created)
#' @param meta_columns,meta_tables `metadata.json`'s `columns` and `tables` blocks
#' @param xlsx_max_rows passed to [bundle_format_decision()]
#' @param max_bytes the largest zip allowed
#' @param prefix the prefix [bundle_build_tables()] used
#' @param licenses_dir passed to [bundle_license()]
#' @return A data frame, one row per [bundle_formats()]: `format`, `written`, `file`,
#'   `path`, `bytes`, `sha256`, `excel_openable`, `reason`, `note`.
#' @export
#' @concept bundle
#' @importFrom writexl write_xlsx
#' @seealso [bundle_manifest_entry()]
write_bundle <- function(con, record, release, tables, plan, out_dir,
                         meta_columns = list(), meta_tables = list(),
                         xlsx_max_rows = BUNDLE_XLSX_MAX_ROWS, max_bytes = BUNDLE_MAX_ZIP_BYTES,
                         prefix = "bundle_",
                         licenses_dir = system.file("licenses", package = "calcofi4db")) {
  key <- .s(record[["dataset_key"]]); v <- .s(release[["version"]])
  stopifnot(nzchar(key), nzchar(v), nrow(tables) > 0)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  out_dir <- normalizePath(out_dir)
  stamp <- .bundle_stamp(release[["release_date"]])
  decision <- bundle_format_decision(stats::setNames(tables$rows, tables$table), xlsx_max_rows)
  lic  <- bundle_license(record, licenses_dir)
  cit  <- bundle_citation(record, release)
  cols <- bundle_columns(con, tables$table, meta_columns, prefix)
  docs <- c("README.md", "CITATION.md", "CITATION.bib", "LICENSE.txt", "columns.csv")

  res <- lapply(seq_len(nrow(decision)), function(i) {
    f <- decision$format[i]
    row <- data.frame(format = f, written = FALSE, file = NA_character_, path = NA_character_,
                      bytes = NA_real_, sha256 = NA_character_,
                      excel_openable = decision$excel_openable[i], reason = decision$reason[i],
                      note = decision$note[i], stringsAsFactors = FALSE)
    if (!decision$write[i]) return(row)
    stem <- bundle_file_name(key, v, f, "")
    dir  <- file.path(out_dir, stem)
    unlink(dir, recursive = TRUE); dir.create(dir)
    on.exit(unlink(dir, recursive = TRUE), add = TRUE)
    readme <- bundle_readme(record, release, f, tables, plan, decision, lic,
                            has_citation = cit$has_provider_citation, meta_tables = meta_tables)
    .write_text(readme, file.path(dir, "README.md"))
    .write_text(cit$md, file.path(dir, "CITATION.md"))
    .write_text(cit$bib, file.path(dir, "CITATION.bib"))
    .write_text(lic$text, file.path(dir, "LICENSE.txt"))
    utils::write.csv(cols, file.path(dir, "columns.csv"), row.names = FALSE, na = "", fileEncoding = "UTF-8")
    data_files <- switch(f,
      csv = vapply(tables$table, function(tb) {
        p <- file.path(dir, paste0(tb, ".csv"))
        DBI::dbExecute(con, sprintf("COPY (SELECT * FROM %s) TO %s (FORMAT CSV, HEADER, NULLSTR '')",
                                    .q(paste0(prefix, tb)), .lit(p)))
        basename(p) }, ""),
      parquet = {
        th <- DBI::dbGetQuery(con, "SELECT current_setting('threads') AS t")$t
        DBI::dbExecute(con, "SET threads = 1")
        on.exit(DBI::dbExecute(con, sprintf("SET threads = %s", th)), add = TRUE)
        vapply(tables$table, function(tb) {
          p <- file.path(dir, paste0(tb, ".parquet"))
          DBI::dbExecute(con, sprintf(
            "COPY (SELECT * FROM %s) TO %s (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d, PARQUET_VERSION '%s')",
            .q(paste0(prefix, tb)), .lit(p), CC_PARQUET_WRITER$compression,
            as.integer(CC_PARQUET_WRITER$row_group_size), CC_PARQUET_WRITER$parquet_version))
          basename(p) }, "")
      },
      xlsx = {
        p <- file.path(dir, bundle_file_name(key, v, "xlsx", ".xlsx"))
        sheets <- c(
          list(README = data.frame(README = strsplit(readme, "\n", fixed = TRUE)[[1]], stringsAsFactors = FALSE)),
          stats::setNames(lapply(tables$table, function(tb)
            DBI::dbGetQuery(con, sprintf("SELECT * FROM %s", .q(paste0(prefix, tb))))), tables$table),
          list(columns = cols))
        writexl::write_xlsx(sheets, p)
        .xlsx_fix_time(p, stamp)
        basename(p)
      })
    zip_path <- file.path(out_dir, bundle_file_name(key, v, f))
    bundle_zip(out_dir, file.path(stem, c(docs[docs != "columns.csv"], unname(data_files), "columns.csv")),
               zip_path, stamp)
    bytes <- file.size(zip_path)
    if (bytes > max_bytes)
      stop(sprintf("%s is %.2f GB, over the %.2f GB a browser download is held to (max_bytes): ",
                   basename(zip_path), bytes / 1e9, max_bytes / 1e9),
           "decide how to split or serve it before publishing", call. = FALSE)
    row$written <- TRUE; row$file <- basename(zip_path); row$path <- zip_path; row$bytes <- bytes
    row$sha256 <- digest::digest(zip_path, algo = "sha256", file = TRUE)
    row
  })
  do.call(rbind, res)
}

# manifest -----------------------------------------------------------------------------

#' One dataset's entry in the bundle manifest the website reads
#'
#' The contract between `publish_to-zip.qmd` and calcofi.io: per dataset, the release the
#' bundles were built from (`version`) and last confirmed against (`checked_version`), the
#' licence kind, whether the provider gave a citation, the rows of every table, what was
#' left out, and per format either the file (`url`, `bytes`, `sha256`, `excel_openable`) or
#' the reason it is not offered, in plain words.
#'
#' @param record one dataset record from `datasets.json`
#' @param version the release the bundles were built from
#' @param release_date that release's date
#' @param checked_version the release this run confirmed them against
#' @param fingerprint the list from [publish_fingerprint()]
#' @param tables the data frame from [bundle_build_tables()]
#' @param plan the data frame from [bundle_plan()]
#' @param written the data frame from [write_bundle()]
#' @param license_kind,has_provider_citation from [bundle_license()] / [bundle_citation()]
#' @param base_url the public root the zips are served from (`{base_url}/{dataset_key}/{file}`)
#' @return A named list, ready for `jsonlite::toJSON(auto_unbox = TRUE)`.
#' @export
#' @concept bundle
bundle_manifest_entry <- function(record, version, release_date, checked_version, fingerprint,
                                  tables, plan, written, license_kind, has_provider_citation,
                                  base_url = CC_BUNDLE_BASE_URL) {
  key <- .s(record[["dataset_key"]])
  rows <- stats::setNames(as.list(as.numeric(tables$rows)), tables$table)
  excl <- plan[plan$rule == "excluded", , drop = FALSE]
  list(
    dataset_key     = key,
    dataset_name    = .chr_or_null(record[["dataset_name"]]),
    version         = version,
    release_date    = .chr_or_null(release_date),
    checked_version = checked_version,
    license         = .chr_or_null(.bundle_attr(record)[["license"]]),
    license_kind    = license_kind,
    has_provider_citation = isTRUE(has_provider_citation),
    input_fingerprint = list(hash = fingerprint$hash, parts = as.list(fingerprint$parts)),
    tables          = lapply(seq_len(nrow(tables)), function(i) list(
      table = tables$table[i], rows = as.numeric(tables$rows[i]),
      role = plan$role[match(tables$table[i], plan$table)])),
    excluded        = lapply(seq_len(nrow(excl)), function(i) list(table = excl$table[i], reason = excl$reason[i])),
    bundles         = lapply(seq_len(nrow(written)), function(i) {
      w <- written[i, ]
      if (!isTRUE(w$written))
        return(list(format = w$format, written = FALSE, reason = w$reason))
      e <- list(format = w$format, written = TRUE, file = w$file,
                url = sprintf("%s/%s/%s", sub("/+$", "", base_url), key, w$file),
                bytes = as.numeric(w$bytes), sha256 = w$sha256, media_type = "application/zip",
                excel_openable = isTRUE(w$excel_openable), rows = rows)
      if (nzchar(.s(w$note))) e$note <- w$note
      e
    }))
}

#' The bundle manifest of every dataset published by `publish_to-zip.qmd`
#'
#' @param url the manifest's address
#' @param fetch the HTTP function (tests inject one)
#' @return The parsed manifest (a list with `datasets[]`), or NULL when it does not answer.
#' @export
#' @concept bundle
fetch_bundle_manifest <- function(url = CC_BUNDLE_MANIFEST_URL, fetch = NULL) {
  if (is.null(fetch)) fetch <- function(url, ...) .http_get(url, ...)
  r <- fetch(url)
  if (!.ok200(r) || !nzchar(.s(r[["content"]]))) return(NULL)
  tryCatch(jsonlite::fromJSON(r[["content"]], simplifyVector = FALSE), error = function(e) NULL)
}

# a dataset's bundle entry for a release: the one built from it, else the newest built
.bundle_entry_for <- function(bundles, key, version = NULL) {
  ents <- Filter(function(e) identical(.s(e[["dataset_key"]]), key), .rows(bundles[["datasets"]]))
  if (!length(ents)) return(NULL)
  pick <- Filter(function(e) identical(.s(e[["version"]]), .s(version)), ents)
  if (length(pick)) return(pick[[1]])
  vv <- vapply(ents, function(e) .s(e[["version"]]), "")
  ents[order(vv, decreasing = TRUE)][[1]]
}
