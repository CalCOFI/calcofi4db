# site_key: the CalCOFI station as a string, one canonical spelling ------------
#
# `sample.site_key` is the real line/station ("090.0 028.0"); `grid_key` is the
# point-in-polygon cell it falls in ("st30-ln90"). The inshore cells hold 2–4
# stations, all occupied every cruise (st30-ln90 = 90.30, 90.28, 90.27.7 and
# 88.5/30.1), so a section or a climatology keyed on the cell blends stations
# 15–30 km apart where the gradient is steepest. Keying on the station needs one
# spelling of it: v2026.09.06 shipped 28 CTD casts with the source's own forms
# ("93.3    26.4", "0093. 060.0", "090.0 27.76", "88.50 030.1") beside the
# canonical "093.3 026.4", because the ingest carried `Sta_ID` through unchanged.

#' The canonical `site_key` spelling, as a SQL expression
#'
#' Returns a DuckDB expression that rewrites any spelling of a CalCOFI line/station
#' pair into the one form the rest of the pipeline uses,
#' `printf('%05.1f %05.1f', line, station)` — the form
#' [standardize_site_key()] writes from numeric columns. The first two signed
#' decimal numbers in the string are the line and the station, whatever separates
#' them (a space, several, a comma) and however they are padded; a string with
#' fewer than two numbers becomes `NULL`. A negative station (ichthyo's
#' `"011.7 -02.6"`, inshore of the line origin) keeps its sign; the width-5 format
#' makes it `-02.6`, which is what `%05.1f` already produced for those rows.
#'
#' Rounding is to one decimal, the grid's own resolution (0.1 station unit is
#' 0.74 km along a line): the one source form with two decimals, `090.0 27.76`,
#' becomes `090.0 027.8`.
#'
#' @param expr a SQL expression or column name holding the string (default
#'   `"site_key"`).
#' @return a length-one character SQL expression.
#' @seealso [normalize_site_key()] for the R vector form, [check_site_key_format()]
#'   for the release gate, [append_sample()] which applies this to every ingest.
#' @export
#' @concept keys
#' @examples
#' site_key_sql("site_key")
site_key_sql <- function(expr = "site_key") {
  stopifnot(is.character(expr), length(expr) == 1, nzchar(expr))
  nums <- glue::glue("regexp_extract_all(CAST({expr} AS VARCHAR), '-?[0-9]+(?:\\.[0-9]+)?')")
  as.character(glue::glue(
    "CASE WHEN {expr} IS NULL THEN NULL
          WHEN len({nums}) < 2 THEN NULL
          ELSE printf('%05.1f %05.1f',
                      CAST({nums}[1] AS DOUBLE), CAST({nums}[2] AS DOUBLE)) END"))
}

#' Normalize `site_key` strings in R
#'
#' The vector twin of [site_key_sql()]: the first two signed decimal numbers in each
#' string, formatted `sprintf("%05.1f %05.1f", line, station)`; `NA` where there are
#' fewer than two. Use it in an ingest that builds `site_key` in R before the arm
#' reaches [append_sample()] (which normalises again, harmlessly).
#'
#' @param x character vector of station strings in any spelling.
#' @return character vector, same length, canonical or `NA_character_`.
#' @export
#' @concept keys
#' @examples
#' normalize_site_key(c("93.3    26.4", "0093. 060.0", "090.0 27.76", "88.50 030.1",
#'                      "093.3 026.4", "011.7 -02.6", "93.3,26.4", "line 90", NA))
normalize_site_key <- function(x) {
  stopifnot(is.character(x) || all(is.na(x)))
  x <- as.character(x)
  out <- rep(NA_character_, length(x))
  ok  <- !is.na(x)
  if (!any(ok)) return(out)
  nums <- regmatches(x[ok], gregexpr("-?[0-9]+(?:\\.[0-9]+)?", x[ok]))
  two  <- vapply(nums, length, integer(1)) >= 2L
  out[ok][two] <- vapply(nums[two], function(n)
    sprintf("%05.1f %05.1f", as.numeric(n[1]), as.numeric(n[2])), character(1))
  out
}

#' Fail unless every non-NULL `site_key` is in its canonical spelling
#'
#' The release gate for [site_key_sql()]: a `site_key` that is not equal to its own
#' normalisation would key a section or a climatology cell onto a station that
#' does not exist. NULL is allowed (underway, transect and region-pooled samples
#' have no station). Reports per dataset so the offending ingest is named.
#'
#' @param con DuckDB connection holding the sample table.
#' @param sample_tbl the sample table (default `"sample"`).
#' @return invisibly, a data.frame `dataset_key`, `n_rows`, `n_site`, `n_bad`,
#'   `example` (one offending value, or NA).
#' @export
#' @concept validation
check_site_key_format <- function(con, sample_tbl = "sample") {
  canon <- site_key_sql("site_key")
  res <- DBI::dbGetQuery(con, glue::glue("
    SELECT dataset_key,
           COUNT(*)                                                    AS n_rows,
           COUNT(site_key)                                             AS n_site,
           COUNT(*) FILTER (WHERE site_key IS NOT NULL
                              AND site_key IS DISTINCT FROM ({canon})) AS n_bad,
           any_value(site_key) FILTER (WHERE site_key IS NOT NULL
                              AND site_key IS DISTINCT FROM ({canon})) AS example
    FROM {sample_tbl}
    GROUP BY dataset_key ORDER BY dataset_key"))
  bad <- res[res$n_bad > 0, , drop = FALSE]
  if (nrow(bad))
    stop("site_key is not canonical in the release: ",
         paste(sprintf("%s: %d row(s), e.g. '%s'", bad$dataset_key, bad$n_bad, bad$example),
               collapse = "; "),
         " — every ingest's sample arm passes through append_sample(), which normalises; ",
         "a table written another way must call standardize_site_key() or normalize_site_key().",
         call. = FALSE)
  invisible(res)
}
