# combine_sensor_pair(): one value from a CTD's two sensors, by the provider's flags -------------
#
# CalCOFI CTD files carry two sensors for temperature, salinity and oxygen and a
# quality code per sensor (`metadata/measurement_qual.csv`, code_set `ctd`):
# 0 / blank good · 1 use primary · 2 use secondary · 8 questionable · 9 bad. The
# files also publish an average (`*_ave_*`), but how it treated a flagged sensor is
# the processing software's business. Rasmus Swalethorp (2026-09-09, on the
# transect plotter's data streams): compute the mean of the two CORRECTED sensors
# ourselves, one alone when the other is flagged 8/9, honouring 1/2 — "theoretically
# already done within decodr, but this is a safer option". This is that rule, once,
# in SQL and in R, so ctd-transects, the Explorer and a notebook cannot disagree.

#' Combine a CTD sensor pair by the provider's quality flags
#'
#' The rule, in precedence order:
#' 1. a sensor flagged **8** (questionable) or **9** (bad) is dropped;
#' 2. a **1** (use primary) on either sensor selects sensor 1 alone, a **2** (use
#'    secondary) selects sensor 2 alone — when both say the same thing; if they
#'    disagree (one says 1, the other 2) neither is trusted over the other and the
#'    mean is used;
#' 3. otherwise the **mean** of the two; one alone when the other is `NULL`;
#'    `NULL` when neither survives.
#'
#' Flags are compared as the source writes them — `"8"`, `"8.0"`, `8`, `NA` and `""`
#' all mean what the vocabulary says — so a consumer can pass `measurement_qual`
#' straight from `obs`. `combine_sensor_pair_sql()` returns the same rule as one SQL
#' expression over four column names or expressions, for use in a DuckDB query
#' (the transect builder, the Explorer's SQL).
#'
#' @param v1,v2 numeric: the two sensors' values (sensor 1 = primary).
#' @param q1,q2 their quality codes (character or numeric; `NA` = good).
#' @return `combine_sensor_pair()`: a numeric vector; `combine_sensor_pair_sql()`: a
#'   length-one character SQL expression.
#' @export
#' @concept ctd
#' @examples
#' combine_sensor_pair(c(10, 10, 10, 10, NA, 10), c(12, 12, 12, 12, 12, 12),
#'                     q1 = c(NA, "8", "1", "2", NA, "9"), q2 = c(NA, NA, NA, NA, NA, "9.0"))
#' # 11 10 10 12 12 NA
#' combine_sensor_pair_sql("salinity_1_corr", "salinity_2_corr", "q1", "q2")
combine_sensor_pair <- function(v1, v2, q1 = NA, q2 = NA) {
  n <- max(length(v1), length(v2))
  v1 <- rep_len(as.numeric(v1), n); v2 <- rep_len(as.numeric(v2), n)
  q1 <- rep_len(.norm_qual(q1), n);  q2 <- rep_len(.norm_qual(q2), n)
  bad1 <- q1 %in% c("8", "9"); bad2 <- q2 %in% c("8", "9")
  a <- ifelse(bad1, NA_real_, v1)
  b <- ifelse(bad2, NA_real_, v2)
  say1 <- (q1 == "1" & !is.na(q1)) | (q2 == "1" & !is.na(q2))
  say2 <- (q1 == "2" & !is.na(q1)) | (q2 == "2" & !is.na(q2))
  use1 <- say1 & !say2 & !is.na(a)
  use2 <- say2 & !say1 & !is.na(b)
  both <- !is.na(a) & !is.na(b)
  out <- ifelse(both, (a + b) / 2, ifelse(is.na(a), b, a))
  out[use1] <- a[use1]
  out[use2] <- b[use2]
  as.numeric(out)
}

#' @rdname combine_sensor_pair
#' @export
combine_sensor_pair_sql <- function(v1, v2, q1, q2) {
  stopifnot(is.character(v1), is.character(v2), is.character(q1), is.character(q2),
            length(v1) == 1, length(v2) == 1, length(q1) == 1, length(q2) == 1)
  nq <- function(q) glue::glue("regexp_replace(CAST({q} AS VARCHAR), '\\.0+$', '')")
  n1 <- nq(q1); n2 <- nq(q2)
  a  <- glue::glue("CASE WHEN {n1} IN ('8', '9') THEN NULL ELSE {v1} END")
  b  <- glue::glue("CASE WHEN {n2} IN ('8', '9') THEN NULL ELSE {v2} END")
  say1 <- glue::glue("({n1} = '1' OR {n2} = '1')")
  say2 <- glue::glue("({n1} = '2' OR {n2} = '2')")
  as.character(glue::glue(
    "CASE WHEN COALESCE({say1}, FALSE) AND NOT COALESCE({say2}, FALSE) AND ({a}) IS NOT NULL THEN ({a})
          WHEN COALESCE({say2}, FALSE) AND NOT COALESCE({say1}, FALSE) AND ({b}) IS NOT NULL THEN ({b})
          WHEN ({a}) IS NOT NULL AND ({b}) IS NOT NULL THEN (({a}) + ({b})) / 2
          ELSE COALESCE(({a}), ({b})) END"))
}

# "8.0" -> "8", 8 -> "8", "" -> NA
.norm_qual <- function(q) {
  q <- as.character(q)
  q <- sub("\\.0+$", "", q)
  q[!is.na(q) & q == ""] <- NA_character_
  q
}
