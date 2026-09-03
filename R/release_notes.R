# release notes: narrative from RELEASES.md + a generated appendix --------------
#
# `RELEASE_NOTES.md` was a paste0() template in release_database.qmd: ~55 of its
# 65 lines were a string literal that still listed four datasets (sixteen ship)
# and tables retired months earlier; a diff between two versions was row counts
# and the version string. The narrative — what changed and why — lived only in
# commit subjects, the package NEWS files and a session log.
#
# `RELEASES.md` (workflows root) is now the database's NEWS file: one `# vX`
# section per release, newest first, `# Unreleased` collecting changes until the
# next cut. The release FAILS without a section for its version — the same rule
# the packages apply to NEWS.md. Each version's RELEASE_NOTES.md is that section
# plus an appendix generated from the sidecars, and can be re-published at any
# time (notes are not data, so the republish guard does not apply).

.rn_version_key <- function(v) {
  # "v2026.08.25" -> sortable date-ish key; "v2026.03" -> "2026.03.00"
  p <- strsplit(sub("^v", "", v), ".", fixed = TRUE)[[1]]
  p <- c(p, rep("00", 3 - length(p)))[1:3]
  paste(sprintf("%02d", as.integer(p)), collapse = ".")
}

#' Split RELEASES.md into its top-level sections
#'
#' @param md Character vector of lines, or a single string.
#' @return A tibble with `heading` (the `# ` line), `versions` (list of
#'   version strings the heading names — one, or two for a range), `date`
#'   (from `(YYYY-MM-DD)` if present), `body` (lines below the heading, trimmed).
#' @export
#' @concept release
release_notes_sections <- function(md) {
  lines <- if (length(md) == 1 && grepl("\n", md)) strsplit(md, "\n")[[1]] else md
  h <- grep("^# ", lines)
  if (!length(h)) return(tibble::tibble(heading = character(), versions = list(),
                                        date = character(), body = character()))
  ends <- c(h[-1] - 1, length(lines))
  out <- lapply(seq_along(h), function(i) {
    head <- sub("^# ", "", lines[h[i]])
    vers <- regmatches(head, gregexpr("v20[0-9]{2}(\\.[0-9]{2}){1,2}", head))[[1]]
    if (!length(vers) && grepl("^Unreleased", head, ignore.case = TRUE)) vers <- "Unreleased"
    d <- regmatches(head, regexpr("\\(([0-9]{4}-[0-9]{2}-[0-9]{2})", head))
    body <- if (ends[i] >= h[i] + 1) lines[(h[i] + 1):ends[i]] else character()
    body <- sub("\\s+$", "", body)
    while (length(body) && !nzchar(body[1])) body <- body[-1]
    while (length(body) && !nzchar(body[length(body)])) body <- body[-length(body)]
    tibble::tibble(heading = head, versions = list(vers),
                   date = if (length(d)) sub("^\\(", "", d) else NA_character_,
                   body = paste(body, collapse = "\n"))
  })
  do.call(rbind, out)
}

#' The RELEASES.md section that documents a version
#'
#' Matches a heading naming the version exactly, or a range heading
#' (`# v2026.08.04 – v2026.08.06`) that contains it.
#' @param md RELEASES.md text or lines.
#' @param version e.g. `"v2026.08.25"`.
#' @return A one-row tibble as from [release_notes_sections()], or `NULL`.
#' @export
#' @concept release
release_notes_section <- function(md, version) {
  if (!grepl("^v20[0-9]{2}(\\.[0-9]{2}){1,2}$", version)) return(NULL)
  s <- release_notes_sections(md)
  if (!nrow(s)) return(NULL)
  k <- .rn_version_key(version)
  hit <- vapply(s$versions, function(v) {
    v <- setdiff(v, "Unreleased")
    if (!length(v)) return(FALSE)
    if (version %in% v) return(TRUE)
    if (length(v) == 2) {
      ks <- sort(vapply(v, .rn_version_key, ""))
      return(k >= ks[1] && k <= ks[2])
    }
    FALSE
  }, logical(1))
  if (!any(hit)) return(NULL)
  s[which(hit)[1], ]
}

#' Turn `# Unreleased` into the section for a version being cut
#'
#' If `# Unreleased` has a non-empty body it is renamed
#' `# {version} ({date})` and a fresh empty `# Unreleased` is inserted above
#' it. If it is empty (or absent) and no section for `version` exists, this
#' errors: a release with nothing to say about itself is the failure mode this
#' file exists to prevent.
#' @param md RELEASES.md text (single string) or lines.
#' @param version,date the release being cut.
#' @return The updated text as a single string.
#' @export
#' @concept release
promote_unreleased <- function(md, version, date = Sys.Date()) {
  lines <- if (length(md) == 1 && grepl("\n", md)) strsplit(md, "\n")[[1]] else md
  s <- release_notes_sections(lines)
  i_unrel <- which(vapply(s$versions, function(v) "Unreleased" %in% v, logical(1)))
  has_section <- !is.null(release_notes_section(lines, version))
  unrel_body <- if (length(i_unrel)) s$body[i_unrel[1]] else ""
  if (!nzchar(unrel_body)) {
    if (has_section) return(paste(lines, collapse = "\n"))
    stop("RELEASES.md has no `# ", version, "` section and `# Unreleased` is empty. ",
         "Every change that alters release content must add to `# Unreleased` in ",
         "the same commit; write the entry before cutting the release.", call. = FALSE)
  }
  if (has_section)
    stop("RELEASES.md has BOTH a non-empty `# Unreleased` and a `# ", version,
         "` section — merge them by hand.", call. = FALSE)
  h <- grep("^# Unreleased", lines, ignore.case = TRUE)[1]
  lines[h] <- sprintf("# %s (%s)", version, format(as.Date(date), "%Y-%m-%d"))
  lines <- append(lines, c("# Unreleased", ""), after = h - 1)
  paste(lines, collapse = "\n")
}

#' Render a version's RELEASE_NOTES.md: narrative + generated appendix
#'
#' @param version the release.
#' @param releases_md RELEASES.md text or lines (must contain the section).
#' @param catalog parsed `catalog.json` (list) or `NULL`.
#' @param metadata parsed `metadata.json` (list) or `NULL` — its `datasets`
#'   names are listed.
#' @param test_results parsed `test_results.json` (list) or `NULL`.
#' @param pkg_versions named character vector, e.g.
#'   `c(calcofi4db = "3.20.1", calcofi4r = "1.9.0")`, or `NULL`.
#' @param promoted whether `latest.txt` points at this version (affects one line).
#' @return The markdown as a single string.
#' @export
#' @concept release
render_release_notes <- function(version, releases_md, catalog = NULL,
                                 metadata = NULL, test_results = NULL,
                                 pkg_versions = NULL, promoted = NA) {
  sec <- release_notes_section(releases_md, version)
  if (is.null(sec))
    stop("RELEASES.md has no section for ", version, call. = FALSE)
  # an exact heading carries the authored date; a RANGE heading documents several
  # versions, so each takes its own catalog date (v2026.08.05 is not 2026-08-04)
  exact <- identical(sec$versions[[1]], version)
  date <- if (exact && !is.na(sec$date)) sec$date else
    if (!is.null(catalog$release_date)) catalog$release_date else
    if (!is.na(sec$date)) sec$date else NA_character_
  out <- c(
    sprintf("# CalCOFI integrated database release %s", version),
    "",
    sprintf("**Release date:** %s%s", if (is.na(date)) "unknown" else date,
            if (isTRUE(promoted)) " · **promoted** (`latest.txt`)" else ""),
    if (!identical(sec$versions[[1]], version))
      sprintf("*Documented with %s.*", sec$heading),
    "",
    sec$body,
    "",
    "## Contents (generated)")
  if (!is.null(catalog) && length(catalog$tables)) {
    tb <- catalog$tables
    # a deprecated table (catalog.json `deprecated` / `replaced_by` / `removed_in`,
    # 3.31.0) says so in the appendix, whichever form the catalog was read in
    dep_note <- function(dep, rep, rem) {
      if (!isTRUE(dep)) return("")
      rep <- as.character(unlist(rep)); rep <- rep[!is.na(rep)]
      sprintf("deprecated%s%s",
              if (length(rep)) paste0(" → ", paste(sprintf("`%s`", rep), collapse = ", ")) else "",
              if (!is.null(rem) && !is.na(rem)) sprintf(" (objects removed in %s)", rem) else "")
    }
    if (!is.data.frame(tb)) {
      notes <- vapply(tb, function(t) dep_note(t$deprecated, t$replaced_by, t$removed_in), "")
      tb <- do.call(rbind, lapply(tb, function(t)
        data.frame(name = t$name, rows = t$rows %||% NA_real_,
                   partitioned = isTRUE(t$partitioned), supplemental = isTRUE(t$supplemental))))
    } else {
      notes <- vapply(seq_len(nrow(tb)), function(i) dep_note(
        tb$deprecated[i] %||% FALSE,
        if (is.null(tb$replaced_by)) NULL else tb$replaced_by[[i]],
        if (is.null(tb$removed_in)) NULL else tb$removed_in[i]), "")
    }
    tb$dep_note <- notes
    for (cl in c("partitioned", "supplemental"))
      if (is.null(tb[[cl]])) tb[[cl]] <- FALSE
    if (is.null(tb$rows)) tb$rows <- NA_real_
    tb <- tb[order(tb$supplemental %in% TRUE, tb$name), ]
    fmt <- function(x) format(x, big.mark = ",", scientific = FALSE, trim = TRUE)
    out <- c(out, "", "| table | rows | |", "|---|---:|---|",
             sprintf("| `%s` | %s | %s |", tb$name, fmt(tb$rows),
                     ifelse(nzchar(tb$dep_note), tb$dep_note,
                     ifelse(tb$supplemental %in% TRUE, "supplemental",
                            ifelse(tb$partitioned %in% TRUE, "partitioned", "")))),
             "",
             sprintf("**%d tables, %s rows, %s GB.**", nrow(tb), fmt(sum(tb$rows, na.rm = TRUE)),
                     if (!is.null(catalog$total_size)) sprintf("%.2f", catalog$total_size / 1e9) else "?"))
  }
  if (!is.null(metadata) && length(metadata$datasets)) {
    ds <- metadata$datasets
    keys <- if (is.data.frame(ds)) ds$dataset_key %||% ds$key else names(ds)
    if (!is.null(keys))
      out <- c(out, "", sprintf("**Datasets (%d):** %s", length(keys),
                                paste(sprintf("`%s`", sort(keys)), collapse = ", ")))
  }
  if (!is.null(test_results)) {
    out <- c(out, "", sprintf(
      "**Validation:** %s pass / %s fail / %s skip (consumer-contract suite, %s).",
      test_results$n_pass %||% "?", test_results$n_fail %||% "?",
      test_results$n_skip %||% "?", test_results$tested_at %||% "untested"))
  }
  if (!is.null(pkg_versions) && length(pkg_versions)) {
    out <- c(out, "", sprintf("**Software:** %s.", paste(
      sprintf("%s %s", names(pkg_versions), pkg_versions), collapse = ", ")))
  }
  out <- c(out, "", "## Access", "",
    "```r", sprintf('con <- calcofi4r::cc_get_db(version = "%s")', version), "```",
    "```python", sprintf('con = calcofi4py.cc_get_db("%s")', version), "```",
    sprintf("Parquet: `https://storage.googleapis.com/calcofi-db/ducklake/releases/%s/parquet/{table}.parquet`; ",
            version),
    "full history: [RELEASES.md](https://storage.googleapis.com/calcofi-db/ducklake/releases/RELEASES.md).")
  paste(out, collapse = "\n")
}

#' Render and (re)publish RELEASE_NOTES.md for a version
#'
#' Notes-only: renders from `RELEASES.md` + the version's local sidecars,
#' writes `dir_releases/{version}/RELEASE_NOTES.md`, and uploads it and
#' `RELEASES.md` to the bucket with `cache-control: no-cache`. Safe to run for
#' any version at any time — it never touches data, `catalog.json` or
#' `latest.txt`.
#'
#' @param version the release.
#' @param releases_md path to RELEASES.md.
#' @param dir_releases the local `data/releases` directory.
#' @param bucket GCS bucket (default `"calcofi-db"`); `NULL` to skip upload.
#' @param pkg_versions see [render_release_notes()].
#' @return Invisibly, the local RELEASE_NOTES.md path.
#' @export
#' @concept release
publish_release_notes <- function(version, releases_md, dir_releases,
                                  bucket = "calcofi-db", pkg_versions = NULL) {
  stopifnot(file.exists(releases_md), dir.exists(dir_releases))
  rl <- readLines(releases_md, warn = FALSE)
  if (is.null(release_notes_section(rl, version)))
    stop("RELEASES.md has no section for ", version, call. = FALSE)
  vdir <- file.path(dir_releases, version)
  if (!dir.exists(vdir)) stop("no local sidecar dir for ", version, ": ", vdir, call. = FALSE)
  rd <- function(f) { p <- file.path(vdir, f)
    if (file.exists(p)) jsonlite::fromJSON(p, simplifyVector = TRUE) else NULL }
  promoted <- NA
  if (!is.null(bucket)) promoted <- tryCatch(
    identical(read_promoted_release(bucket = bucket), version), error = function(e) NA)
  md <- render_release_notes(
    version, rl,
    catalog = rd("catalog.json"), metadata = rd("metadata.json"),
    test_results = rd("test_results.json"), pkg_versions = pkg_versions,
    promoted = promoted)
  out <- file.path(vdir, "RELEASE_NOTES.md")
  writeLines(md, out)
  if (!is.null(bucket)) {
    gcloud <- find_gcloud()
    dst <- sprintf("gs://%s/ducklake/releases/%s/RELEASE_NOTES.md", bucket, version)
    put_gcs_file(out, dst)
    put_gcs_file(releases_md, sprintf("gs://%s/ducklake/releases/RELEASES.md", bucket))
    system2(gcloud, c("storage", "objects", "update", "--cache-control=no-cache",
                      dst, sprintf("gs://%s/ducklake/releases/RELEASES.md", bucket)),
            stdout = FALSE, stderr = FALSE)
    message("published release notes for ", version, " -> ", dst)
  }
  invisible(out)
}
