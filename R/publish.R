# publish change detection -------------------------------------------------------
#
# The publishers (`publish_to-{edi,obis,netcdf,erddap}.qmd`) run after every release,
# but most releases change a few datasets and leave the rest byte-for-byte alone. A
# publisher that rebuilds everything anyway exports gigabytes of CSV nobody needs, and
# worse, mints a "new" package for a portal whose copy is still current — so an EDI
# revision or an OBIS re-upload would say "changed" of data that did not.
#
# So each publisher fingerprints, per dataset, what its output is a function of:
#
#   data      the dataset's rows in each release table it reads, identified by the
#             release's own row signatures — never by bytes, and never by version
#   metadata  the dataset's catalog record (minus what changes every release without
#             changing the dataset) plus any registry the publisher reads
#   code      the publisher's own code, so a fix to the output format rebuilds
#
# An unchanged fingerprint + outputs still on disk -> the build is reused; anything
# else rebuilds and `changed_inputs()` names why. The upload status is a second,
# separate question — built bytes vs the bytes last deposited at the portal — so a
# rebuild whose output did not change never asks for an upload.
#
# Row signatures come from the catalog wherever they can: an object hive-partitioned
# by `dataset_key` IS one dataset's rows, so its `content_hash` is that dataset's
# signature for free. A shared object (`sample`, `sample_measurement`, `obs_bio`, the
# `obs_env` partitions keyed by measurement type) holds every dataset's rows, so its
# `content_hash` moves when ANY dataset changes; those are read once, grouped by
# `dataset_key`, and the per-dataset signatures cached by the object's `content_hash`.
# Objects are content-addressed, so a cached entry is true forever.

# dataset record keys that change with a release without changing the dataset: the
# objects and their `since`, the distribution/registration rows (which report on the
# publishers themselves, so hashing them would feed a build back into its own input)
# and the curation status block
PUBLISH_RECORD_VOLATILE <- c("objects", "since_version", "distributions", "registrations", "status")

# every row of the signature cache has these columns
.PUBLISH_SIG_COLS <- c("table", "content_hash", "dataset_key", "signature")

#' Per-dataset row signatures of a release's objects
#'
#' For each object of each table in `tables` (read off `catalog.json`'s `objects[]`),
#' one row per dataset holding rows in it:
#'
#' * an object hive-partitioned by `dataset_key` — its own `content_hash`, no read;
#' * an object of a table only one dataset reads (`owners`), or with no `dataset_key`
#'   column at all (a vocabulary such as `measurement_type`) — `dataset_key = "*"`
#'   and its `content_hash`: the whole object is an input of every dataset that reads
#'   the table;
#' * any other object — read once, `GROUP BY dataset_key`, each group's row signature
#'   (the one `freeze_plan()` uses to decide `upload`/`copy`), cached in `cache` under
#'   the object's `content_hash` so an unchanged object is never read again.
#'
#' @param con a DuckDB connection with `httpfs` loaded when `base_url` is remote
#' @param catalog the parsed (`simplifyVector = FALSE`) `catalog.json`
#' @param tables table names to sign
#' @param owners optional named list, table -> the dataset_keys that read it (from
#'   `datasets.json`'s `tables[]`); a table read by exactly one dataset is never scanned
#' @param cache optional CSV path of the signature cache (created if absent)
#' @param base_url prefix joined to each object's `path` (`/`-separated)
#' @return A data frame: `table`, `content_hash`, `dataset_key` (`"*"` for a whole
#'   object), `signature`. A table absent from the catalog gives one row with
#'   `content_hash = "<missing>"`.
#' @export
#' @concept publish
#' @seealso [publish_data_parts()], [publish_fingerprint()]
publish_object_signatures <- function(con, catalog, tables, owners = NULL, cache = NULL,
                                      base_url = "https://storage.googleapis.com/calcofi-db") {
  cached <- .publish_read_sig_cache(cache)
  fresh  <- list()
  out    <- list()
  row <- function(tb, ch, dk, sig)
    data.frame(table = tb, content_hash = ch, dataset_key = dk, signature = sig,
               stringsAsFactors = FALSE)
  entries <- catalog[["tables"]] %||% list()

  for (tb in unique(as.character(tables))) {
    entry <- Find(function(t) identical(t[["name"]], tb), entries)
    if (is.null(entry)) { out[[length(out) + 1]] <- row(tb, "<missing>", "*", "<missing>"); next }
    objs <- entry[["objects"]] %||% list()
    # a pre-v2026.09 catalog has no objects[]: the table's own hash is all there is
    if (!length(objs)) {
      ch <- .s(entry[["content_hash"]])
      out[[length(out) + 1]] <- row(tb, ch, "*", ch)
      next
    }
    single_owner <- length(owners[[tb]] %||% character()) == 1
    for (o in objs) {
      ch <- .s(o[["content_hash"]])
      if (identical(.s(o[["partition_by"]]), "dataset_key")) {
        out[[length(out) + 1]] <- row(tb, ch, .s(o[["partition_value"]]), ch)
        next
      }
      if (single_owner) { out[[length(out) + 1]] <- row(tb, ch, "*", ch); next }
      hit <- cached[cached$table == tb & cached$content_hash == ch, , drop = FALSE]
      if (nrow(hit)) { out[[length(out) + 1]] <- hit; next }
      sigs <- .publish_sign_object(con, paste0(base_url, "/", .s(o[["path"]])))
      r <- if (is.null(sigs)) row(tb, ch, "*", ch) else row(tb, ch, names(sigs), unname(sigs))
      fresh[[length(fresh) + 1]] <- r
      out[[length(out) + 1]] <- r
    }
  }
  if (length(fresh) && !is.null(cache)) {
    all <- rbind(cached, do.call(rbind, fresh))
    all <- all[order(all$table, all$content_hash, all$dataset_key), , drop = FALSE]
    dir.create(dirname(cache), recursive = TRUE, showWarnings = FALSE)
    utils::write.csv(all, cache, row.names = FALSE, na = "")
  }
  res <- if (length(out)) do.call(rbind, out) else
    stats::setNames(data.frame(matrix(character(), 0, 4), stringsAsFactors = FALSE), .PUBLISH_SIG_COLS)
  rownames(res) <- NULL
  res
}

# one object -> named character dataset_key -> signature, or NULL when the object
# has no dataset_key column (a vocabulary: the whole object is everyone's input)
.publish_sign_object <- function(con, url) {
  cols <- DBI::dbGetQuery(con, glue::glue("DESCRIBE SELECT * FROM read_parquet('{url}')"))$column_name
  if (!"dataset_key" %in% cols) return(NULL)
  vw <- "_publish_sig_obj"
  DBI::dbExecute(con, glue::glue("CREATE OR REPLACE VIEW {vw} AS SELECT * FROM read_parquet('{url}')"))
  on.exit(try(DBI::dbExecute(con, glue::glue("DROP VIEW IF EXISTS {vw}")), silent = TRUE), add = TRUE)
  sigs <- .partition_content_hashes(con, vw, "dataset_key")
  # GROUP BY returns groups in no fixed order; sort so a scan and the cache agree row for row
  sigs <- sigs[order(names(sigs))]
  vapply(sigs, function(s) digest::digest(s, algo = "md5", serialize = FALSE), "")
}

#' Per-dataset row signatures of tables already in a connection
#'
#' The local twin of [publish_object_signatures()], for a publisher that materializes
#' the core before building (the OBIS and netCDF publishers do, to join across tables):
#' each table's rows grouped by `dataset_key` and signed. A table with no `dataset_key`
#' column is signed per dataset through `via` — the rows a dataset actually reaches by
#' a key — so a vocabulary row another dataset added does not rebuild this one; without
#' a `via` it is signed whole (`dataset_key = "*"`). A table absent from the connection
#' contributes nothing, which [publish_data_parts()] reports as `"<absent>"`.
#'
#' @param con a DuckDB connection holding the tables
#' @param tables table (or view) names
#' @param via named list, table -> `c(from, key)`: sign `table`'s rows joined to the
#'   distinct (`dataset_key`, `key`) pairs of table `from`, e.g.
#'   `list(taxon = c("obs_bio", "taxon_key"), cruise = c("sample", "cruise_key"))`
#' @return The data frame [publish_object_signatures()] returns, `content_hash` empty.
#' @export
#' @concept publish
publish_table_signatures <- function(con, tables, via = list()) {
  have <- DBI::dbGetQuery(con, "SELECT DISTINCT table_name FROM information_schema.columns")$table_name
  row <- function(tb, dk, sig)
    data.frame(table = tb, content_hash = "", dataset_key = dk, signature = sig,
               stringsAsFactors = FALSE)
  md5 <- function(s) vapply(s, function(x) digest::digest(x, algo = "md5", serialize = FALSE), "")
  out <- list()
  for (tb in unique(as.character(tables))) {
    if (!tb %in% have) next
    cols <- DBI::dbGetQuery(con, glue::glue(
      "SELECT column_name FROM information_schema.columns WHERE table_name = '{tb}'"))$column_name
    src <- tb
    if (!"dataset_key" %in% cols) {
      v <- via[[tb]]
      if (is.null(v) || !v[1] %in% have) {
        out[[length(out) + 1]] <- row(tb, "*", md5(.table_content_hash(con, tb)))
        next
      }
      src <- paste0("_publish_via_", tb)
      DBI::dbExecute(con, glue::glue(
        'CREATE OR REPLACE VIEW "{src}" AS
         SELECT DISTINCT k.dataset_key, t.* FROM "{tb}" t
         JOIN (SELECT DISTINCT dataset_key, "{v[2]}" FROM "{v[1]}" WHERE "{v[2]}" IS NOT NULL) k
           USING ("{v[2]}")'))
      on.exit(try(DBI::dbExecute(con, glue::glue('DROP VIEW IF EXISTS "{src}"')), silent = TRUE), add = TRUE)
    }
    sigs <- .partition_content_hashes(con, src, "dataset_key")
    sigs <- sigs[order(names(sigs))]
    if (length(sigs)) out[[length(out) + 1]] <- row(tb, names(sigs), md5(unname(sigs)))
  }
  res <- if (length(out)) do.call(rbind, out) else
    stats::setNames(data.frame(matrix(character(), 0, 4), stringsAsFactors = FALSE), .PUBLISH_SIG_COLS)
  rownames(res) <- NULL
  res
}

.publish_read_sig_cache <- function(cache) {
  empty <- stats::setNames(data.frame(matrix(character(), 0, 4), stringsAsFactors = FALSE),
                           .PUBLISH_SIG_COLS)
  if (is.null(cache) || !file.exists(cache)) return(empty)
  d <- tryCatch(utils::read.csv(cache, colClasses = "character", na.strings = character()),
                error = function(e) empty)
  if (!all(.PUBLISH_SIG_COLS %in% names(d))) return(empty)
  d[, .PUBLISH_SIG_COLS, drop = FALSE]
}

#' One dataset's data inputs, table by table
#'
#' @param signatures the data frame from [publish_object_signatures()]
#' @param dataset_key the dataset
#' @param tables the tables this publisher reads for it
#' @return A named character vector, `data:{table}` -> a digest of the dataset's
#'   signatures in that table (`"<absent>"` when it has no rows there, `"<missing>"`
#'   when the release has no such table). Order-independent over objects.
#' @export
#' @concept publish
publish_data_parts <- function(signatures, dataset_key, tables) {
  tables <- sort(unique(as.character(tables)))
  v <- vapply(tables, function(tb) {
    s <- signatures[signatures$table == tb, , drop = FALSE]
    if (any(s$content_hash == "<missing>")) return("<missing>")
    s <- s[s$dataset_key %in% c(dataset_key, "*"), , drop = FALSE]
    if (!nrow(s)) return("<absent>")
    digest::digest(paste(sort(unique(s$signature)), collapse = "|"), algo = "md5", serialize = FALSE)
  }, character(1))
  stats::setNames(v, paste0("data:", tables))
}

#' Digest a dataset's catalog record, ignoring what changes every release
#'
#' @param record one dataset record from `datasets.json` (a list), or any list
#' @param drop top-level keys to leave out; defaults to the internal `PUBLISH_RECORD_VOLATILE`
#'   (`objects`, `since_version`, `distributions`, `registrations`, `status`)
#' @return An md5 over the record's canonical JSON (names sorted at every level), so
#'   key order never matters and a changed value always does.
#' @export
#' @concept publish
publish_record_digest <- function(record, drop = PUBLISH_RECORD_VOLATILE) {
  if (is.null(record)) return("<missing>")
  if (is.list(record) && !is.null(names(record)))
    record <- record[setdiff(names(record), drop)]
  .publish_digest(record)
}

.publish_sort_names <- function(x) {
  if (!is.list(x)) return(x)
  if (!is.null(names(x)) && all(nzchar(names(x)))) x <- x[order(names(x))]
  nm <- names(x)
  x <- lapply(x, .publish_sort_names)
  names(x) <- nm
  x
}

.publish_digest <- function(x) {
  json <- jsonlite::toJSON(.publish_sort_names(x), auto_unbox = TRUE, digits = NA,
                           null = "null", na = "null")
  digest::digest(as.character(json), algo = "md5", serialize = FALSE)
}

#' Digest a publisher's own code
#'
#' A `.qmd` contributes only its code chunks, with comment lines dropped — prose and
#' comments are the document, not the output, and a narrative edit must not rebuild
#' every package. Chunk options (`#|`) are kept: `eval:` changes what runs. Any other
#' file contributes its whole text. A missing file is `"<missing>"`, never skipped.
#'
#' @param files paths of the notebook, its `libs/` helpers and the package sources
#'   it calls
#' @return A named character vector, `code:{basename}` -> md5.
#' @export
#' @concept publish
publish_code_parts <- function(files) {
  files <- as.character(files)
  v <- vapply(files, function(f) {
    if (!file.exists(f)) return("<missing>")
    txt <- readLines(f, warn = FALSE, encoding = "UTF-8")
    if (grepl("\\.qmd$", f, ignore.case = TRUE)) txt <- .qmd_code_lines(txt)
    digest::digest(paste(txt, collapse = "\n"), algo = "md5", serialize = FALSE)
  }, character(1))
  stats::setNames(v, paste0("code:", basename(files)))
}

# the lines inside ```{r ...} chunks, minus blank and comment lines (not `#|` options)
.qmd_code_lines <- function(txt) {
  open  <- grepl("^\\s*```\\s*\\{r", txt)
  close <- grepl("^\\s*```\\s*$", txt)
  inside <- logical(length(txt)); on <- FALSE
  for (i in seq_along(txt)) {
    if (!on && open[i]) { on <- TRUE; next }
    if (on && close[i]) { on <- FALSE; next }
    inside[i] <- on
  }
  code <- sub("\\s+$", "", txt[inside])
  code[nzchar(code) & (!grepl("^\\s*#", code) | grepl("^\\s*#\\|", code))]
}

#' Fingerprint what one dataset's published output is a function of
#'
#' @param data named character from [publish_data_parts()]
#' @param metadata named list of anything else the output reads — the record digest
#'   from [publish_record_digest()], a registry table, a sidecar list; each element
#'   is digested and named `meta:{name}`
#' @param code named character from [publish_code_parts()]
#' @return A list shaped like [input_fingerprint()]'s — `hash` and named `parts` — so
#'   [changed_inputs()] reports what moved.
#' @export
#' @concept publish
publish_fingerprint <- function(data = character(), metadata = list(), code = character()) {
  meta <- if (length(metadata))
    stats::setNames(vapply(metadata, .publish_digest, ""), paste0("meta:", names(metadata))) else
    character()
  parts <- c(data, meta, code)
  parts <- parts[order(names(parts))]
  list(hash  = digest::digest(paste(names(parts), parts, sep = "=", collapse = "\n"),
                              algo = "md5", serialize = FALSE),
       parts = parts)
}

#' Reuse a dataset's previous build, or rebuild it?
#'
#' @param fp this run's fingerprint, from [publish_fingerprint()]
#' @param prior the fingerprint recorded with the previous build — a list with
#'   `hash` and `parts` (as stored in a manifest), or NULL when there is none
#' @param outputs paths the previous build must still have on disk to be reused
#' @return A list: `action` (`"reuse"` | `"build"`), `changed` (the input names that
#'   differ, via [changed_inputs()]; every input when there is no prior), and
#'   `reason` (one line for the notebook to print).
#' @export
#' @concept publish
publish_decide <- function(fp, prior = NULL, outputs = character()) {
  missing <- outputs[!file.exists(outputs)]
  if (is.null(prior) || !nzchar(.s(prior[["hash"]])))
    return(list(action = "build", changed = names(fp$parts), reason = "no previous build"))
  changed <- changed_inputs(fp, list(parts = as.list(unlist(prior[["parts"]]))))
  if (identical(.s(prior[["hash"]]), fp$hash) && !length(missing))
    return(list(action = "reuse", changed = character(), reason = "no input changed"))
  reason <- if (length(changed)) paste("changed:", paste(changed, collapse = ", ")) else
    paste("outputs missing:", paste(basename(missing), collapse = ", "))
  list(action = "build", changed = changed, reason = reason)
}

#' Does a portal need a fresh upload?
#'
#' The built bytes against the bytes last deposited: a package or archive is current
#' only while its `content_hash` is the one uploaded. Vectorised.
#'
#' @param content_hash the built output's hash (NA: nothing built)
#' @param uploaded_hash the hash recorded when it was last deposited (NA: never)
#' @param uploaded_version the release the deposited copy was built from, for the label
#' @return A data frame: `upload_status` (`not built` | `never uploaded` | `current` |
#'   `changed since {version}`) and `needs_upload` (logical).
#' @export
#' @concept publish
publish_upload_status <- function(content_hash, uploaded_hash = NA_character_,
                                  uploaded_version = NA_character_) {
  n <- length(content_hash)
  ch <- as.character(content_hash)
  uh <- rep_len(as.character(uploaded_hash), n)
  uv <- rep_len(as.character(uploaded_version), n)
  built <- !is.na(ch) & nzchar(ch)
  up    <- !is.na(uh) & nzchar(uh)
  status <- ifelse(!built, "not built",
            ifelse(!up, "never uploaded",
            ifelse(uh == ch, "current",
                   paste0("changed since ", ifelse(is.na(uv) | !nzchar(uv), "last upload", uv)))))
  data.frame(upload_status = status, needs_upload = built & (!up | uh != ch),
             stringsAsFactors = FALSE)
}
