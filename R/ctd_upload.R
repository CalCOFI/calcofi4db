# CTD upload: shipboard files -> the core model ---------------------------------
#
# THE DESIGN PRINCIPLE THAT MAKES THIS CHEAP: every QC rule targets `obs` /
# `sample`. Project an uploaded file into that shape and the whole registry runs
# against it unchanged — no rule needs to know where the data came from. So the
# work here is parsing and mapping, not checking.
#
# Formats, and what each one costs to support:
#
#   .csv   the CalCOFI CTD cast file. Column names map through the existing
#          `metadata/measurement_type.csv` `_source_column`, which already holds
#          exactly these names.
#   .cnv   Sea-Bird converted data. The header carries `# name N = short: long`,
#          so the column vocabulary is UNAMBIGUOUS. Prefer this format.
#   .asc   Sea-Bird ASCII export. Same vocabulary, but the names are laid out
#          fixed-width and ADJACENT NAMES RUN TOGETHER — measured across the
#          CalCOFI archive, 179 of 200 files have a header that cannot be split on
#          whitespace (`Sbeox0ML/LSbeox0Mm/Kg`). See [sbe_split_header()].
#   .btl   Sea-Bird bottle summary: several statistic rows per bottle, tagged
#          `(avg)` / `(sdev)` / `(min)` / `(max)`.
#   .hex   raw instrument frames. NOT SUPPORTED, and this is not laziness: the
#          hex is A/D counts, and converting it needs the instrument
#          configuration file (.xmlcon / .con) with the calibration coefficients.
#          Without it any "conversion" would be invented numbers.
#
# The SBE short names are crosswalked to measurement types by
# `metadata/sbe_name_map.csv` — a reviewable registry, not a lookup buried in
# code, because it encodes judgements (raw voltages are deliberately unmapped;
# `FlECO-AFL` is converted chlorophyll fluorescence while the registry carries the
# fluorometer VOLTAGE, so mapping it would silently change units).

# -- reading -------------------------------------------------------------------

# SeaSave writes theta as a non-UTF8 byte, so every read goes through latin1
.sbe_lines <- function(path, n = -1L) {
  iconv(readLines(path, n = n, warn = FALSE, encoding = "latin1"),
        "latin1", "UTF-8", sub = "?")
}

.field_extents <- function(line) {
  m <- gregexpr("\\S+", line)[[1]]
  list(start = as.integer(m),
       stop  = as.integer(m) + attr(m, "match.length") - 1L)
}

#' Recover the column names from a fixed-width Sea-Bird ASCII header
#'
#' The `.asc` header is fixed-width, and in most CalCOFI files adjacent names
#' touch: `Sbeox0ML/L` and `Sbeox0Mm/Kg` arrive as `Sbeox0ML/LSbeox0Mm/Kg`.
#' Splitting on whitespace therefore produces the wrong number of columns and
#' silently mis-assigns every column after the collision — which in a QC tool is
#' worse than not reading the file at all.
#'
#' Both the numbers and the header names are RIGHT-ALIGNED in their column, so the
#' aligned edge is each field's stop position. Those are taken from the data rows
#' (where fields are always separated) and the header is sliced at them.
#'
#' It ERRORS rather than guessing when the result is not self-consistent — an
#' empty name, a name containing a space, or a count that does not match the data.
#' Measured on the CalCOFI archive this reads ~86% of `.asc` files cleanly; the
#' rest report the problem and ask for the `.cnv`, whose header is unambiguous.
#'
#' @param header the header line
#' @param data_rows a few data lines to take column edges from
#'
#' @return character vector of column names
#' @export
#' @concept upload
sbe_split_header <- function(header, data_rows) {
  ex <- lapply(data_rows, .field_extents)
  n  <- max(vapply(ex, function(e) length(e$start), 1L))
  ex <- Filter(function(e) length(e$start) == n, ex)
  if (!length(ex))
    stop("could not find a consistent column layout in the data rows", call. = FALSE)

  sp <- do.call(pmax, lapply(ex, `[[`, "stop"))
  sp[n] <- max(sp[n], nchar(header))
  cuts  <- c(0L, sp)
  nm <- vapply(seq_len(n), function(i)
    trimws(substr(header, cuts[i] + 1L, cuts[i + 1L])), character(1))

  if (any(!nzchar(nm)) || any(grepl("\\s", nm))) {
    stop("the fixed-width header could not be split unambiguously",
         " — supply the .cnv for this cast, whose header names each column",
         " explicitly.\n  recovered: ", paste(nm, collapse = " | "), call. = FALSE)
  }
  nm
}

#' Parse a Sea-Bird `*` / `**` header block
#'
#' @param lines lines of the file (or of a `.hdr`)
#'
#' @return named list with any of `ship`, `cruise`, `station`, `cast`, `latitude`,
#'   `longitude`, `datetime`, `file_name`; absent keys are simply not present
#' @export
#' @concept upload
read_sbe_header <- function(lines) {
  h <- list()
  grab <- function(re) {
    m <- regmatches(lines, regexec(re, lines, ignore.case = TRUE))
    v <- vapply(m, function(x) if (length(x) > 1) trimws(x[2]) else NA_character_,
                character(1))
    v <- v[!is.na(v) & nzchar(v)]
    if (length(v)) v[1] else NULL
  }
  h$ship      <- grab("^\\*+\\s*Ship\\s*:\\s*(.+?)\\s*$")
  h$cruise    <- grab("^\\*+\\s*Cruise\\s*:\\s*(.+?)\\s*$")
  h$station   <- grab("^\\*+\\s*Station\\s*:\\s*(.+?)\\s*$")
  h$cast      <- grab("^\\*+\\s*Cast\\s*:\\s*(.+?)\\s*$")
  h$file_name <- grab("^\\*\\s*FileName\\s*=\\s*(.+?)\\s*$")

  # NMEA position is degrees + decimal minutes with a hemisphere letter
  dm <- function(s) {
    if (is.null(s)) return(NULL)
    p <- regmatches(s, regexec("([0-9.]+)\\s+([0-9.]+)\\s*([NSEW])", s))[[1]]
    if (length(p) < 4) return(NULL)
    v <- as.numeric(p[2]) + as.numeric(p[3]) / 60
    if (toupper(p[4]) %in% c("S", "W")) v <- -v
    v
  }
  h$latitude  <- dm(grab("^\\*\\s*NMEA Latitude\\s*=\\s*(.+?)\\s*$"))
  h$longitude <- dm(grab("^\\*\\s*NMEA Longitude\\s*=\\s*(.+?)\\s*$"))

  tstr <- grab("^\\*\\s*NMEA UTC \\(Time\\)\\s*=\\s*(.+?)\\s*$")
  if (is.null(tstr)) tstr <- grab("^\\*\\s*System UpLoad Time\\s*=\\s*(.+?)\\s*$")
  if (!is.null(tstr)) {
    tt <- as.POSIXct(gsub("\\s+", " ", tstr), tz = "UTC",
                     tryFormats = c("%b %d %Y %H:%M:%S", "%b %d %Y %H:%M"))
    if (!is.na(tt)) h$datetime <- tt
  }
  h[!vapply(h, is.null, logical(1))]
}

.sbe_data_frame <- function(nm, data_lines) {
  # tab-delimited when the caller has already assembled fields (see .btl)
  sep   <- if (any(grepl("\t", data_lines))) "\t" else "\\s+"
  parts <- strsplit(trimws(data_lines), sep)
  keep  <- lengths(parts) == length(nm)
  if (!any(keep))
    stop("no data row has ", length(nm), " fields", call. = FALSE)
  m <- do.call(rbind, parts[keep])
  d <- as.data.frame(m, stringsAsFactors = FALSE)
  names(d) <- nm
  # numeric where possible; date/time columns stay character
  for (j in names(d)) {
    v <- suppressWarnings(as.numeric(d[[j]]))
    if (!all(is.na(v))) d[[j]] <- v
  }
  tibble::as_tibble(d)
}

#' Read a Sea-Bird `.asc` ASCII export
#' @param path file path
#' @return a tibble with a `sbe_header` attribute
#' @export
#' @concept upload
read_sbe_asc <- function(path) {
  l <- .sbe_lines(path)
  l <- l[nzchar(trimws(l))]
  if (length(l) < 2) stop("empty .asc file: ", basename(path), call. = FALSE)
  nm <- sbe_split_header(l[1], utils::head(l[-1], 10))
  d  <- .sbe_data_frame(nm, l[-1])
  attr(d, "sbe_header") <- .companion_header(path)
  d
}

# the `.hdr` beside an `.asc` carries the instrument/station block
.companion_header <- function(path) {
  hdr <- sub("\\.[^.]+$", ".hdr", path)
  if (file.exists(hdr)) read_sbe_header(.sbe_lines(hdr)) else list()
}

#' Read a Sea-Bird `.cnv` converted data file
#'
#' The `# name N = short: long [units]` header names every column explicitly, so
#' unlike `.asc` there is nothing to infer. `# bad_flag` (conventionally
#' `-9.990e-29`) becomes `NA` — the same pseudo-NA the CalCOFI ingest already
#' strips from coordinates.
#'
#' @param path file path
#' @return a tibble with `sbe_header` and `sbe_units` attributes
#' @export
#' @concept upload
read_sbe_cnv <- function(path) {
  l <- .sbe_lines(path)
  i_end <- grep("^\\*END\\*", l)
  if (!length(i_end)) stop("no *END* marker — is this a .cnv?", call. = FALSE)
  head_l <- l[seq_len(i_end[1])]
  data_l <- l[-seq_len(i_end[1])]
  data_l <- data_l[nzchar(trimws(data_l))]

  nm_l <- grep("^#\\s*name\\s+\\d+\\s*=", head_l, value = TRUE)
  if (!length(nm_l)) stop("no `# name N =` lines — is this a .cnv?", call. = FALSE)
  m  <- regmatches(nm_l, regexec("^#\\s*name\\s+(\\d+)\\s*=\\s*([^:]+):\\s*(.*)$", nm_l))
  nm <- vapply(m, function(x) trimws(x[3]), character(1))
  lg <- vapply(m, function(x) trimws(x[4]), character(1))

  d <- .sbe_data_frame(nm, data_l)

  bf <- grep("^#\\s*bad_flag\\s*=", head_l, value = TRUE)
  if (length(bf)) {
    v <- suppressWarnings(as.numeric(trimws(sub("^#\\s*bad_flag\\s*=", "", bf[1]))))
    if (!is.na(v)) for (j in names(d))
      if (is.numeric(d[[j]])) d[[j]][d[[j]] == v] <- NA_real_
  }
  attr(d, "sbe_header") <- read_sbe_header(head_l)
  attr(d, "sbe_units")  <- stats::setNames(lg, nm)
  d
}

#' Read a Sea-Bird `.btl` bottle summary
#'
#' Each bottle contributes several rows tagged `(avg)` / `(sdev)` / `(min)` /
#' `(max)`. Only `(avg)` is read, for two reasons: the others describe the scatter
#' of the scans the bottle was fired over rather than separate observations, and
#' structurally they are not the same table — the `(sdev)` row omits the bottle
#' number and carries a time where the `(avg)` row carries a date, so it does not
#' share the column layout.
#'
#' @param path file path
#' @param statistic tag to keep; `"avg"` is the only complete layout
#' @return a tibble with a `sbe_header` attribute
#' @export
#' @concept upload
read_sbe_btl <- function(path, statistic = "avg") {
  l <- .sbe_lines(path)
  i_hdr <- grep("^\\s*Bottle\\s", l)[1]
  if (is.na(i_hdr)) stop("no `Bottle` column header — is this a .btl?", call. = FALSE)

  head_l <- l[seq_len(i_hdr - 1)]
  body   <- l[(i_hdr + 2):length(l)]
  body   <- body[nzchar(trimws(body))]

  tag <- sub(".*\\(([a-z]+)\\)\\s*$", "\\1", body)
  tag[!grepl("\\([a-z]+\\)\\s*$", body)] <- NA_character_
  keep <- which(tag %in% statistic)
  if (!length(keep))
    stop("no `(", paste(statistic, collapse = "|"), ")` rows found", call. = FALSE)
  rows <- sub("\\s*\\([a-z]+\\)\\s*$", "", body[keep])

  # .btl is NOT read fixed-width. Its first two columns break the assumption the
  # .asc reader relies on: `Bottle` is left-aligned over a right-aligned number,
  # and the single header word `Date` sits over three data fields (`Nov 09 2022`).
  # So names come from the header words, and the date is re-joined by position.
  nm <- strsplit(trimws(l[i_hdr]), "\\s+")[[1]]
  i_date <- match("Date", nm)
  parts  <- strsplit(trimws(rows), "\\s+")
  extra  <- if (is.na(i_date)) 0L else 2L   # `Mon DD YYYY` is 3 fields, 1 name
  ok     <- lengths(parts) == length(nm) + extra
  if (!any(ok)) {
    stop("the .btl header does not line up with its data (", length(nm),
         " names, ", stats::median(lengths(parts)), " fields per row). This is ",
         "usually adjacent header names running together, e.g. ",
         "`Sbeox0PSSbeox0Mm/Kg` — supply the .cnv for this cast, whose header ",
         "names each column explicitly.", call. = FALSE)
  }
  parts <- parts[ok]
  if (!is.na(i_date)) parts <- lapply(parts, function(p) c(
    p[seq_len(i_date - 1)],
    paste(p[i_date:(i_date + 2)], collapse = " "),
    p[-seq_len(i_date + 2)]))

  d <- .sbe_data_frame(nm, vapply(parts, paste, character(1), collapse = "\t"))
  attr(d, "sbe_header") <- read_sbe_header(head_l)
  d
}

#' Read any supported CTD upload
#'
#' @param path file path; the extension selects the reader
#' @return a tibble, with `sbe_header` attached for Sea-Bird formats
#' @export
#' @concept upload
read_ctd_upload <- function(path) {
  ext <- tolower(tools::file_ext(path))
  switch(
    ext,
    csv = readr::read_csv(path, show_col_types = FALSE, guess_max = 10000) |>
            janitor::clean_names(),
    cnv = read_sbe_cnv(path),
    asc = read_sbe_asc(path),
    btl = read_sbe_btl(path),
    hex = stop(
      ".hex is raw instrument output — A/D counts, not engineering units. ",
      "Converting it requires the instrument configuration file (.xmlcon or ",
      ".con) holding the sensor calibration coefficients, which is not part of ",
      "the upload. Run SBE Data Processing (or oce::read.ctd.sbe with the .con) ",
      "and upload the resulting .cnv.", call. = FALSE),
    stop("unsupported upload format: .", ext,
         " — expected .csv, .cnv, .asc or .btl", call. = FALSE))
}

# -- mapping -------------------------------------------------------------------

.norm_name <- function(x) tolower(trimws(x))

#' Map an uploaded file's columns onto measurement types
#'
#' Two vocabularies, one answer. A CalCOFI `.csv` maps through
#' `measurement_type.csv`'s `_source_column`, which already holds exactly those
#' names; a Sea-Bird file maps through `metadata/sbe_name_map.csv`.
#'
#' UNMAPPED COLUMNS ARE A RESULT, NOT AN ERROR. They are where a format change
#' announces itself — a renamed sensor, a new instrument, a column nobody has seen
#' before — so they are returned and shown rather than dropped quietly.
#'
#' @param cols column names of the uploaded data
#' @param d_meas_type the measurement registry ([read_measurement_type()])
#' @param d_sbe_map the Sea-Bird crosswalk (`metadata/sbe_name_map.csv`)
#' @param format one of `"csv"`, `"cnv"`, `"asc"`, `"btl"`
#'
#' @return a tibble of `column`, `measurement_type`, `role`, `qual_column`,
#'   `units`, `note` — one row per uploaded column, `role = "unmapped"` where
#'   nothing matched
#' @export
#' @concept upload
ctd_map_columns <- function(cols, d_meas_type, d_sbe_map = NULL,
                            format = c("csv", "cnv", "asc", "btl")) {
  format <- match.arg(format)
  out <- tibble::tibble(
    column = cols, measurement_type = NA_character_, role = "unmapped",
    qual_column = NA_character_, units = NA_character_, note = "")

  if (format == "csv") {
    reg <- d_meas_type[!is.na(d_meas_type[["_source_column"]]), ]
    i <- match(.norm_name(cols), .norm_name(reg[["_source_column"]]))
    hit <- !is.na(i)
    out$measurement_type[hit] <- reg$measurement_type[i[hit]]
    out$units[hit]            <- reg$units[i[hit]]
    out$qual_column[hit]      <- reg[["_qual_column"]][i[hit]]
    out$role[hit]             <- "measurement"
    # a column that IS a quality flag for a mapped type
    isq <- .norm_name(cols) %in% .norm_name(stats::na.omit(reg[["_qual_column"]]))
    out$role[isq & !hit] <- "quality"
  } else {
    stopifnot("a Sea-Bird upload needs metadata/sbe_name_map.csv" =
                !is.null(d_sbe_map))
    i <- match(.norm_name(cols), .norm_name(d_sbe_map$sbe_name))
    hit <- !is.na(i)
    out$measurement_type[hit] <- d_sbe_map$measurement_type[i[hit]]
    out$role[hit]             <- d_sbe_map$role[i[hit]]
    out$note[hit]             <- d_sbe_map$note[i[hit]]
    u <- match(out$measurement_type, d_meas_type$measurement_type)
    out$units <- d_meas_type$units[u]
  }
  out$note[is.na(out$note)] <- ""
  out
}

#' Project an uploaded file into the core `obs` / `sample` shape
#'
#' Once this has run, every QC rule in `metadata/qc_rules/` applies unchanged.
#'
#' The two source-specific repairs the pipeline already knows about are applied
#' here too, because a new file is exactly where they arrive: the `-99` sentinel
#' (and `-9.99e-29`) are deleted rather than carried as readings, and quality
#' codes stored as `"9.0"` by a double-to-string cast are stripped textually — not
#' via an integer cast, which would round an unexpected `"9.5"`.
#'
#' @param d the parsed upload
#' @param mapping output of [ctd_map_columns()]
#' @param header the Sea-Bird header list, if any
#' @param dataset_key stamped on every row
#' @param cruise_key,site_key,cast_id override the header-derived values
#'
#' @return list with `sample`, `obs` (both data frames) and `n_sentinel`
#' @export
#' @concept upload
ctd_upload_to_core <- function(d, mapping, header = list(),
                               dataset_key = "upload",
                               cruise_key = NULL, site_key = NULL,
                               cast_id = NULL) {
  meas <- mapping[mapping$role == "measurement" &
                    !is.na(mapping$measurement_type), ]
  if (!nrow(meas))
    stop("no column mapped to a measurement type — nothing to check", call. = FALSE)

  depth_col <- mapping$column[mapping$role == "depth"][1]
  if (is.na(depth_col)) {
    # fall back to pressure, which is what depth is derived from anyway
    pc <- meas$column[meas$measurement_type == "pressure"]
    depth_col <- if (length(pc)) pc[1] else NA_character_
  }
  if (is.na(depth_col))
    stop("no depth or pressure column — every QC rule is depth-aware", call. = FALSE)

  cruise_key <- cruise_key %||% header$cruise %||% "upload"
  site_key   <- site_key   %||% header$station %||% NA_character_
  cast_id    <- cast_id    %||% header$cast %||% "001"
  sample_key <- paste0(dataset_key, ":cast:", cruise_key, "_", cast_id)

  lat <- .col_or(d, mapping, "position", "latitude",  header$latitude)
  lon <- .col_or(d, mapping, "position", "longitude", header$longitude)
  dtm <- header$datetime %||% as.POSIXct(NA)

  smp <- data.frame(
    sample_key = sample_key, sample_type = "cast",
    parent_sample_key = NA_character_, root_sample_key = sample_key,
    dataset_key = dataset_key, grid_key = NA_character_, site_key = site_key,
    cruise_key = cruise_key, order_occ = NA_integer_,
    latitude = lat[1], longitude = lon[1], datetime = dtm[1],
    depth_min_m = suppressWarnings(min(d[[depth_col]], na.rm = TRUE)),
    depth_max_m = suppressWarnings(max(d[[depth_col]], na.rm = TRUE)),
    tow_type = NA_character_, stringsAsFactors = FALSE)

  qual_of <- function(mt) {
    q <- meas$qual_column[meas$measurement_type == mt]
    if (!length(q) || is.na(q[1]) || !(q[1] %in% names(d))) return(NULL)
    d[[q[1]]]
  }

  obs <- do.call(rbind, lapply(seq_len(nrow(meas)), function(k) {
    cl <- meas$column[k]; mt <- meas$measurement_type[k]
    v  <- suppressWarnings(as.numeric(d[[cl]]))
    q  <- qual_of(mt)
    data.frame(
      realm = "env", dataset_key = dataset_key, sample_key = sample_key,
      grid_key = NA_character_, cruise_key = cruise_key,
      latitude = lat, longitude = lon, datetime = dtm,
      depth_min_m = suppressWarnings(as.numeric(d[[depth_col]])),
      depth_max_m = suppressWarnings(as.numeric(d[[depth_col]])),
      taxon_key = NA_character_, life_stage = NA_character_,
      measurement_type = mt, measurement_value = v,
      measurement_qual = if (is.null(q)) NA_character_ else .clean_qual(q),
      measurement_prec = NA_real_, stringsAsFactors = FALSE)
  }))

  n0 <- nrow(obs)
  obs <- obs[!is.na(obs$measurement_value), ]
  # the source's missing markers, deleted rather than carried as readings
  obs <- obs[!(obs$measurement_value %in% c(-99)) &
               abs(obs$measurement_value - (-9.99e-29)) > 1e-30, ]
  obs$obs_id <- seq_len(nrow(obs))

  list(sample = smp, obs = obs, n_sentinel = n0 - nrow(obs))
}

# a quality code stored as "9.0" by a double->string cast must match the "9" in
# the vocabulary. Stripped textually, NOT via an integer cast, which would round
# an unexpected "9.5" into a code that means something else.
.clean_qual <- function(x) {
  x <- trimws(as.character(x))
  x[!nzchar(x)] <- NA_character_
  sub("\\.0+$", "", x)
}

.col_or <- function(d, mapping, role, want, fallback) {
  cl <- mapping$column[mapping$role == role &
                         grepl(want, mapping$column, ignore.case = TRUE)]
  if (length(cl) && cl[1] %in% names(d)) suppressWarnings(as.numeric(d[[cl[1]]]))
  else rep(fallback %||% NA_real_, nrow(d))
}

#' A connection an uploaded cast can be QC'd on
#'
#' The upload becomes `obs` and `sample` — the names every rule uses — so the
#' registry runs against it verbatim. `obs_ctd_full` is the same data: an uploaded
#' cast IS full resolution, so the profile rules apply to it directly rather than
#' skipping.
#'
#' Nothing here touches a release. The connection is in-memory and dies with the
#' session.
#'
#' @param core output of [ctd_upload_to_core()]
#' @param dir_workflows root of the workflows checkout, for [qc_stage_reference()]
#' @param gebco_tif optional bathymetry raster
#'
#' @return a DBI connection; the caller closes it
#' @export
#' @concept upload
qc_upload_con <- function(core, dir_workflows, gebco_tif = NULL) {
  con <- get_duckdb_con(":memory:")
  DBI::dbWriteTable(con, "sample", core$sample, overwrite = TRUE)
  DBI::dbWriteTable(con, "obs",    core$obs,    overwrite = TRUE)
  DBI::dbExecute(con, "CREATE OR REPLACE VIEW obs_ctd_full AS SELECT * FROM obs")
  qc_stage_reference(con, dir_workflows, gebco_tif = gebco_tif,
                     quiet = TRUE)
  con
}
