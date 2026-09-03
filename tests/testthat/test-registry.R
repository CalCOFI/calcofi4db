# The shared metadata registries are hand-edited AND written back by ingest
# notebooks. write_csv() defaults to na = "NA", so an empty cell round-trips to the
# 2-character string "NA" — invisible from R (read_csv reads it back to NA) but NOT
# from DuckDB's read_csv_auto, which shipped 161 such rows into the released
# measurement_type table. These pin both halves: writes emit empty cells, and reads
# refuse a corrupted registry.

mt_fixture <- function(env = parent.frame()) {
  d <- tibble::tibble(
    measurement_type  = c("abundance", "body_length"),
    description       = c("Specimen count", "Larva body length"),
    units             = c("count", "mm"),
    is_canonical      = c(TRUE, TRUE),
    `_qual_column`    = c(NA_character_, NA_character_),
    grain             = c("obs", "attribute"))
  path <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  readr::write_csv(d, path, na = "")
  path
}

test_that("check_registry_na_strings() passes a clean registry and names the culprit", {
  clean <- tibble::tibble(a = c("x", NA_character_), b = c(NA_character_, "y"))
  expect_silent(check_registry_na_strings(clean))
  expect_identical(check_registry_na_strings(clean), clean)

  dirty <- tibble::tibble(a = c("x", "NA"), b = c("NULL", "y"))
  expect_error(check_registry_na_strings(dirty, path = "reg.csv"),
               "sentinel strings")
  # the error must say WHERE, so it is actionable
  expect_error(check_registry_na_strings(dirty, path = "reg.csv"), "reg\\.csv")
  expect_error(check_registry_na_strings(dirty), "\\ba\\b")
  expect_error(check_registry_na_strings(dirty), "\\bb\\b")
  # and it must point at the cause
  expect_error(check_registry_na_strings(dirty), 'na = \\\\?""')

  # non-character columns are not candidates, and " NA " still counts
  expect_silent(check_registry_na_strings(tibble::tibble(n = c(1, 2))))
  expect_error(check_registry_na_strings(tibble::tibble(a = " NA ")), "sentinel")
})

test_that("read_measurement_type() errors on the write_csv(na='NA') round trip", {
  path <- mt_fixture()
  expect_s3_class(read_measurement_type(path), "data.frame")

  # reproduce the exact corruption: rewrite with readr's DEFAULT na
  readr::write_csv(readr::read_csv(path, show_col_types = FALSE), path)
  expect_error(read_measurement_type(path), "sentinel strings")
  # and confirm this is invisible to a plain read — which is why the guard exists
  expect_true(all(is.na(
    readr::read_csv(path, show_col_types = FALSE)$`_qual_column`)))
  # escape hatch for inspecting a known-broken file
  expect_s3_class(read_measurement_type(path, validate = FALSE), "data.frame")
})

test_that("register_measurement_types() writes empty cells, not 'NA'", {
  path <- mt_fixture()
  new <- tibble::tibble(
    measurement_type = "carapace_length", description = "Carapace length",
    units = "mm", is_canonical = TRUE, `_qual_column` = NA_character_,
    grain = "attribute")

  out <- register_measurement_types(new, path, quiet = TRUE)
  expect_true("carapace_length" %in% out$measurement_type)
  expect_equal(nrow(out), 3)

  # the file on disk must be readable by a NULL-strict reader: no literal "NA"
  raw <- readLines(path)
  expect_false(any(grepl("(^|,)NA(,|$)", raw)))
  expect_silent(read_measurement_type(path))
})

test_that("register_measurement_types() is idempotent and does not duplicate", {
  path <- mt_fixture()
  before <- readLines(path)

  # an existing type is not re-added, and the file is left untouched
  same <- tibble::tibble(measurement_type = "abundance", description = "changed")
  out <- register_measurement_types(same, path, quiet = TRUE)
  expect_equal(nrow(out), 2)
  expect_identical(readLines(path), before)
  # existing rows are never overwritten by a same-named candidate
  expect_equal(out$description[out$measurement_type == "abundance"],
               "Specimen count")

  # duplicates within new_types collapse to one row
  dup <- tibble::tibble(measurement_type = c("zoea_other", "zoea_other"),
                        description = c("first", "second"))
  out2 <- register_measurement_types(dup, path, quiet = TRUE)
  expect_equal(sum(out2$measurement_type == "zoea_other"), 1)

  # NULL / zero-row input is a no-op returning the registry
  expect_equal(nrow(register_measurement_types(NULL, path, quiet = TRUE)), 3)
  expect_equal(nrow(register_measurement_types(dup[0, ], path, quiet = TRUE)), 3)
})

test_that("register_measurement_types() will not silently widen the registry", {
  path <- mt_fixture()
  new <- tibble::tibble(measurement_type = "settled_volume_ml",
                        description = "Settled volume", bogus_col = "x")
  expect_warning(out <- register_measurement_types(new, path, quiet = TRUE),
                 "bogus_col")
  expect_false("bogus_col" %in% names(out))
})

test_that("a corrupted registry is visible to DuckDB but not to readr", {
  skip_if_not_installed("duckdb")
  path <- mt_fixture()
  readr::write_csv(readr::read_csv(path, show_col_types = FALSE), path)  # na = "NA"

  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con), add = TRUE)
  n <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM read_csv_auto('%s') WHERE \"_qual_column\" = 'NA'",
    path))$n
  # this is the bug: DuckDB sees literal 'NA', readr sees NA
  expect_gt(n, 0)
})

# register_measurement_types() only appends, so it could not put a bound on a type
# that already existed without one — which was all 73 unbounded types at
# v2026.08.07. declare_measurement_bounds() is the narrow counterpart: bound
# columns only, existing rows only, and an unknown type is an error rather than a
# silent insert that no observation would ever match.

test_that("declare_measurement_bounds() sets bounds on existing rows only", {
  path <- mt_fixture()
  d <- declare_measurement_bounds(
    data.frame(measurement_type = "abundance", valid_min = 0),
    path, quiet = TRUE)

  expect_equal(d$valid_min[d$measurement_type == "abundance"], 0)
  # one-sided: the undeclared side stays empty, not 0
  expect_true(is.na(d$valid_max[d$measurement_type == "abundance"]))
  # the other type is untouched
  expect_true(is.na(d$valid_min[d$measurement_type == "body_length"]))
  # and nothing else about the row moved
  expect_identical(d$units[d$measurement_type == "abundance"], "count")
  expect_identical(nrow(d), 2L)

  # round trip through the strict reader: an empty cell must not become "NA"
  expect_silent(read_measurement_type(path))
})

test_that("declare_measurement_bounds() refuses an unregistered type", {
  path <- mt_fixture()
  before <- readLines(path)
  expect_error(
    declare_measurement_bounds(
      data.frame(measurement_type = "abundnace", valid_min = 0), path),
    "not in the registry")
  expect_error(
    declare_measurement_bounds(
      data.frame(measurement_type = "abundnace", valid_min = 0), path),
    "register_measurement_types")
  # the file is untouched by a rejected call — byte-for-byte, since a partial
  # write is exactly what a half-validated update would leave behind
  expect_identical(readLines(path), before)
})

test_that("declare_measurement_bounds() will not silently move an agreed bound", {
  path <- mt_fixture()
  declare_measurement_bounds(
    data.frame(measurement_type = "abundance", valid_min = 0, valid_max = 100),
    path, quiet = TRUE)

  expect_error(
    declare_measurement_bounds(
      data.frame(measurement_type = "abundance", valid_max = 50), path),
    "already-declared")
  # re-declaring the SAME value is a no-op, not an error — re-running an ingest
  # must stay idempotent
  expect_message(
    declare_measurement_bounds(
      data.frame(measurement_type = "abundance", valid_min = 0), path),
    "unchanged")
  # explicit overwrite is allowed
  d <- declare_measurement_bounds(
    data.frame(measurement_type = "abundance", valid_max = 50),
    path, overwrite = TRUE, quiet = TRUE)
  expect_equal(d$valid_max[d$measurement_type == "abundance"], 50)
})

test_that("declare_measurement_bounds() rejects duplicates and empty input", {
  path <- mt_fixture()
  expect_error(
    declare_measurement_bounds(
      data.frame(measurement_type = c("abundance", "abundance"),
                 valid_min = c(0, 1)), path),
    "duplicate")
  expect_error(
    declare_measurement_bounds(
      data.frame(measurement_type = "abundance", nope = 1), path),
    "none of valid_min")
  expect_identical(nrow(declare_measurement_bounds(NULL, path)), 2L)
})

# Ten ingests replace their registry row with a freshly-built literal. Every
# curated column that literal omits was destroyed on each re-run — which is how
# provider-agreed bounds vanished from euphausiid_abundance and the picoplankton
# types mid-release. upsert carries them forward.

test_that("upsert_measurement_types() preserves curated bounds through a rewrite", {
  d <- readr::read_csv(mt_fixture(), na = "", show_col_types = FALSE)
  d$valid_min <- c(0, NA); d$valid_max <- c(100, NA)

  # exactly what an ingest builds: a definition literal with no bound columns
  lit <- tibble::tibble(
    measurement_type = "abundance", description = "Specimen count (revised)",
    units = "count", is_canonical = TRUE, grain = "obs")

  out <- upsert_measurement_types(d, lit)
  expect_identical(nrow(out), 2L)
  # the definition IS updated ...
  expect_identical(out$description[out$measurement_type == "abundance"],
                   "Specimen count (revised)")
  # ... and the curated bound survives
  expect_equal(out$valid_min[out$measurement_type == "abundance"], 0)
  expect_equal(out$valid_max[out$measurement_type == "abundance"], 100)
  # the untouched row is untouched
  expect_true(is.na(out$valid_min[out$measurement_type == "body_length"]))

  # the naive pattern this replaces loses the bound — pin the contrast
  naive <- dplyr::bind_rows(d[d$measurement_type != "abundance", ], lit)
  expect_true(is.na(naive$valid_min[naive$measurement_type == "abundance"]))
})

test_that("upsert_measurement_types() lets an explicit value win, and adds new types", {
  d <- readr::read_csv(mt_fixture(), na = "", show_col_types = FALSE)
  d$valid_min <- c(0, NA); d$valid_max <- c(100, NA)

  # an ingest that DOES author a bound overrides the stored one
  out <- upsert_measurement_types(
    d, tibble::tibble(measurement_type = "abundance", valid_max = 50))
  expect_equal(out$valid_max[out$measurement_type == "abundance"], 50)

  # a type not yet in the registry is added, not dropped
  out <- upsert_measurement_types(
    d, tibble::tibble(measurement_type = "biomass", units = "mg"))
  expect_identical(nrow(out), 3L)
  expect_true("biomass" %in% out$measurement_type)

  expect_error(upsert_measurement_types(
    d, tibble::tibble(measurement_type = c("a","a"))), "duplicate")
  expect_identical(nrow(upsert_measurement_types(d, NULL)), 2L)
})

# ---- declare_measurement_fields(): the Browse tab's category + the cross-dataset variable ----
test_that("declare_measurement_fields() sets category / variable on existing rows only, checked against the category registry", {
  p <- withr::local_tempfile(fileext = ".csv")
  readr::write_csv(data.frame(measurement_type = c("temperature", "temperature_ave", "nitrate"), units = c("degC", "degC", "umol/L")), p, na = "")
  cats <- c("Physical Oceanography", "Nutrients & Chemistry")
  d <- declare_measurement_fields(data.frame(measurement_type = c("temperature", "temperature_ave"), category = "Physical Oceanography", variable = "temperature"), p, categories = cats, quiet = TRUE)
  expect_true(all(c("category", "variable") %in% names(d)))              # a registry predating the columns gains them
  expect_equal(d$variable[d$measurement_type %in% c("temperature", "temperature_ave")], c("temperature", "temperature"))
  expect_true(is.na(d$category[d$measurement_type == "nitrate"]))       # untouched rows stay empty — and empty, not "NA"
  expect_false(any(grepl("^NA$", readLines(p))))
  expect_error(declare_measurement_fields(data.frame(measurement_type = "salinity", category = "Physical Oceanography"), p, cats), "not in the registry")
  expect_error(declare_measurement_fields(data.frame(measurement_type = "nitrate", category = "Nutrients and Chemistry"), p, cats), "category not in the registry")
  expect_error(declare_measurement_fields(data.frame(measurement_type = "temperature", variable = "temp"), p, cats), "already-declared")
  d2 <- declare_measurement_fields(data.frame(measurement_type = "temperature", variable = "temp"), p, cats, overwrite = TRUE, quiet = TRUE)
  expect_equal(d2$variable[d2$measurement_type == "temperature"], "temp")
  expect_error(declare_measurement_fields(data.frame(measurement_type = "nitrate", units = "x"), p, cats), "none of")
})

# ---- declare_measurement_fields(): derivation / is_canonical (WS-G, 2026-09-03 — the bottle r_* types) ----
test_that("declare_measurement_fields() also sets derivation / is_canonical on existing rows only", {
  p <- withr::local_tempfile(fileext = ".csv")
  # no derivation/is_canonical column yet — a registry predating them, like measurement_type.csv today
  readr::write_csv(data.frame(measurement_type = c("r_temperature", "temperature")), p, na = "")
  d <- declare_measurement_fields(
    data.frame(measurement_type = "r_temperature",
               derivation = "reported value interpolated to standard depth (pre-QC, decodr); carries no quality code by design; not an input for further interpolation",
               is_canonical = "FALSE"),
    p, quiet = TRUE)
  expect_true(all(c("derivation", "is_canonical") %in% names(d)))         # a registry predating the columns gains them
  # is_canonical round-trips through read_measurement_type() as LOGICAL (readr
  # type-guesses "TRUE"/"FALSE" text on re-read, same as the real registry) —
  # declare_measurement_fields() compares/writes via as.character() internally,
  # so this is a property of the reader, not something the function must resist.
  expect_identical(d$is_canonical[d$measurement_type == "r_temperature"], FALSE)
  expect_match(d$derivation[d$measurement_type == "r_temperature"], "^reported value interpolated")
  expect_true(is.na(d$is_canonical[d$measurement_type == "temperature"]))  # untouched row stays empty
  expect_false(any(grepl("^NA$", readLines(p))))                          # na = "" round trip, not the literal string "NA"

  expect_error(declare_measurement_fields(data.frame(measurement_type = "salinity", derivation = "x"), p), "not in the registry")
  expect_error(declare_measurement_fields(data.frame(measurement_type = "r_temperature", is_canonical = "TRUE"), p), "already-declared")
  d2 <- declare_measurement_fields(data.frame(measurement_type = "r_temperature", is_canonical = "TRUE"), p, overwrite = TRUE, quiet = TRUE)
  expect_identical(d2$is_canonical[d2$measurement_type == "r_temperature"], TRUE)
})

# ---- declare_measurement_fields(): the NERC vocabulary ids (WS-H2, 2026-09-03 — decision D-S2) ----
test_that("declare_measurement_fields() sets nerc_p01 / units_nerc_p06 and rejects a wrong-collection URI", {
  p <- withr::local_tempfile(fileext = ".csv")
  # a registry predating both columns, like measurement_type.csv before this change
  readr::write_csv(data.frame(measurement_type = c("temperature", "nitrate"),
                              units = c("degC", "umol/L")), p, na = "")
  p01 <- "http://vocab.nerc.ac.uk/collection/P01/current/TEMPPR01/"
  p06 <- "http://vocab.nerc.ac.uk/collection/P06/current/UPAA/"
  d <- declare_measurement_fields(
    data.frame(measurement_type = "temperature", nerc_p01 = p01, units_nerc_p06 = p06),
    p, quiet = TRUE)
  expect_true(all(c("nerc_p01", "units_nerc_p06") %in% names(d)))
  expect_identical(d$nerc_p01[d$measurement_type == "temperature"], p01)
  expect_identical(d$units_nerc_p06[d$measurement_type == "temperature"], p06)
  # a type with no exact concept stays EMPTY — and empty, not the string "NA"
  expect_true(is.na(d$nerc_p01[d$measurement_type == "nitrate"]))
  expect_false(any(grepl("^NA$", readLines(p))))

  # the point of the prefix check: a P06 unit URI in the P01 column is a
  # plausible-looking string that would otherwise reach an OBIS eMoF export intact
  expect_error(declare_measurement_fields(
    data.frame(measurement_type = "nitrate", nerc_p01 = p06), p), "must be a full NERC concept URI")
  # a bare concept code is not a URI
  expect_error(declare_measurement_fields(
    data.frame(measurement_type = "nitrate", nerc_p01 = "NTRAZZXX"), p), "must be a full NERC concept URI")
  # ...nor is a URI missing its trailing slash
  expect_error(declare_measurement_fields(
    data.frame(measurement_type = "nitrate",
               nerc_p01 = "http://vocab.nerc.ac.uk/collection/P01/current/NTRAZZXX"), p),
    "must be a full NERC concept URI")
  expect_error(declare_measurement_fields(
    data.frame(measurement_type = "nitrate", units_nerc_p06 = p01), p), "must be a full NERC concept URI")

  # same overwrite discipline as the other declarable fields
  expect_error(declare_measurement_fields(
    data.frame(measurement_type = "temperature",
               nerc_p01 = "http://vocab.nerc.ac.uk/collection/P01/current/TEMPST01/"), p),
    "already-declared")
  d2 <- declare_measurement_fields(
    data.frame(measurement_type = "temperature",
               nerc_p01 = "http://vocab.nerc.ac.uk/collection/P01/current/TEMPST01/"),
    p, overwrite = TRUE, quiet = TRUE)
  expect_identical(d2$nerc_p01[d2$measurement_type == "temperature"],
                   "http://vocab.nerc.ac.uk/collection/P01/current/TEMPST01/")
})
