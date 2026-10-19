# measurement-face registries (WS-MF1, 2026-09-11 plan, D1-D4 D7) --------------
# one small fixture per validator rule (root CLAUDE.md testing rules).

mchem_fixture <- function() tibble::tibble(
  key = c("oxygen_umol_kg", "salinity"), chebi_id = c("CHEBI:15379", "CHEBI:17996"),
  role = c("the_thing", "component"), mass_fraction = c(NA, 0.55),
  via = c("nerc_s27", "registry"), source = c("NVS P01 DOXMZZXX -> S27 CS002779", "TEOS-10 Manual Table D.3"),
  source_url = c("http://vocab.nerc.ac.uk/collection/P01/current/DOXMZZXX/", "https://www.teos-10.org/pubs/TEOS-10_Manual.pdf"),
  note = c(NA, "chloride"))

mmethod_fixture <- function() tibble::tibble(
  dataset_key = "calcofi_bottle", measurement_type = "oxygen_umol_kg", platform = "bottle",
  instrument = "Automated Winkler titrator, UV end point", nerc_l22 = NA_character_,
  principle = "Carpenter's (1965) modification of Winkler's method.", steps = NA_character_,
  wavelength_nm = 350, precision = NA_character_, bibkeys = "carpenter1965",
  calcofi_org_url = "https://calcofi.org/sampling-info/methods/bottle-sampling-methods/",
  text_fragment = "Dissolved Oxygen Sampling",
  source = "calcofi.org Bottle sampling methods", source_url = "https://calcofi.org/sampling-info/methods/bottle-sampling-methods/")

mscale_fixture <- function() tibble::tibble(
  key = "temperature", value = -1.91, lo = NA, hi = NA, label = "seawater (S 35) freezes",
  kind = "physical", how = "gsw.t_freezing(35, 0)", source = "TEOS-10 gsw",
  source_url = "https://www.teos-10.org/")

mface_fixture <- function() tibble::tibble(
  key = "temperature", face_kind = "scale", face_of = NA_character_,
  stands_in_note = NA_character_, source = "NERC P01 TEMPPR01 -> S06 property")

# ---- read/write round trip + na="" ----

test_that("register_measurement_chem() bootstraps, writes na='', and reads clean", {
  p <- withr::local_tempfile(fileext = ".csv")
  out <- register_measurement_chem(mchem_fixture(), p, quiet = TRUE)
  expect_equal(nrow(out), 2)
  raw <- readLines(p)
  expect_true(any(startsWith(raw, "#")))
  expect_false(any(grepl(",NA,", raw)))
  expect_s3_class(read_measurement_chem(p), "data.frame")
})

# regression: a `#` inside a field is data, not a comment. readr's comment = "#" cut every
# line at its first `#`, so a URL with a text fragment lost its tail and the fields after it
# (workflows measurement_chem.csv rows btl_chlorophyll_a / chlorophyll_a, found 2026-10-19).
test_that("a registry row whose URL carries a #fragment reads whole", {
  p <- withr::local_tempfile(fileext = ".csv")
  x <- mchem_fixture()
  x$source_url[2] <- "https://calcofi.org/sampling-info/methods/bottle-sampling-methods/#:~:text=Chlorophyll-a"
  x$note[2]       <- "what acid turns it into"
  register_measurement_chem(x, p, quiet = TRUE)
  expect_no_warning(d <- read_measurement_chem(p))
  expect_equal(d$source_url[d$key == "salinity"], x$source_url[2])
  expect_equal(d$note[d$key == "salinity"], "what acid turns it into")
})

test_that("read_variable() keeps a #fragment inside a field", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("# header comment", "variable,label,description,units,nerc_p01,category,is_unified",
               "temperature,Temperature,see https://example.org/a#b,degC,,Physical Oceanography,TRUE"), p)
  d <- read_variable(p)
  expect_equal(nrow(d), 1)
  expect_equal(d$description, "see https://example.org/a#b")
})

test_that("register_measurement_* upserts by natural key (append-or-update, not append-only)", {
  p <- withr::local_tempfile(fileext = ".csv")
  register_measurement_face(mface_fixture(), p, quiet = TRUE)
  updated <- mface_fixture()
  updated$source <- "revised source"
  out <- register_measurement_face(updated, p, quiet = TRUE)
  expect_equal(nrow(out), 1)
  expect_equal(out$source, "revised source")
})

test_that("register_measurement_method() refuses a platform outside Appendix A", {
  p <- withr::local_tempfile(fileext = ".csv")
  bad <- mmethod_fixture(); bad$platform <- "boat"
  expect_error(register_measurement_method(bad, p, quiet = TRUE), "platform")
})

test_that("register_measurement_chem() refuses a role outside Appendix A", {
  p <- withr::local_tempfile(fileext = ".csv")
  bad <- mchem_fixture(); bad$role[1] <- "flavor"
  expect_error(register_measurement_chem(bad, p, quiet = TRUE), "role")
})

test_that("register_measurement_* refuses a row without a source", {
  p <- withr::local_tempfile(fileext = ".csv")
  bad <- mface_fixture(); bad$source <- NA_character_
  expect_error(register_measurement_face(bad, p, quiet = TRUE), "source")
})

# ---- validate_measurement_faces(): one fixture per rule ----

validate_fixture_paths <- function(chem = mchem_fixture(), method = mmethod_fixture(),
                                    scale = mscale_fixture(), why = NULL, face = mface_fixture(),
                                    env = parent.frame()) {
  # local_tempfile()'s cleanup is deferred to `.local_envir` (default: its OWN
  # caller's frame); called from inside this helper that would be THIS frame,
  # deleting every fixture the instant this function returns. Pass the test's
  # own frame explicitly (same pattern as test-registry.R) so the files live
  # for the whole test_that() body.
  pchem   <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  pmethod <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  pscale  <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  pwhy    <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  pface   <- withr::local_tempfile(fileext = ".csv", .local_envir = env)
  register_measurement_chem(chem, pchem, quiet = TRUE)
  register_measurement_method(method, pmethod, quiet = TRUE)
  register_measurement_scale(scale, pscale, quiet = TRUE)
  register_measurement_why(why, pwhy, quiet = TRUE)
  register_measurement_face(face, pface, quiet = TRUE)
  list(chem = pchem, method = pmethod, scale = pscale, why = pwhy, face = pface)
}

test_that("validate_measurement_faces() is clean on well-formed fixtures", {
  p <- validate_fixture_paths()
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_equal(nrow(out), 0)
})

test_that("validate_measurement_faces() flags a row without a source (written by hand, bypassing the writer)", {
  p <- validate_fixture_paths()
  # simulate a hand-edited registry with an empty source cell, past the writer's own guard
  d <- read_measurement_face(p$face); d$source <- NA_character_
  writeLines(c("#", readr::format_csv(d, na = "")), p$face)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "missing_source" & out$registry == "measurement_face"))
})

test_that("validate_measurement_faces() flags a measurement_why key with zero rank==1 rows", {
  why <- tibble::tibble(key = "temperature", rank = 2, kind = "authored", text = "x",
                         bibkeys = NA_character_, source_url = NA_character_, eov = NA_character_, goos_doc = NA_character_)
  p <- validate_fixture_paths(why = why)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "rank1_count" & out$detail == "zero rank == 1 rows"))
})

test_that("validate_measurement_faces() flags a measurement_why key with two rank==1 rows", {
  # two rank==1 rows for one key share the (key, rank) natural key, so
  # register_measurement_why() would refuse them as a duplicate (by design —
  # that upsert path can never write this bad state); simulate the hand-edited
  # file the validator exists to catch by writing the CSV directly.
  p <- validate_fixture_paths()
  why <- tibble::tibble(key = c("temperature", "temperature"), rank = c(1, 1),
                         kind = "authored", text = c("a", "b"), bibkeys = NA_character_,
                         source_url = NA_character_, eov = NA_character_, goos_doc = NA_character_)
  writeLines(c("#", readr::format_csv(why, na = "")), p$why)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "rank1_count" & out$detail == "more than one rank == 1 row"))
})

test_that("validate_measurement_faces() flags a measurement_face key absent from measurement_type.csv", {
  p <- validate_fixture_paths()
  mtp <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("measurement_type,units", "salinity,PSS-78"), mtp)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face, measurement_type_path = mtp)
  expect_true(any(out$rule == "unknown_key" & out$key == "temperature"))
})

test_that("validate_measurement_faces() flags a nerc_l22 that is not an exact L22 concept URI", {
  bad_method <- mmethod_fixture(); bad_method$nerc_l22 <- "SBE43"
  p <- validate_fixture_paths(method = bad_method)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "bad_nerc_l22"))
})

test_that("validate_measurement_faces() accepts a well-formed nerc_l22 concept URI", {
  ok_method <- mmethod_fixture()
  ok_method$nerc_l22 <- "http://vocab.nerc.ac.uk/collection/L22/current/TOOL0374/"
  p <- validate_fixture_paths(method = ok_method)
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_false(any(out$rule == "bad_nerc_l22"))
})

# ---- short_label (issue #30 D13) ----

test_that("read_measurement_face() reads a registry without short_label as NA (older header)", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("# an older registry", "key,face_kind,face_of,stands_in_note,source",
               "temperature,scale,,,TEOS-10"), p)
  d <- expect_silent(read_measurement_face(p))
  expect_identical(names(d), c("key", "short_label", "face_kind", "face_of", "stands_in_note", "source"))
  expect_true(is.na(d$short_label))
  expect_identical(d$face_kind, "scale")
})

test_that("read_measurement_face() reads short_label when the column is present", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("#", "key,short_label,face_kind,face_of,stands_in_note,source",
               "temperature,Temperature,scale,,,TEOS-10", "salinity,,scale,,,TEOS-10"), p)
  d <- read_measurement_face(p)
  expect_identical(d$short_label, c("Temperature", NA))
})

test_that("read_measurement_face() still refuses a file missing a required column", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("#", "key,short_label,face_kind,face_of,stands_in_note", "temperature,T,scale,,"), p)
  # readr also warns that the `source` parser matches no column; the stop is what matters
  suppressWarnings(expect_error(read_measurement_face(p), "missing column"))
})

test_that("register_measurement_face() upgrades an older file to carry short_label after key", {
  p <- withr::local_tempfile(fileext = ".csv")
  writeLines(c("# old", "key,face_kind,face_of,stands_in_note,source",
               "salinity,scale,,,TEOS-10", "temperature,scale,,,TEOS-10"), p)
  d <- read_measurement_face(p)
  d$short_label[d$key == "temperature"] <- "Temperature"
  register_measurement_face(d, p, quiet = TRUE)
  raw <- readLines(p)
  expect_true(any(raw == "key,short_label,face_kind,face_of,stands_in_note,source"))
  expect_true(any(raw == "salinity,,scale,,,TEOS-10"))
  expect_true(any(raw == "temperature,Temperature,scale,,,TEOS-10"))
})

sl_face <- function(key, short_label)
  tibble::tibble(key = key, short_label = short_label, face_kind = "scale",
                 face_of = NA_character_, stands_in_note = NA_character_, source = "a source")

test_that("register_measurement_face() refuses a duplicate short_label (ignoring case)", {
  p <- withr::local_tempfile(fileext = ".csv")
  expect_error(register_measurement_face(sl_face(c("par_surf", "spar"), c("Surface PAR", "surface par")),
                                         p, quiet = TRUE), "duplicate_short_label")
  # and against a key already in the file
  register_measurement_face(sl_face("spar", "Surface PAR"), p, quiet = TRUE)
  expect_error(register_measurement_face(sl_face("par_surf", "Surface PAR"), p, quiet = TRUE),
               "duplicate_short_label")
  # re-registering the SAME key with its own label is an update, not a duplicate
  expect_no_error(register_measurement_face(sl_face("spar", "Surface PAR"), p, quiet = TRUE))
})

test_that("register_measurement_face() refuses a blank, too long or punctuated short_label", {
  p <- withr::local_tempfile(fileext = ".csv")
  expect_error(register_measurement_face(sl_face("a", "  "), p, quiet = TRUE), "empty_short_label")
  expect_error(register_measurement_face(sl_face("a", strrep("x", 49)), p, quiet = TRUE),
               "short_label_too_long")
  expect_no_error(register_measurement_face(sl_face("a", strrep("x", 48)), p, quiet = TRUE))
  expect_error(register_measurement_face(sl_face("b", "Salinity,"), p, quiet = TRUE),
               "short_label_trailing_punct")
  # a closing parenthesis is not trailing punctuation
  expect_no_error(register_measurement_face(sl_face("c", "Dissolved oxygen (ml/L)"), p, quiet = TRUE))
  # NA is allowed: no title authored yet
  expect_no_error(register_measurement_face(sl_face("d", NA_character_), p, quiet = TRUE))
})

# a hand-edited face file past the writer's guard, for the validator
write_face_by_hand <- function(p, key, short_label) {
  d <- sl_face(key, short_label)
  writeLines(c("#", readr::format_csv(d, na = "")), p)
}

test_that("validate_measurement_faces() flags a duplicate short_label", {
  p <- validate_fixture_paths()
  write_face_by_hand(p$face, c("spar", "par_surf"), c("Surface PAR", "Surface PAR"))
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  dup <- out[out$rule == "duplicate_short_label", ]
  expect_setequal(dup$key, c("spar", "par_surf"))
})

test_that("a blank short_label is flagged in memory and reads from a file as NA (absent)", {
  # the rule itself, on values a data frame can hold
  out <- calcofi4db:::.check_short_labels(c("a", "b", "c"), c("", "  ", NA))
  expect_identical(out$key, c("a", "b"))
  expect_true(all(out$rule == "empty_short_label"))
  # a CSV cannot hold a blank: readr trims it and na = "" reads it as NA, which
  # is allowed (no title authored), so the validator stays clean
  p <- validate_fixture_paths()
  writeLines(c("#", "key,short_label,face_kind,face_of,stands_in_note,source",
               'temperature,"   ",scale,,,a source'), p$face)
  expect_true(is.na(read_measurement_face(p$face)$short_label))
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_equal(nrow(out), 0)
})

test_that("validate_measurement_faces() flags a too-long short_label", {
  p <- validate_fixture_paths()
  write_face_by_hand(p$face, "temperature", strrep("x", 49))
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "short_label_too_long" & out$key == "temperature"))
})

test_that("validate_measurement_faces() flags a short_label ending in punctuation", {
  p <- validate_fixture_paths()
  write_face_by_hand(p$face, "temperature", "Temperature.")
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_true(any(out$rule == "short_label_trailing_punct" & out$key == "temperature"))
})

test_that("validate_measurement_faces() accepts unique short_labels and NA ones", {
  p <- validate_fixture_paths()
  write_face_by_hand(p$face, c("temperature", "salinity", "oxygen"), c("Temperature", "Salinity", NA))
  out <- validate_measurement_faces(p$chem, p$method, p$scale, p$why, p$face)
  expect_equal(nrow(out), 0)
})

test_that("the shipped workflows measurement_face.csv passes validation, short_label included", {
  # the sibling checkout, whether the package sits at CalCOFI/calcofi4db or in a
  # worktree under CalCOFI/.worktrees/
  cand <- c(test_path("..", "..", "..", "workflows", "metadata"),
            test_path("..", "..", "..", "..", "workflows", "metadata"))
  md <- cand[dir.exists(cand)][1]
  skip_if(is.na(md), "workflows/metadata not beside the package")
  # the face file alone: validate_measurement_faces() also reads the other four,
  # whose own state is not this test's subject
  face <- read_measurement_face(file.path(md, "measurement_face.csv"))
  expect_true("short_label" %in% names(face))
  out <- calcofi4db:::.check_short_labels(face$key, face$short_label)
  expect_equal(nrow(out), 0, info = paste(out$key, out$rule, collapse = "; "))
})
