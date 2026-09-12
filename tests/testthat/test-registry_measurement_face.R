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
