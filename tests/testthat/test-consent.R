# The consent gate: a dataset reaches the public database only after its
# provider's consent row (`related_field = consent` in questions.csv) is
# `answered` with a yes and a who. A preview run exempts the datasets it
# includes; a dataset released before the gate existed (no `in_release` key in
# its notebook) is reported, and halts only on a provider's no.

# a workflows dir: one notebook per dataset, each with its own questions.csv
make_consent_dir <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  dir.create(file.path(dir, "metadata"))
  dir
}

add_dataset <- function(dir, provider, dataset, in_release = NULL, consent = NULL,
                        questions = !is.null(consent)) {
  writeLines(c(
    "---",
    glue::glue("title: \"Ingest {provider} {dataset}\""),
    "calcofi:",
    glue::glue("  target_name: ingest_{provider}_{dataset}"),
    "  workflow_type: ingest",
    glue::glue("  output: data/parquet/{provider}_{dataset}/manifest.json"),
    glue::glue("  provider: {provider}"),
    glue::glue("  dataset: {dataset}"),
    if (!is.null(in_release)) glue::glue("  in_release: {in_release}"),
    "---", "", "## Body"),
    file.path(dir, glue::glue("ingest_{provider}_{dataset}.qmd")))
  if (!questions) return(invisible())
  d <- tibble::tibble(
    label = "Q01", id = glue::glue("{provider}_{dataset}_01"),
    question = "May we publish?", context = "ctx", status = "open", priority = "normal",
    proposed_answer = NA, answer = NA, asked_date = "2026-10-22", answered_date = NA,
    who = NA, related_table = "dataset", related_field = NA)
  if (!is.null(consent)) {
    d <- rbind(d, tibble::tibble(
      label = "Q02", id = glue::glue("{provider}_{dataset}_02"),
      question = "Consent?", context = "Standing consent row",
      status = consent$status, priority = "high",
      proposed_answer = NA, answer = consent$answer %||% NA, asked_date = "2026-10-22",
      answered_date = consent$answered_date %||% NA, who = consent$who %||% NA,
      related_table = "dataset", related_field = "consent"))
  }
  mdir <- file.path(dir, "metadata", provider, dataset)
  dir.create(mdir, recursive = TRUE)
  readr::write_csv(d, file.path(mdir, "questions.csv"), na = "")
  invisible()
}

given <- list(status = "answered", answer = "yes. By email 2026-10-19.",
              who = "Rasmus Swalethorp", answered_date = "2026-10-19")

test_that("none: no questions.csv, or no consent row, is ok", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "nofile")
  add_dataset(dir, "x", "norow", questions = TRUE)
  out <- check_dataset_consent(dir)
  expect_equal(out$dataset_key, c("x_nofile", "x_norow"))
  expect_equal(out$consent, c("none", "none"))
  expect_true(all(out$ok))
  expect_true(all(is.na(out$label)))
  expect_equal(out$why, rep("no consent row", 2))
})

test_that("given: answered + yes + who is ok, with the exact row values", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", in_release = "true", consent = given)
  out <- check_dataset_consent(dir)
  expect_equal(nrow(out), 1L)
  expect_equal(out$dataset_key, "x_ds")
  expect_equal(out$consent, "given")
  expect_equal(out$label, "Q02")
  expect_equal(out$status, "answered")
  expect_equal(out$answer, "yes. By email 2026-10-19.")
  expect_equal(out$who, "Rasmus Swalethorp")
  expect_equal(out$answered_date, "2026-10-19")
  expect_true(out$ok)
  expect_false(out$exempt)
  expect_false(out$legacy)
  expect_equal(out$why, "consent given")
})

test_that("given: 'Yes' is case-insensitive and 'No ask needed' (licence at source) counts", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "caps", in_release = "true",
              consent = list(status = "answered", answer = "  YES, verbal 2026-10-30", who = "PI"))
  add_dataset(dir, "x", "lic", in_release = "true",
              consent = list(status = "answered",
                             answer = "No ask needed: openly licensed at its public source.",
                             who = "Ben Best (licence at source)"))
  out <- check_dataset_consent(dir)
  expect_equal(out$consent, c("given", "given"))
  expect_true(all(out$ok))
})

test_that("pending: an open ask on an un-held dataset fails, and halt names it", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", in_release = "true", consent = list(status = "open"))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_equal(out$consent, "pending")
  expect_equal(out$status, "open")
  expect_false(out$ok)
  expect_equal(out$why, "ask pending: the provider has not yet said yes")
  expect_error(check_dataset_consent(dir), "x_ds: pending \\(Q02, status open\\)")
  expect_error(check_dataset_consent(dir), "1 dataset\\(s\\) lack the provider's consent")
})

test_that("pending: a proposed row is pending too", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", in_release = "true", consent = list(status = "proposed"))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_equal(out$consent, "pending")
  expect_false(out$ok)
})

test_that("declined: answered 'No' fails; wontfix fails", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "said-no", in_release = "true",
              consent = list(status = "answered", answer = "No, not this year.", who = "PI"))
  add_dataset(dir, "x", "wf", in_release = "true", consent = list(status = "wontfix"))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_equal(out$consent, c("declined", "declined"))
  expect_equal(out$ok, c(FALSE, FALSE))
  expect_equal(out$why, c("answered, but not a yes", "closed without consent (wontfix)"))
  expect_error(check_dataset_consent(dir), "x_said-no: declined")
  expect_error(check_dataset_consent(dir), "x_wf: declined")
})

test_that("yes without who fails: the consent must name who gave it", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", in_release = "true",
              consent = list(status = "answered", answer = "yes", who = ""))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_equal(out$consent, "given")
  expect_true(is.na(out$who))
  expect_false(out$ok)
  expect_match(out$why, "`who` is empty")
  expect_error(check_dataset_consent(dir), "x_ds: given")
})

test_that("exempt: a preview-included (held) dataset is reported but never fails", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "held", in_release = "false", consent = list(status = "open"))
  add_dataset(dir, "x", "pub",  consent = given)
  # a held dataset is not in the default set unless exempted
  expect_equal(check_dataset_consent(dir)$dataset_key, "x_pub")
  out <- check_dataset_consent(dir, exempt = "x_held")
  expect_setequal(out$dataset_key, c("x_held", "x_pub"))
  held <- out[out$dataset_key == "x_held", ]
  expect_equal(held$consent, "pending")
  expect_true(held$exempt)
  expect_true(held$ok)
  expect_match(held$why, "exempt")
  # even a declined answer never fails an exempt dataset
  dir2 <- make_consent_dir()
  add_dataset(dir2, "x", "held", in_release = "false",
              consent = list(status = "answered", answer = "no", who = "PI"))
  out2 <- check_dataset_consent(dir2, exempt = "x_held")
  expect_equal(out2$consent, "declined")
  expect_true(out2$ok)
})

test_that("legacy: a notebook with no in_release key reports a pending ask without failing", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "old", consent = list(status = "open", who = "PI"))
  out <- check_dataset_consent(dir)
  expect_equal(out$consent, "pending")
  expect_true(out$legacy)
  expect_true(out$ok)
  expect_match(out$why, "released before the consent gate")
  # legacy = character() tightens it to the literal rule
  expect_false(check_dataset_consent(dir, legacy = character(), halt = FALSE)$ok)
  expect_error(check_dataset_consent(dir, legacy = character()), "x_old: pending")
})

test_that("legacy: a provider's no still fails", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "old", consent = list(status = "answered", answer = "No.", who = "PI"))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_equal(out$consent, "declined")
  expect_true(out$legacy)
  expect_false(out$ok)
  expect_error(check_dataset_consent(dir), "x_old: declined")
})

test_that("an un-held dataset (in_release: true) is never legacy", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", in_release = "true", consent = list(status = "open"))
  out <- check_dataset_consent(dir, halt = FALSE)
  expect_false(out$legacy)
  expect_false(out$ok)
})

test_that("datasets= overrides the default set; an unknown key errors", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "a", consent = given)
  add_dataset(dir, "x", "b", consent = given)
  expect_equal(check_dataset_consent(dir, datasets = "x_b")$dataset_key, "x_b")
  expect_error(check_dataset_consent(dir, datasets = "x_zzz"), "no ingest notebook declares")
})

test_that("two consent rows in one questions.csv is an error", {
  dir <- make_consent_dir()
  add_dataset(dir, "x", "ds", consent = given)
  p <- file.path(dir, "metadata", "x", "ds", "questions.csv")
  q <- readr::read_csv(p, na = "", show_col_types = FALSE,
                       col_types = readr::cols(.default = readr::col_character()))
  q$related_field[q$label == "Q01"] <- "consent"
  readr::write_csv(q, p, na = "")
  expect_error(check_dataset_consent(dir), "2 consent rows")
})

test_that("the empty result has the full column set", {
  dir <- make_consent_dir()
  out <- check_dataset_consent(dir, datasets = character())
  expect_equal(nrow(out), 0L)
  expect_equal(names(out), c("dataset_key", "consent", "label", "status", "answer", "who",
                             "answered_date", "ok", "exempt", "legacy", "why"))
})
