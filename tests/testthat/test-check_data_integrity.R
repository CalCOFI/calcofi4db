# a minimal read_csv_files()-shaped list: one source table `t` with fields
# `flds`, redefined by `rd_tbl` / `rd_flds`
cdi_d <- function(tbl = "t", flds = c("a", "b"), rd_tbl = "t", rd_flds = c("a", "b")) {
  list(
    d_csv = list(data = tibble::tibble(
      tbl  = tbl,
      flds = list(tibble::tibble(fld = flds, type = "character")))),
    d_tbls_rd = tibble::tibble(tbl_old = rd_tbl, tbl_new = rd_tbl),
    d_flds_rd = tibble::tibble(
      tbl_old = rd_tbl, fld_old = rd_flds, type_old = "character"),
    paths = list(tbls_rd_csv = "tbls_redefine.csv", flds_rd_csv = "flds_redefine.csv"))
}

cdi_eval <- function(env = parent.frame()) {
  # check_data_integrity() sets knitr's global chunk option; restore it
  old <- knitr::opts_chunk$get("eval")
  withr::defer(knitr::opts_chunk$set(eval = old), envir = env)
}

test_that("a failing check stops a non-interactive render (the ichthyo silent halt)", {
  cdi_eval()
  # the 2026-09-26 export renamed every table to TitleCase: the check found 30
  # table-level mismatches, set eval = FALSE, and the render still exited 0
  d <- cdi_d(tbl = "Cruise", rd_tbl = "cruise")
  expect_error(
    suppressWarnings(check_data_integrity(
      d, "ichthyo", display_format = "tibble", verbose = FALSE, stop_on_fail = TRUE)),
    "Data integrity check failed for ichthyo: 2 mismatch")
})

test_that("stop_on_fail defaults to TRUE when the session is not interactive", {
  cdi_eval()
  withr::local_options(rlang_interactive = FALSE)
  d <- cdi_d(flds = c("a", "b", "c"))   # a source field nobody redefined
  expect_error(
    suppressWarnings(check_data_integrity(d, "x", display_format = "tibble", verbose = FALSE)),
    "Fields added")
})

test_that("interactively a failing check only disables the remaining chunks", {
  cdi_eval()
  withr::local_options(rlang_interactive = TRUE)
  d <- cdi_d(flds = c("a", "b", "c"))
  res <- suppressWarnings(check_data_integrity(
    d, "x", display_format = "tibble", verbose = FALSE))
  expect_false(res$passed)
  expect_equal(res$n_changes, 1)
  expect_false(knitr::opts_chunk$get("eval"))
})

test_that("halt_on_fail = FALSE never stops, even non-interactively", {
  cdi_eval()
  withr::local_options(rlang_interactive = FALSE)
  d <- cdi_d(flds = c("a", "b", "c"))
  res <- suppressWarnings(check_data_integrity(
    d, "x", halt_on_fail = FALSE, display_format = "tibble", verbose = FALSE))
  expect_false(res$passed)
})

test_that("a passing check never stops and re-enables evaluation", {
  cdi_eval()
  withr::local_options(rlang_interactive = FALSE)
  res <- check_data_integrity(cdi_d(), "x", display_format = "tibble", verbose = FALSE)
  expect_true(res$passed)
  expect_true(knitr::opts_chunk$get("eval"))
})

test_that("check_multiple_datasets() stops non-interactively when any dataset fails", {
  cdi_eval()
  withr::local_options(rlang_interactive = FALSE)
  ds <- list(ok = cdi_d(), bad = cdi_d(flds = c("a", "b", "c")))
  expect_error(
    suppressWarnings(utils::capture.output(
      check_multiple_datasets(ds, display_format = "tibble"))),
    "1 dataset\\(s\\): bad")
})
