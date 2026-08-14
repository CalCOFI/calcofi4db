# The whole point of this module is WHO chooses when WoRMS offers several English
# vernaculars. WoRMS returns an unordered bag with no preferred-name flag, so an
# automatic rule is wrong for exactly the taxon that motivated this:
# worms:440388 Metacarcinus magister -> Californian crab | Dungeness crab |
# Dungeness rock crab | Pacific crab. These pin that one name is taken, several
# are left for a human, and a human's pick is never overwritten.

skip_if_not_installed("worrms")

CRAB <- c("Californian crab", "Dungeness crab", "Dungeness rock crab", "Pacific crab")

# stub the network so the tests are offline and deterministic
with_vernaculars <- function(map, expr) {
  testthat::local_mocked_bindings(
    .worms_vernaculars_en = function(worms_id, sleep = 0.3) {
      v <- map[[as.character(worms_id)]]
      if (is.null(v)) character() else v
    })
  force(expr)
}

taxa_df <- function(...) {
  d <- list(...)
  data.frame(taxon_key = names(d), worms_id = as.integer(unlist(d)),
             scientific_name = names(d), stringsAsFactors = FALSE)
}

test_that("one English vernacular is taken automatically", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list("158711" = "hardhead sea catfish"), {
    out <- ensure_taxon_common(taxa_df("worms:158711" = 158711L),
                               cache_csv = csv, sleep = 0, verbose = FALSE)
    expect_equal(out$common_name, "hardhead sea catfish")
    expect_equal(out$n_candidates_en, 1L)
  })
})

test_that("several vernaculars are NOT chosen between — the crab is left for a human", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list("440388" = CRAB), {
    out <- ensure_taxon_common(taxa_df("worms:440388" = 440388L),
                               cache_csv = csv, sleep = 0, verbose = FALSE)
    expect_true(is.na(out$common_name))          # no guess
    expect_equal(out$n_candidates_en, 4L)
    expect_equal(out$candidates_en, paste(CRAB, collapse = " | "))
    expect_match(out$notes, "pick one")
    # and specifically NOT the alphabetically-first one, which is the trap
    expect_false(identical(out$common_name, "Californian crab"))
  })
})

test_that("a human's pick survives a re-run, including refresh = TRUE", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list("440388" = CRAB), {
    ensure_taxon_common(taxa_df("worms:440388" = 440388L),
                        cache_csv = csv, sleep = 0, verbose = FALSE)
    # the selection: a human edits the one cell
    d <- read_taxon_common(csv)
    d$common_name <- "Dungeness crab"
    utils::write.csv(d, csv, row.names = FALSE, na = "")

    again <- ensure_taxon_common(taxa_df("worms:440388" = 440388L),
                                 cache_csv = csv, sleep = 0, refresh = TRUE,
                                 verbose = FALSE)
    expect_equal(again$common_name, "Dungeness crab")
  })
})

test_that("no vernaculars is cached, not retried forever", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list(), {
    out <- ensure_taxon_common(taxa_df("worms:999999" = 999999L),
                               cache_csv = csv, sleep = 0, verbose = FALSE)
    expect_equal(nrow(out), 1L)
    expect_equal(out$n_candidates_en, 0L)
    expect_true(is.na(out$common_name))
  })
})

test_that("an empty cell round-trips empty, not as the string 'NA'", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list("440388" = CRAB), {
    ensure_taxon_common(taxa_df("worms:440388" = 440388L),
                        cache_csv = csv, sleep = 0, verbose = FALSE)
  })
  raw <- paste(readLines(csv), collapse = "\n")
  expect_false(grepl(",NA,", raw, fixed = TRUE))
  expect_true(is.na(read_taxon_common(csv)$common_name))
})

test_that("apply_taxon_common fills only the blanks and never overwrites a dataset's own name", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "taxon", data.frame(
    taxon_key       = c("worms:440388", "worms:158711", "itis:1", "worms:2"),
    scientific_name = c("Metacarcinus magister", "Ariopsis felis", "Aves sp.", "Nemo"),
    # the bird already has a name from ITS OWN dataset vocabulary
    common_name     = c(NA, NA, "Sooty Shearwater", ""),
    stringsAsFactors = FALSE))

  csv <- withr::local_tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    taxon_key       = c("worms:440388", "worms:158711", "itis:1", "worms:2"),
    scientific_name = c("Metacarcinus magister", "Ariopsis felis", "Aves sp.", "Nemo"),
    common_name     = c("Dungeness crab", "hardhead sea catfish", "SHOULD NOT WIN", NA),
    candidates_en   = NA, n_candidates_en = 1L, source = "worms",
    checked_date = "2026-08-14", notes = NA,
    stringsAsFactors = FALSE), csv, row.names = FALSE, na = "")

  n <- apply_taxon_common(con, csv, verbose = FALSE)
  got <- DBI::dbGetQuery(con, "SELECT taxon_key, common_name FROM taxon ORDER BY taxon_key")
  nm <- setNames(got$common_name, got$taxon_key)

  expect_equal(unname(nm["worms:440388"]), "Dungeness crab")
  expect_equal(unname(nm["worms:158711"]), "hardhead sea catfish")
  # the dataset's own name wins over the registry
  expect_equal(unname(nm["itis:1"]), "Sooty Shearwater")
  # empty-string counts as blank and stays unnamed when the registry has nothing
  expect_true(is.na(nm[["worms:2"]]) || !nzchar(nm[["worms:2"]]))
  expect_equal(n, 2L)
})
