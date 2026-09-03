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

# --- D5: one written precedence, applied centrally ---------------------------
# manual choice > swfsc_ichthyo's own vocabulary > WoRMS single vernacular >
# any other dataset's vocabulary (dataset_key order) > empty. The merged taxon
# table's own common_name is NOT a rank: it is whatever shard won the merge,
# which is exactly the undocumented order this replaces.

common_release_con <- function() {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbWriteTable(con, "taxon", data.frame(
    taxon_key       = c("worms:1", "worms:2", "worms:3", "worms:4", "worms:5", "worms:6", "worms:126175"),
    scientific_name = c("Manualis", "Ichthyus", "Wormsingle", "Otherus", "Nobody",
                        "Shardwinner", "Sebastes"),
    common_name     = c("stale", "stale", "stale", "stale", NA, "from the winning shard", "Rockfishes"),
    stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "dataset_taxon", data.frame(
    ds_taxon_key = c("calcofi:1", "calcofi:2", "calcofi:3", "zoo:4", "bird:4", "calcofi:683", "calcofi:3023"),
    dataset_key  = c("swfsc_ichthyo", "swfsc_ichthyo", "swfsc_ichthyo",
                     "cce-lter_zoodb", "farallon_bird-mammal", "swfsc_ichthyo", "swfsc_ichthyo"),
    taxon_key    = c("worms:1", "worms:2", "worms:3", "worms:4", "worms:4", "worms:126175", "worms:126175"),
    ds_scientific_name = c("Manualis", "Ichthyus", "Wormsingle", "Otherus", "Otherus",
                           "Sebastes", "Sebastes crocotulus"),
    ds_common_name = c("ichthyo name 1", "ichthyo name 2", NA, "ZOODB NAME", "Bird name",
                       "Rockfishes", "Sunset rockfish"),
    ds_taxa_code = c("1", "2", "3", "4", "4", "683", "3023"),
    stringsAsFactors = FALSE))
  con
}

common_release_csv <- function() {
  csv <- withr::local_tempfile(fileext = ".csv", .local_envir = parent.frame())
  utils::write.csv(data.frame(
    taxon_key       = c("worms:1", "worms:2", "worms:3", "worms:4"),
    scientific_name = c("Manualis", "Ichthyus", "Wormsingle", "Otherus"),
    common_name     = c("a human's pick", "worms single 2", "worms single 3", "worms single 4"),
    candidates_en   = c("x | y", "worms single 2", "worms single 3", "worms single 4"),
    n_candidates_en = c(2L, 1L, 1L, 1L),
    source          = c("manual", "worms", "worms", "worms"),
    checked_date = "2026-08-14", notes = NA,
    stringsAsFactors = FALSE), csv, row.names = FALSE, na = "")
  csv
}

test_that("apply_taxon_common applies the D5 order and reports each rank's count", {
  con <- common_release_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  csv <- common_release_csv()

  counts <- apply_taxon_common(con, csv, verbose = FALSE)
  got <- DBI::dbGetQuery(con, "SELECT taxon_key, common_name FROM taxon ORDER BY taxon_key")
  nm <- setNames(got$common_name, got$taxon_key)

  expect_equal(unname(nm["worms:1"]), "a human's pick")     # 1. manual beats ichthyo
  expect_equal(unname(nm["worms:2"]), "ichthyo name 2")     # 2. ichthyo beats worms single
  expect_equal(unname(nm["worms:3"]), "worms single 3")     # 3. worms single (ichthyo has none)
  expect_equal(unname(nm["worms:4"]), "worms single 4")     # 3. ...and beats another dataset
  expect_true(is.na(nm[["worms:5"]]))                       # 5. empty, never a guess
  # the merged shard's own value is not a rank: nothing supplies worms:6 -> empty
  expect_true(is.na(nm[["worms:6"]]))

  expect_s3_class(counts, "data.frame")
  expect_equal(counts$n[counts$source == "manual"],        1L)
  expect_equal(counts$n[counts$source == "swfsc_ichthyo"], 2L)   # worms:2 + worms:126175
  expect_equal(counts$n[counts$source == "worms_single"],  2L)
  expect_equal(counts$n[counts$source == "other"],         0L)
  expect_equal(counts$n[counts$source == "empty"],         2L)
})

test_that("other datasets supply a name in dataset_key order, once WoRMS has no single one", {
  con <- common_release_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  csv <- common_release_csv()
  d <- read_taxon_common(csv); d <- d[d$taxon_key != "worms:4", ]
  write_taxon_common(d, csv)

  counts <- apply_taxon_common(con, csv, verbose = FALSE)
  nm <- DBI::dbGetQuery(con, "SELECT common_name FROM taxon WHERE taxon_key = 'worms:4'")$common_name
  expect_equal(nm, "ZOODB NAME")            # cce-lter_zoodb < farallon_bird-mammal
  expect_equal(counts$n[counts$source == "other"], 1L)
})

test_that("two codes of one dataset sharing a key: the code named as the taxon wins, then ds_taxon_key", {
  # worms:126175 IS the genus Sebastes; ichthyo code 683 'Sebastes' -> 'Rockfishes'
  # and code 3023 'Sebastes crocotulus' -> 'Sunset rockfish' both resolve to it.
  # ds_taxon_key ascending alone would pick 'calcofi:3023' (a string sort), i.e.
  # a species name for a genus row; the rule prefers the code whose
  # ds_scientific_name equals taxon.scientific_name.
  con <- common_release_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  apply_taxon_common(con, common_release_csv(), verbose = FALSE)
  nm <- DBI::dbGetQuery(con, "SELECT common_name FROM taxon WHERE taxon_key = 'worms:126175'")$common_name
  expect_equal(nm, "Rockfishes")

  # with no name match at all, ds_taxon_key ascending is the deterministic fallback
  DBI::dbExecute(con, "UPDATE taxon SET scientific_name = 'Nomatch' WHERE taxon_key = 'worms:126175'")
  apply_taxon_common(con, common_release_csv(), verbose = FALSE)
  nm <- DBI::dbGetQuery(con, "SELECT common_name FROM taxon WHERE taxon_key = 'worms:126175'")$common_name
  expect_equal(nm, "Sunset rockfish")       # 'calcofi:3023' < 'calcofi:683'
})

test_that("apply_taxon_common works without a dataset_taxon table (ranks 2 and 4 empty)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "taxon", data.frame(
    taxon_key = c("worms:440388", "worms:9"), scientific_name = c("Metacarcinus magister", "x"),
    common_name = c(NA, "gone"), stringsAsFactors = FALSE))
  csv <- withr::local_tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    taxon_key = "worms:440388", scientific_name = "Metacarcinus magister",
    common_name = "Dungeness crab", candidates_en = "Californian crab | Dungeness crab",
    n_candidates_en = 2L, source = "manual", checked_date = "2026-08-14", notes = NA,
    stringsAsFactors = FALSE), csv, row.names = FALSE, na = "")
  apply_taxon_common(con, csv, verbose = FALSE)
  got <- DBI::dbGetQuery(con, "SELECT taxon_key, common_name FROM taxon ORDER BY taxon_key")
  expect_equal(got$common_name, c("Dungeness crab", NA))
})

# --- the manual tag ----------------------------------------------------------
# taxon_common.csv never carried a literal "manual": every fetched row was stamped
# source = "worms" whether the value was auto-filled or hand-picked. The two are
# told apart by construction (ensure_taxon_common() only ever auto-fills the one
# candidate), and that reconstruction is written into the registry once.

test_that("mark_taxon_common_manual tags hand-picked rows once, idempotently", {
  csv <- withr::local_tempfile(fileext = ".csv")
  utils::write.csv(data.frame(
    taxon_key = c("worms:1", "worms:2", "worms:3", "worms:4", "worms:5"),
    scientific_name = letters[1:5],
    common_name   = c("auto single", "picked of two", "typed with none", NA, "overrode the single"),
    candidates_en = c("auto single", "x | y", "", "x | y", "the single"),
    n_candidates_en = c(1L, 2L, 0L, 2L, 1L),
    source = c("worms", "worms", NA, "worms", "worms"),
    checked_date = "2026-08-14", notes = NA, stringsAsFactors = FALSE),
    csv, row.names = FALSE, na = "")

  n <- mark_taxon_common_manual(csv, verbose = FALSE)
  expect_equal(n, 3L)
  d <- read_taxon_common(csv)
  expect_equal(setNames(d$source, d$taxon_key),
               c("worms:1" = "worms", "worms:2" = "manual", "worms:3" = "manual",
                 "worms:4" = "worms", "worms:5" = "manual"))
  expect_equal(mark_taxon_common_manual(csv, verbose = FALSE), 0L)   # nothing left to tag
  raw <- paste(readLines(csv), collapse = "\n")
  expect_false(grepl(",NA,", raw, fixed = TRUE))
})

test_that("ensure_taxon_common keeps a manual tag on re-run and tags a new human pick", {
  csv <- withr::local_tempfile(fileext = ".csv")
  with_vernaculars(list("440388" = CRAB, "158711" = "hardhead sea catfish"), {
    ensure_taxon_common(taxa_df("worms:440388" = 440388L, "worms:158711" = 158711L),
                        cache_csv = csv, sleep = 0, verbose = FALSE)
    d <- read_taxon_common(csv)
    d$common_name[d$taxon_key == "worms:440388"] <- "Dungeness crab"   # the human's pick
    write_taxon_common(d, csv)
    again <- ensure_taxon_common(taxa_df("worms:440388" = 440388L, "worms:158711" = 158711L),
                                 cache_csv = csv, sleep = 0, refresh = TRUE, verbose = FALSE)
    src <- setNames(again$source, again$taxon_key)
    expect_equal(unname(src["worms:440388"]), "manual")
    expect_equal(unname(src["worms:158711"]), "worms")
  })
})
