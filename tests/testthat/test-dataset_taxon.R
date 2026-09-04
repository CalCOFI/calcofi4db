# The staged crosswalk (taxon plan D1 / D2 / D6, calcofi4db 3.29.0).
#
# An ingest appends its vocabulary to `dataset_taxon` with `taxon_key` empty;
# the package fills `taxon_key` from the authorities; the key rule reads the
# classification, not a flag. Everything here is offline: the lineage is
# pre-staged as `_taxon_lineage_flat`, the cross-reference as `_taxon_xref`.

# a connection carrying the flattened classification ensure_taxon_lineage()
# would stage — the class decides the key authority, so the fixture pins it
new_staged_con <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  DBI::dbExecute(con, "CREATE TABLE _taxon_lineage_flat AS
    SELECT 174715 requested_id, 'ITIS' authority, 'Species' AS \"rank\", 174712 parent_id,
           'Phalacrocorax carbo' scientific_name, 'Animalia' kingdom, 'Chordata' phylum,
           'Aves' AS \"class\", 'Pelecaniformes' order_taxon, 'Phalacrocoracidae' AS \"family\"
    UNION ALL SELECT 1255050,'ITIS','Species',1255018,'Ardenna grisea','Animalia','Chordata','Aves','Procellariiformes','Procellariidae'
    UNION ALL SELECT 137090,'WoRMS','Species',137013,'Balaenoptera musculus','Animalia','Chordata','Mammalia','Cetartiodactyla','Balaenopteridae'
    UNION ALL SELECT 137179,'WoRMS','Species',137071,'Phalacrocorax carbo','Animalia','Chordata','Aves','Pelecaniformes','Phalacrocoracidae'
    UNION ALL SELECT 217452,'WoRMS','Species',125464,'Sardinops sagax','Animalia','Chordata','Actinopteri','Clupeiformes','Clupeidae'
    UNION ALL SELECT 137202,'WoRMS','Species',137039,'Ardenna grisea','Animalia','Chordata','Aves','Procellariiformes','Procellariidae'")
  con
}

# the D1 input shape: what a notebook hands over
staged_df <- function() data.frame(
  ds_taxa_code       = c("PASA", "GRCO", "BLWH", "NOID", "SOSH"),
  ds_scientific_name = c("Sardinops sagax", "Phalacrocorax carbo",
                         "Balaenoptera musculus", "(species group)", "Puffinus griseus"),
  ds_common_name     = c("Pacific sardine", "Great Cormorant", "Blue Whale",
                         "Unidentified thing", "Sooty Shearwater"),
  worms_id = c(217452L, NA, NA, NA, NA),
  itis_id  = c(161729L, 174715L, NA, NA, 174553L),
  gbif_id  = c(2415428L, NA, NA, NA, NA),
  stringsAsFactors = FALSE)

read_dt <- function(con, ds = NULL) {
  q <- "SELECT * FROM dataset_taxon"
  if (!is.null(ds)) q <- paste0(q, " WHERE dataset_key = '", ds, "'")
  DBI::dbGetQuery(con, paste(q, "ORDER BY ds_taxon_key"))
}

# --- D1: the column contract --------------------------------------------------

test_that("append_dataset_taxon stages rows with taxon_key empty and ds_source_json built", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  n <- append_dataset_taxon(con, "demo_ds", staged_df())
  expect_equal(n, 5L)
  dt <- read_dt(con)

  expect_equal(names(dt), c("ds_taxon_key", "dataset_key", "taxon_key",
                            "ds_scientific_name", "ds_common_name", "ds_taxa_code",
                            "ds_source_json"))
  expect_equal(dt$dataset_key, rep("demo_ds", 5))
  expect_equal(dt$ds_taxon_key, paste0("demo_ds:", sort(staged_df()$ds_taxa_code)))
  expect_true(all(is.na(dt$taxon_key)))              # the package fills it, not the notebook
  # the source's own claims, verbatim, as one JSON object; NULL when it made none
  j <- setNames(dt$ds_source_json, dt$ds_taxa_code)
  expect_equal(unname(j["GRCO"]), '{"itis_id":174715}')
  expect_equal(unname(j["PASA"]), '{"worms_id":217452,"itis_id":161729,"gbif_id":2415428}')
  expect_true(is.na(j[["NOID"]]))
  # VARCHAR even though this run's taxon_key is all-NULL (not a BOOLEAN column)
  ty <- DBI::dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns
                              WHERE table_name = 'dataset_taxon'")
  expect_true(all(ty$data_type == "VARCHAR"))
})

test_that("ds_prefix mints the ds_taxon_key; the dataset_key stays the observing dataset", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "swfsc_ichthyo", staged_df()[1, ], ds_prefix = "calcofi")
  dt <- read_dt(con)
  expect_equal(dt$ds_taxon_key, "calcofi:PASA")
  expect_equal(dt$dataset_key, "swfsc_ichthyo")
})

test_that("a second append replaces this dataset's rows and leaves other datasets alone", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  append_dataset_taxon(con, "other_ds", staged_df()[1:2, ])
  append_dataset_taxon(con, "demo_ds", staged_df()[3, ])      # shrinks to one row
  dt <- read_dt(con)
  expect_equal(sum(dt$dataset_key == "demo_ds"), 1L)
  expect_equal(sum(dt$dataset_key == "other_ds"), 2L)
})

test_that("append_dataset_taxon refuses a missing required column", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- staged_df(); d$ds_scientific_name <- NULL
  expect_error(append_dataset_taxon(con, "demo_ds", d), "ds_scientific_name")
  d <- staged_df(); d$ds_taxa_code <- NULL
  expect_error(append_dataset_taxon(con, "demo_ds", d), "ds_taxa_code")
})

test_that("append_dataset_taxon refuses an unknown column — a rename is a hard stop, not an NA", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- staged_df(); d$is_bird <- TRUE
  expect_error(append_dataset_taxon(con, "demo_ds", d), "is_bird")
  d <- staged_df(); names(d)[names(d) == "itis_id"] <- "itis"
  expect_error(append_dataset_taxon(con, "demo_ds", d), "itis")
})

test_that("append_dataset_taxon refuses a duplicate or NA ds_taxa_code", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- rbind(staged_df(), staged_df()[2, ])                   # SBIG appears twice
  expect_error(append_dataset_taxon(con, "demo_ds", d), "GRCO")
  d <- staged_df(); d$ds_taxa_code[1] <- NA
  expect_error(append_dataset_taxon(con, "demo_ds", d), "NA")
  d <- staged_df(); d$ds_taxa_code[1] <- ""
  expect_error(append_dataset_taxon(con, "demo_ds", d), "empty")
})

test_that("append_dataset_taxon refuses an id that does not coerce to an integer", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- staged_df(); d$itis_id <- as.character(d$itis_id); d$itis_id[2] <- "abc"
  expect_error(append_dataset_taxon(con, "demo_ds", d), "itis_id")
  d <- staged_df(); d$worms_id <- as.numeric(d$worms_id); d$worms_id[1] <- 1.5
  expect_error(append_dataset_taxon(con, "demo_ds", d), "worms_id")
  # a string of digits and a whole double are fine
  d <- staged_df(); d$itis_id <- as.character(d$itis_id); d$worms_id <- as.numeric(d$worms_id)
  expect_no_error(append_dataset_taxon(con, "demo_ds", d))
  expect_equal(read_dt(con)$ds_source_json[read_dt(con)$ds_taxa_code == "GRCO"],
               '{"itis_id":174715}')
})

test_that("an empty vocabulary is refused rather than silently staging nothing", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  expect_error(append_dataset_taxon(con, "demo_ds", staged_df()[0, ]), "no rows")
})

# --- D2: the key is derived from the class, and filled in place --------------

test_that("resolve_dataset_taxon fills taxon_key on the staged rows without touching the rest", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  before <- read_dt(con)
  resolve_dataset_taxon(con)
  after <- read_dt(con)

  # every non-key column is byte-identical — this is a fill, not a rebuild
  keep <- setdiff(names(before), "taxon_key")
  expect_equal(after[keep], before[keep])
  k <- setNames(after$taxon_key, after$ds_taxa_code)
  expect_equal(unname(k["PASA"]), "worms:217452")   # fish: worms
  expect_equal(unname(k["GRCO"]), "itis:174715")    # class Aves + TSN: itis
  expect_equal(unname(k["NOID"]), "demo_ds:NOID")   # nothing resolved: dataset-local
})

test_that("a bird keys itis: only when its class is Aves AND a TSN resolves", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  # GRCO carries only a TSN; WORMBIRD carries only an AphiaID whose class is Aves
  d <- data.frame(ds_taxa_code = c("GRCO", "WORMBIRD"),
                  ds_scientific_name = c("Phalacrocorax carbo", "Phalacrocorax carbo"),
                  worms_id = c(NA, 137179L), itis_id = c(174715L, NA),
                  stringsAsFactors = FALSE)
  append_dataset_taxon(con, "demo_ds", d)
  resolve_dataset_taxon(con)
  build_taxon_reference(con)
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "GRCO"],     "itis:174715")
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "WORMBIRD"], "worms:137179")
  # ...and the worms:-keyed bird says so in its notes — visible, not silent
  nt <- DBI::dbGetQuery(con, "SELECT notes FROM taxon WHERE taxon_key = 'worms:137179'")$notes
  expect_match(nt, "class Aves")
  expect_match(nt, "no accepted TSN")
})

test_that("a non-bird carrying only a TSN falls back to the dataset-local key (D2, not itis:)", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- data.frame(ds_taxa_code = "ONLYTSN", ds_scientific_name = "Nemo",
                  itis_id = 999999L, stringsAsFactors = FALSE)
  append_dataset_taxon(con, "demo_ds", d)
  resolve_dataset_taxon(con)
  expect_equal(read_dt(con)$taxon_key, "demo_ds:ONLYTSN")
})

test_that("overrides reach staged rows through the ds_* match columns", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  ov <- data.frame(dataset_key = "demo_ds", match_column = "ds_taxa_code",
                   match_value = "BLWH", worms_id = 137090L, itis_id = NA_integer_,
                   scientific_name = "Balaenoptera musculus", rank = "Species",
                   stringsAsFactors = FALSE)
  resolve_dataset_taxon(con, overrides = ov)
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "BLWH"], "worms:137090")
  # the source's claim is unchanged: it supplied nothing for BLWH
  expect_true(is.na(dt$ds_source_json[dt$ds_taxa_code == "BLWH"]))

  # a source column name from the old arm no longer exists on a staged dataset
  ov$match_column <- "species_code"
  expect_error(resolve_dataset_taxon(con, overrides = ov), "species_code")
})

test_that("the staged cross-reference re-keys a deprecated TSN on staged rows", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  DBI::dbWriteTable(con, "_taxon_xref", data.frame(
    query_type = "tsn", query_value = "174553", worms_id = 137202L, itis_id = 1255050L,
    matched_name = "Puffinus griseus", accepted_name = "Ardenna grisea",
    rank = "Species", status = "accepted", checked_date = "2026-08-05",
    notes = "2026-08-05: itis:174553 deprecated in ITIS -> itis:1255050 (Ardenna grisea)",
    stringsAsFactors = FALSE))
  resolve_dataset_taxon(con)
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "SOSH"], "itis:1255050")
  expect_equal(dt$ds_scientific_name[dt$ds_taxa_code == "SOSH"], "Puffinus griseus")  # verbatim
  expect_equal(dt$ds_source_json[dt$ds_taxa_code == "SOSH"], '{"itis_id":174553}')    # the claim
})

test_that("a per-dataset taxon table is no longer read, and the error names it", {
  # 4.0.0 deleted the seven arms. A notebook that did not migrate leaves its
  # working table in the connection and resolves NOTHING -- that must be a hard
  # stop naming the table, never a silently empty vocabulary.
  con <- new_staged_con(); on.exit(close_duckdb(con))
  DBI::dbExecute(con, "CREATE TABLE bird_mammal_species AS
    SELECT 'GRCO' species_code,'Great Cormorant' common_name,'Phalacrocorax carbo' scientific_name,
           174715 itis_id, TRUE is_bird, FALSE is_mammal, FALSE is_unidentified, TRUE include_flag")
  expect_error(resolve_dataset_taxon(con), "append_dataset_taxon")
  expect_error(resolve_dataset_taxon(con), "bird_mammal_species")

  # ...and once it stages, only the staged rows are the vocabulary
  append_dataset_taxon(con, "farallon_bird-mammal", staged_df()[2, ])   # GRCO only
  resolve_dataset_taxon(con)
  dt <- read_dt(con, "farallon_bird-mammal")
  expect_equal(dt$ds_taxa_code, "GRCO")
  expect_equal(dt$taxon_key, "itis:174715")
})

test_that("resolve_dataset_taxon is idempotent on staged rows", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  resolve_dataset_taxon(con); a <- read_dt(con)
  resolve_dataset_taxon(con); b <- read_dt(con)
  expect_equal(a, b)
})

test_that("build_dataset_taxon is a deprecated alias of resolve_dataset_taxon", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  lifecycle::expect_deprecated(build_dataset_taxon(con))
  expect_equal(read_dt(con)$taxon_key[read_dt(con)$ds_taxa_code == "PASA"], "worms:217452")
})

# --- D6: the ingest asserts its own crosswalk --------------------------------

test_that("check_dataset_taxon is silent on a clean crosswalk", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df()[1:3, ])
  ov <- data.frame(dataset_key = "demo_ds", match_column = "ds_taxa_code",
                   match_value = "BLWH", worms_id = 137090L, itis_id = NA_integer_,
                   scientific_name = "Balaenoptera musculus", rank = "Species",
                   stringsAsFactors = FALSE)
  resolve_dataset_taxon(con, overrides = ov)
  build_taxon_reference(con, overrides = ov)
  rpt <- check_dataset_taxon(con, "demo_ds", codes = c("PASA", "GRCO", "BLWH"), verbose = FALSE)
  expect_s3_class(rpt, "data.frame")
  expect_equal(nrow(rpt), 0L)
})

test_that("check_dataset_taxon fails on a code the observations use but the vocabulary lacks (MEGU)", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df()[1:2, ])
  resolve_dataset_taxon(con)
  expect_error(check_dataset_taxon(con, "demo_ds", codes = c("PASA", "MEGU"), verbose = FALSE),
               "MEGU")
  rpt <- check_dataset_taxon(con, "demo_ds", codes = c("PASA", "MEGU"), halt = FALSE, verbose = FALSE)
  expect_equal(rpt$check, "missing_code")
  expect_equal(rpt$ds_taxa_code, "MEGU")
})

test_that("check_dataset_taxon fails on an unresolved taxon unless it is allow-listed", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df()[c(1, 2, 4), ])   # NOID resolves to nothing
  resolve_dataset_taxon(con)
  expect_error(check_dataset_taxon(con, "demo_ds", verbose = FALSE), "demo_ds:NOID")
  rpt <- check_dataset_taxon(con, "demo_ds", allow = "demo_ds:NOID", halt = FALSE, verbose = FALSE)
  expect_false(any(rpt$check == "unresolved"))
})

test_that("check_dataset_taxon flags an Aves taxon that did not key itis:", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  d <- data.frame(ds_taxa_code = "WORMBIRD", ds_scientific_name = "Phalacrocorax carbo",
                  worms_id = 137179L, stringsAsFactors = FALSE)
  append_dataset_taxon(con, "demo_ds", d)
  resolve_dataset_taxon(con)
  build_taxon_reference(con)
  expect_error(check_dataset_taxon(con, "demo_ds", verbose = FALSE), "Aves")
  rpt <- check_dataset_taxon(con, "demo_ds", halt = FALSE, verbose = FALSE)
  expect_equal(rpt$check, "aves_not_itis")
  # the ingest can accept it explicitly, in the open
  expect_equal(nrow(check_dataset_taxon(con, "demo_ds", allow = "worms:137179", verbose = FALSE)), 0L)
})

test_that("check_dataset_taxon refuses a dataset that staged nothing", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  expect_error(check_dataset_taxon(con, "demo_ds", verbose = FALSE), "no dataset_taxon rows")
})

# --- registries: claims are validated where every dataset is present ---------

test_that("check_taxon_registries catches an override or group rule naming a dataset nothing supplies", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "demo_ds", staged_df())
  resolve_dataset_taxon(con)
  ov <- data.frame(dataset_key = c("demo_ds", "sio_mesopelagic_fish"),   # underscore typo
                   match_column = "ds_taxa_code", match_value = "x",
                   worms_id = 1L, itis_id = NA_integer_, stringsAsFactors = FALSE)
  expect_error(check_taxon_registries(con, overrides = ov), "sio_mesopelagic_fish")
  expect_no_error(check_taxon_registries(con, overrides = ov[1, ]))
  # measurement_taxon datasets count as present
  mt <- data.frame(dataset_key = "swfsc_cufes", stringsAsFactors = FALSE)
  ov2 <- ov; ov2$dataset_key[2] <- "swfsc_cufes"
  expect_no_error(check_taxon_registries(con, overrides = ov2, measurement_taxon = mt))
  rules <- data.frame(taxon_group_key = "x:y", description = "d", rule = "dataset_taxon",
                      rule_value = NA, dataset_key = "nope_ds", match_column = "ds_common_name",
                      match_value = "v", stringsAsFactors = FALSE)
  expect_error(check_taxon_registries(con, group_rules = rules), "nope_ds")
})

# --- the override rule (Ben, 2026-09-04): an override never replaces an id the
# source supplied. A row matched on a NON-code column (`ds_common_name`,
# `ds_scientific_name`; the phyto arm's `taxa`) applies only where the source
# supplied no worms_id / itis_id; a row matched on the dataset's own code
# (`ds_taxa_code`) applies always. v2026.08.25 released 22 phytoplankton keys
# for 393 codes because six functional-group rows replaced 287 species ids.

phyto_pair <- function() data.frame(
  ds_taxa_code       = c("600", "316"),
  ds_scientific_name = c("Actinocyclus", NA),
  ds_common_name     = c("diatom, centric", "diatom, centric"),
  worms_id           = c(196347L, NA),
  stringsAsFactors = FALSE)

group_override <- function(match_column = "ds_common_name", match_value = "diatom, centric")
  data.frame(dataset_key = "calcofi_phytoplankton", match_column = match_column,
             match_value = match_value, worms_id = 148899L, itis_id = NA_integer_,
             scientific_name = "Bacillariophyceae", rank = "Class",
             stringsAsFactors = FALSE)

test_that("a non-code override applies only where the source supplied no id", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "calcofi_phytoplankton", phyto_pair())
  resolve_dataset_taxon(con, overrides = group_override())
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "600"], "worms:196347")   # the source's own id, kept
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "316"], "worms:148899")   # the source had none: filled
})

test_that("a code-matched override applies even where the source supplied an id", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "calcofi_phytoplankton", phyto_pair())
  resolve_dataset_taxon(con, overrides = group_override("ds_taxa_code", "600"))
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "600"], "worms:148899")   # the code names the row: wins
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "316"], "calcofi_phytoplankton:316")
})

test_that("a code-matched override wins over a name-matched one on the same row", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "calcofi_phytoplankton", phyto_pair())
  ov <- rbind(group_override(),                                   # name -> Bacillariophyceae
              group_override("ds_taxa_code", "316"))              # code -> something finer
  ov$worms_id[2] <- 196347L; ov$scientific_name[2] <- "Actinocyclus"; ov$rank[2] <- "Genus"
  resolve_dataset_taxon(con, overrides = ov)
  dt <- read_dt(con)
  expect_equal(dt$taxon_key[dt$ds_taxa_code == "316"], "worms:196347")
  # ...whichever order the registry lists them in
  resolve_dataset_taxon(con, overrides = ov[2:1, ])
  expect_equal(read_dt(con)$taxon_key[read_dt(con)$ds_taxa_code == "316"], "worms:196347")
})

test_that("report_taxon_overrides counts matched / applied / skipped per override row", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "calcofi_phytoplankton", phyto_pair())
  resolve_dataset_taxon(con, overrides = group_override())
  rpt <- report_taxon_overrides(con, group_override(), verbose = FALSE)
  expect_s3_class(rpt, "data.frame")
  expect_equal(nrow(rpt), 1L)
  expect_equal(rpt$dataset_key, "calcofi_phytoplankton")
  expect_equal(rpt$match_column, "ds_common_name")
  expect_equal(rpt$n_matched, 2L)
  expect_equal(rpt$n_applied, 1L)
  expect_equal(rpt$n_skipped, 1L)
  expect_equal(rpt$skipped_codes, "600")
  expect_true(rpt$source_json_known)
  # a code-matched row skips nothing
  rpt <- report_taxon_overrides(con, group_override("ds_taxa_code", "600"), verbose = FALSE)
  expect_equal(c(rpt$n_matched, rpt$n_applied, rpt$n_skipped), c(1L, 1L, 0L))
  # a row that matches nothing is reported as such, not dropped
  rpt <- report_taxon_overrides(con, group_override("ds_taxa_code", "999"), verbose = FALSE)
  expect_equal(c(rpt$n_matched, rpt$n_applied, rpt$n_skipped), c(0L, 0L, 0L))
})

test_that("resolve_dataset_taxon stages the override report and says what it skipped", {
  con <- new_staged_con(); on.exit(close_duckdb(con))
  append_dataset_taxon(con, "calcofi_phytoplankton", phyto_pair())
  expect_message(resolve_dataset_taxon(con, overrides = group_override()), "1 skipped")
  st <- DBI::dbGetQuery(con, "SELECT * FROM _taxon_override_report")
  expect_equal(st$n_skipped, 1L)
  # the staged table and the recomputation agree (the drift guard)
  rpt <- report_taxon_overrides(con, group_override(), verbose = FALSE)
  cols <- c("dataset_key", "match_column", "match_value", "n_matched", "n_applied", "n_skipped")
  expect_equal(st[cols], rpt[cols])
})

test_that("report_taxon_overrides at release flags a shard that predates ds_source_json", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:"); on.exit(close_duckdb(con))
  # an assembled dataset_taxon (no staging marker), one dataset with the column NULL throughout
  DBI::dbWriteTable(con, "dataset_taxon", data.frame(
    ds_taxon_key = c("calcofi_phytoplankton:600", "calcofi_phytoplankton:316", "demo_ds:A"),
    dataset_key  = c("calcofi_phytoplankton", "calcofi_phytoplankton", "demo_ds"),
    taxon_key    = c("worms:148899", "worms:148899", "worms:1"),
    ds_scientific_name = c("Actinocyclus", NA, "Aus"),
    ds_common_name = c("diatom, centric", "diatom, centric", "a"),
    ds_taxa_code = c("600", "316", "A"),
    ds_source_json = c(NA, NA, '{"worms_id":1}'),
    stringsAsFactors = FALSE))
  ov <- rbind(group_override(),
              data.frame(dataset_key = "demo_ds", match_column = "ds_common_name", match_value = "a",
                         worms_id = 2L, itis_id = NA_integer_, scientific_name = "B", rank = "Genus",
                         stringsAsFactors = FALSE))
  rpt <- report_taxon_overrides(con, ov, verbose = FALSE)
  p <- rpt[rpt$dataset_key == "calcofi_phytoplankton", ]
  expect_false(p$source_json_known)
  expect_true(is.na(p$n_skipped))            # unknowable: the shard never said what the source supplied
  expect_equal(p$n_matched, 2L)
  d <- rpt[rpt$dataset_key == "demo_ds", ]
  expect_true(d$source_json_known)
  expect_equal(d$n_skipped, 1L)
  # a match_column the vocabulary cannot expose errors, as it does at ingest
  expect_error(report_taxon_overrides(con, group_override("taxa_typo"), verbose = FALSE), "taxa_typo")
})
