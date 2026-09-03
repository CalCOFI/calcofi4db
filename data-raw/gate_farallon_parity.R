# Phase 1 gate of the taxon crosswalk plan
# (`.claude/plans/2026-09-02 Taxon crosswalk — …md`): stage farallon's CURRENT
# `bird_mammal_species` through append_dataset_taxon() + the generic resolvers
# (ensure_taxon_xref -> ensure_taxon_lineage -> build_taxon_reference ->
# resolve_dataset_taxon -> build_taxon_group) and diff its `dataset_taxon`
# slice against the v2026.08.25 parity fixture key-for-key.
#
# Read-only on the wrangling database; the shared caches under the workflows
# repo's metadata/ are READ AND GROWN (bird WoRMS chains for pass (a) of
# ensure_taxon_lineage() are fetched once, then cached). Nothing else is written.
#
# What the old farallon ARM did that the staged path expresses as registry rows,
# emulated here in memory (Phase 2 writes them into taxon_override.csv):
#   - include_flag: rows the source marks as not-a-taxon are excluded before staging
#   - is_unidentified & is_bird   -> Aves      itis:174371
#   - is_unidentified & is_mammal -> Mammalia  worms:1837
#   - the 37 existing farallon override rows match on `species_code`; the staged
#     vocabulary exposes `ds_taxa_code`, so the column is renamed for the gate
#
# Usage (from the calcofi4db repo root, package loaded, NOT installed):
#   Rscript data-raw/gate_farallon_parity.R [path/to/workflows] [path/to/farallon.duckdb]

suppressPackageStartupMessages({ library(DBI); library(dplyr) })
devtools::load_all(".", quiet = TRUE)

args      <- commandArgs(trailingOnly = TRUE)
WORKFLOWS <- if (length(args) >= 1) args[1] else "../workflows"
DB_PATH   <- if (length(args) >= 2) args[2] else
  file.path(WORKFLOWS, "data/wrangling/farallon_bird-mammal.duckdb")
FIX_DIR   <- "tests/testthat/fixtures/taxon_parity_v2026.08.25"
DS        <- "farallon_bird-mammal"
stopifnot(dir.exists(WORKFLOWS), file.exists(DB_PATH), dir.exists(FIX_DIR))

# --- 1. the current source vocabulary ----------------------------------------
src <- dbConnect(duckdb::duckdb(), DB_PATH, read_only = TRUE)
sp  <- dbGetQuery(src, "SELECT * FROM bird_mammal_species")
dbDisconnect(src, shutdown = TRUE)
message(glue::glue("bird_mammal_species: {nrow(sp)} rows, {sum(sp$include_flag)} with include_flag"))

sp <- sp[sp$include_flag, , drop = FALSE]
d_vocab <- sp |>
  transmute(ds_taxa_code = species_code, ds_scientific_name = scientific_name,
            ds_common_name = common_name, itis_id = as.integer(itis_id))

# --- 2. registries -----------------------------------------------------------
mt_taxon <- read.csv(file.path(WORKFLOWS, "metadata/measurement_taxon.csv"),
                     colClasses = "character", na.strings = "") |>
  mutate(worms_id = as.integer(worms_id), itis_id = as.integer(itis_id),
         bin_value = as.numeric(bin_value)) |>
  filter(dataset_key == DS)
tx_over <- read.csv(file.path(WORKFLOWS, "metadata/taxon_override.csv"),
                    colClasses = "character", na.strings = "") |>
  mutate(worms_id = as.integer(worms_id), itis_id = as.integer(itis_id))
# the staged vocabulary exposes ds_taxa_code, not the arm's species_code
tx_over$match_column[tx_over$dataset_key == DS & tx_over$match_column == "species_code"] <- "ds_taxa_code"
# D3: the unidentified fallbacks the arm hard-coded become override rows
unid <- sp[sp$is_unidentified, , drop = FALSE]
d3 <- data.frame(
  dataset_key = DS, match_column = "ds_taxa_code", match_value = unid$species_code,
  worms_id = ifelse(unid$is_mammal, 1837L, NA_integer_),
  itis_id  = ifelse(unid$is_bird, 174371L, NA_integer_),
  scientific_name = ifelse(unid$is_bird, "Aves", "Mammalia"), rank = "Class",
  review = "FALSE", note = "unidentified -> class (was the farallon arm's hard-coded fallback)",
  stringsAsFactors = FALSE)
tx_over <- bind_rows(tx_over, d3)
message(glue::glue("overrides for {DS}: {sum(tx_over$dataset_key == DS)} rows ({nrow(d3)} unidentified fallbacks)"))

rules <- read_taxon_group_rules(file.path(WORKFLOWS, "metadata/taxon_group.csv"))

# --- 3. the generic path -----------------------------------------------------
con <- get_duckdb_con(":memory:")
n_staged <- append_dataset_taxon(con, DS, d_vocab)
message(glue::glue("staged {n_staged} rows"))

ensure_taxon_xref(con, mt_taxon, tx_over,
                  cache_csv = file.path(WORKFLOWS, "metadata/taxon_xref.csv"))
ensure_taxon_lineage(con, mt_taxon, tx_over,
                     cache_csv = file.path(WORKFLOWS, "metadata/taxon_lineage.csv"))
n_taxon <- build_taxon_reference(con, mt_taxon, tx_over)
n_dt    <- resolve_dataset_taxon(con, mt_taxon, tx_over)
n_grp   <- build_taxon_group(con, rules)
rpt     <- check_dataset_taxon(con, DS, halt = FALSE, verbose = TRUE)
message(glue::glue("taxon {n_taxon}, dataset_taxon {n_dt}, taxon_group {n_grp}, findings {nrow(rpt)}"))

# --- 4. the diff -------------------------------------------------------------
rf <- function(f) read.csv(file.path(FIX_DIR, f), colClasses = "character", na.strings = "")
fix_dt <- rf("dataset_taxon.csv") |> filter(dataset_key == DS) |> arrange(ds_taxon_key)
fix_tx <- rf("taxon.csv")
fix_tg <- rf("taxon_group.csv")

got_dt <- dbGetQuery(con, "SELECT * FROM dataset_taxon WHERE dataset_key = ? ORDER BY ds_taxon_key",
                     params = list(DS))
got_tx <- dbGetQuery(con, "SELECT * FROM taxon")
got_tg <- dbGetQuery(con, "SELECT * FROM taxon_group ORDER BY taxon_group_key, taxon_key")

cat("\n=== dataset_taxon: farallon slice vs fixture ===\n")
cat(glue::glue("rows: got {nrow(got_dt)}, fixture {nrow(fix_dt)}\n"))
only_got <- setdiff(got_dt$ds_taxon_key, fix_dt$ds_taxon_key)
only_fix <- setdiff(fix_dt$ds_taxon_key, got_dt$ds_taxon_key)
cat(glue::glue("ds_taxon_key only in got: {length(only_got)}; only in fixture: {length(only_fix)}\n"))
if (length(only_got)) print(only_got); if (length(only_fix)) print(only_fix)
both <- intersect(got_dt$ds_taxon_key, fix_dt$ds_taxon_key)
g <- got_dt[match(both, got_dt$ds_taxon_key), ]; f <- fix_dt[match(both, fix_dt$ds_taxon_key), ]
for (cl in c("taxon_key", "ds_scientific_name", "ds_common_name", "ds_taxa_code", "dataset_key")) {
  same <- (is.na(g[[cl]]) & is.na(f[[cl]])) | (!is.na(g[[cl]]) & !is.na(f[[cl]]) & g[[cl]] == f[[cl]])
  cat(glue::glue("  {cl}: {sum(same)}/{length(both)} identical\n"))
  if (!all(same)) print(data.frame(ds_taxon_key = both[!same], got = g[[cl]][!same], fixture = f[[cl]][!same]))
}
cat("\nds_source_json (new column):\n")
print(table(ifelse(is.na(got_dt$ds_source_json), "NULL", sub(":.*", "", sub("^\\{\"", "", got_dt$ds_source_json)))))

cat("\n=== taxon: the farallon keys vs fixture ===\n")
keys <- unique(got_dt$taxon_key)
gt <- got_tx[match(keys, got_tx$taxon_key), ]; ft <- fix_tx[match(keys, fix_tx$taxon_key), ]
cat(glue::glue("keys: {length(keys)}; in fixture taxon: {sum(!is.na(ft$taxon_key))}\n"))
for (cl in c("worms_id", "itis_id", "scientific_name", "rank", "class", "parent_taxon_key", "kingdom", "family")) {
  a <- as.character(gt[[cl]]); b <- as.character(ft[[cl]])
  same <- (is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & a == b)
  cat(glue::glue("  {cl}: {sum(same, na.rm = TRUE)}/{length(keys)} identical\n"))
  if (!all(same)) print(head(data.frame(taxon_key = keys[!same], got = a[!same], fixture = b[!same]), 10))
}
# ancestors: does the staged path mint any worms: row for a bird?
worms_aves <- got_tx$taxon_key[grepl("^worms:", got_tx$taxon_key) & got_tx$class %in% "Aves"]
cat(glue::glue("worms:-keyed rows with class Aves in the shard: {length(worms_aves)}\n"))
cat(glue::glue("taxon rows in shard: {nrow(got_tx)} (fixture taxon reachable from farallon keys, incl. ancestors, is not sliced here)\n"))

cat("\n=== taxon_group vs fixture ===\n")
for (k in c("calcofi:seabirds", "calcofi:marine_mammals")) {
  a <- got_tg$taxon_key[got_tg$taxon_group_key == k]; b <- fix_tg$taxon_key[fix_tg$taxon_group_key == k]
  cat(glue::glue("  {k}: got {length(a)}, fixture {length(b)}; only got {length(setdiff(a, b))}, only fixture {length(setdiff(b, a))}\n"))
  if (length(setdiff(b, a))) print(fix_tx[match(setdiff(b, a), fix_tx$taxon_key), c("taxon_key", "scientific_name", "class")])
  if (length(setdiff(a, b))) print(got_tx[match(setdiff(a, b), got_tx$taxon_key), c("taxon_key", "scientific_name", "class")])
}

cat("\n=== check_dataset_taxon findings ===\n"); print(rpt)
close_duckdb(con)
