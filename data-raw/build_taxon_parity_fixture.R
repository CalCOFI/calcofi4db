# Phase 0 of the taxon crosswalk plan (`.claude/plans/2026-09-02 Taxon crosswalk …`):
# dump the promoted release's `taxon` / `dataset_taxon` / `taxon_group` tables to
# tests/testthat/fixtures/taxon_parity_v<version>/ as a parity fixture, and compute
# how many `taxon.common_name` values would change under the D5 precedence order.
#
# Read-only: connects to the frozen release via calcofi4r::cc_get_db(), never
# calcofi4db's own DuckDB helpers (this is a snapshot of released bytes, not a
# wrangling database). Not part of the installed package (data-raw/, not R/) —
# kept for reproducibility per the workflows CLAUDE.md "acquisition code must be
# reproducible" rule, and so a later phase can re-run it against a newer version.
#
# Usage: Rscript data-raw/build_taxon_parity_fixture.R
# Requires calcofi4r installed (never installed BY this script — see
# .claude/agents/ws-sonnet-high.md: agents never install packages).

suppressPackageStartupMessages({
  library(calcofi4r)
  library(DBI)
})

VERSION   <- "v2026.08.25"
FIX_DIR   <- file.path("tests/testthat/fixtures", paste0("taxon_parity_", VERSION))
WORKFLOWS <- "../workflows"   # sibling repo checkout, read-only
dir.create(FIX_DIR, showWarnings = FALSE, recursive = TRUE)

write_fixture_csv <- function(df, path) {
  # na = "" per the workflows CLAUDE.md round-trip trap (readr/duckdb default
  # nullstr is "", not the literal string "NA"); integer columns stay integer so
  # they never round-trip with a ".0" suffix.
  utils::write.csv(df, path, row.names = FALSE, na = "")
}

message("Connecting to promoted release ", VERSION, " (read-only, cached)…")
con <- cc_get_db(VERSION)

# --- 1. taxon / dataset_taxon / taxon_group, sorted by primary key ----------

taxon <- dbGetQuery(con, "SELECT * FROM taxon ORDER BY taxon_key")
stopifnot(!anyDuplicated(taxon$taxon_key))
write_fixture_csv(taxon, file.path(FIX_DIR, "taxon.csv"))

dataset_taxon <- dbGetQuery(con, "SELECT * FROM dataset_taxon ORDER BY ds_taxon_key")
stopifnot(!anyDuplicated(dataset_taxon$ds_taxon_key))
write_fixture_csv(dataset_taxon, file.path(FIX_DIR, "dataset_taxon.csv"))

taxon_group <- dbGetQuery(con, "SELECT * FROM taxon_group ORDER BY taxon_group_key, taxon_key")
stopifnot(!anyDuplicated(paste(taxon_group$taxon_group_key, taxon_group$taxon_key)))
write_fixture_csv(taxon_group, file.path(FIX_DIR, "taxon_group.csv"))

message(glue::glue(
  "taxon: {nrow(taxon)} rows; dataset_taxon: {nrow(dataset_taxon)} rows; ",
  "taxon_group: {nrow(taxon_group)} rows"))

# --- 2. D5 common_name precedence — measure what would change ---------------
#
# D5 (the decided plan, Ben Q2): manual > swfsc_ichthyo > WoRMS single > other
# datasets (by dataset_key) > empty. Applied here read-only against the release
# + metadata/taxon_common.csv + dataset_taxon.ds_common_name, to COUNT what
# would change if release_database.qmd's apply_taxon_common() were rewritten to
# this order — no code changes to calcofi4db or the release in Phase 0.
#
# The current registry (metadata/taxon_common.csv, written by
# ensure_taxon_common()) has no literal "manual" tag in `source` — every fetched
# row is stamped source = "worms" regardless of whether the value ended up
# auto-filled (exactly one vernacular) or hand-picked from several. The two are
# told apart by `n_candidates_en`: apply_taxon_common()/ensure_taxon_common()
# only ever auto-fill when n_candidates_en == 1, so any filled row with
# n_candidates_en != 1 (0 or >1) was necessarily a human edit — filed here as
# "manual". This is a faithful reconstruction of the existing rule, not a new
# tag; it becomes the literal `source` value once D5 lands (Phase 1+).

common_path <- file.path(WORKFLOWS, "metadata/taxon_common.csv")
stopifnot(file.exists(common_path))
common <- utils::read.csv(common_path, colClasses = "character", na.strings = c("NA", ""))
common$n_candidates_en <- suppressWarnings(as.integer(common$n_candidates_en))

has_name <- !is.na(common$common_name) & nzchar(common$common_name)
is_worms_single <- has_name & !is.na(common$n_candidates_en) & common$n_candidates_en == 1L &
  !is.na(common$source) & common$source == "worms"
is_manual <- has_name & !is_worms_single

manual_df <- common[is_manual, c("taxon_key", "common_name")]
names(manual_df)[2] <- "manual_common_name"

worms_single_df <- common[is_worms_single, c("taxon_key", "common_name")]
names(worms_single_df)[2] <- "worms_single_common_name"

message(glue::glue(
  "taxon_common.csv: {nrow(common)} rows; {sum(is_manual)} manual (rank 1); ",
  "{sum(is_worms_single)} WoRMS-single (rank 3)"))

# rank 2: swfsc_ichthyo's own vocabulary
ich_df <- dataset_taxon[dataset_taxon$dataset_key == "swfsc_ichthyo" &
                          !is.na(dataset_taxon$ds_common_name) &
                          nzchar(dataset_taxon$ds_common_name),
                        c("taxon_key", "ds_common_name")]
ich_df <- ich_df[!duplicated(ich_df$taxon_key), ]
names(ich_df)[2] <- "ichthyo_common_name"

# rank 4: any OTHER dataset's ds_common_name, first non-empty in dataset_key
# alphabetical order (excludes swfsc_ichthyo, already rank 2)
other <- dataset_taxon[dataset_taxon$dataset_key != "swfsc_ichthyo" &
                         !is.na(dataset_taxon$ds_common_name) &
                         nzchar(dataset_taxon$ds_common_name), ]
other <- other[order(other$taxon_key, other$dataset_key), ]
other_df <- other[!duplicated(other$taxon_key), c("taxon_key", "ds_common_name", "dataset_key")]
names(other_df)[2:3] <- c("other_common_name", "other_dataset_key")

# assemble the COALESCE per taxon_key
m <- merge(taxon[, c("taxon_key", "common_name")], manual_df, by = "taxon_key", all.x = TRUE)
m <- merge(m, ich_df, by = "taxon_key", all.x = TRUE)
m <- merge(m, worms_single_df, by = "taxon_key", all.x = TRUE)
m <- merge(m, other_df, by = "taxon_key", all.x = TRUE)

pick_source <- function(manual, ich, worms1, other, other_ds) {
  if (!is.na(manual))  return(c(manual,  "manual"))
  if (!is.na(ich))     return(c(ich,     "swfsc_ichthyo"))
  if (!is.na(worms1))  return(c(worms1,  "worms_single"))
  if (!is.na(other))   return(c(other,   paste0("other:", other_ds)))
  c(NA_character_, "empty")
}

picked <- t(mapply(pick_source, m$manual_common_name, m$ichthyo_common_name,
                    m$worms_single_common_name, m$other_common_name, m$other_dataset_key))
m$proposed_common_name <- picked[, 1]
m$source_rank <- picked[, 2]

cur <- ifelse(is.na(m$common_name) | !nzchar(m$common_name), NA_character_, m$common_name)
prop <- ifelse(is.na(m$proposed_common_name) | !nzchar(m$proposed_common_name), NA_character_, m$proposed_common_name)
changed <- !((is.na(cur) & is.na(prop)) | (!is.na(cur) & !is.na(prop) & cur == prop))

chg <- data.frame(
  taxon_key       = m$taxon_key[changed],
  current         = cur[changed],
  proposed        = prop[changed],
  source_rank     = m$source_rank[changed],
  stringsAsFactors = FALSE)
chg <- chg[order(chg$taxon_key), ]

write_fixture_csv(chg, file.path(FIX_DIR, "common_name_changes.csv"))

message(glue::glue(
  "D5 common_name order: {sum(changed)} of {nrow(m)} taxa would change ",
  "(current release order vs. manual > swfsc_ichthyo > worms_single > other > empty)"))
print(table(chg$source_rank))

# Diagnostic, not a fixture output: D5 does not specify a tie-break when TWO
# rows of the SAME source dataset resolve to the SAME taxon_key (e.g. a
# taxonomic revision where an old and a new local species code both now
# resolve to one accepted AphiaID). This script breaks such ties by
# `ds_taxon_key` ascending (deterministic, no dataset-specific knowledge). 13
# swfsc_ichthyo taxon_keys have this shape at v2026.08.25; only one
# (worms:126175, "Sebastes crocotulus" #3023 vs "Sebastes" #683) changes the
# picked name relative to the release's own (undocumented) tie order — flagged
# here so Phase 1 states the rule explicitly rather than inheriting whatever
# row order `dplyr::bind_rows()` happens to produce.
ich_all <- dataset_taxon[dataset_taxon$dataset_key == "swfsc_ichthyo", ]
dup_tk <- unique(ich_all$taxon_key[duplicated(ich_all$taxon_key) & !is.na(ich_all$taxon_key)])
message(glue::glue(
  "diagnostic: {length(dup_tk)} swfsc_ichthyo taxon_keys are claimed by >1 local ",
  "species code (tie-break: ds_taxon_key ascending) -- see README caveat"))

# --- 3. README ----------------------------------------------------------------

readme <- glue::glue('
# taxon_parity_{VERSION}

Parity fixture for the taxon crosswalk plan
(`.claude/plans/2026-09-02 Taxon crosswalk — …md`, Phase 0). Snapshot of the
**promoted** release **{VERSION}**, read via `calcofi4r::cc_get_db("{VERSION}")`
(read-only; never calcofi4db\'s wrangling connection). Generated by
`data-raw/build_taxon_parity_fixture.R`.

## Files

| file | rows | primary key | query |
|---|---|---|---|
| `taxon.csv` | {nrow(taxon)} | `taxon_key` | `SELECT * FROM taxon ORDER BY taxon_key` |
| `dataset_taxon.csv` | {nrow(dataset_taxon)} | `ds_taxon_key` | `SELECT * FROM dataset_taxon ORDER BY ds_taxon_key` |
| `taxon_group.csv` | {nrow(taxon_group)} | `(taxon_group_key, taxon_key)` | `SELECT * FROM taxon_group ORDER BY taxon_group_key, taxon_key` |
| `common_name_changes.csv` | {sum(changed)} | `taxon_key` | see below |

Written with `na = ""` (never the literal string `"NA"` — see the round-trip
trap in the workflows `CLAUDE.md`); integer id columns (`worms_id`, `itis_id`,
`gbif_id`, `ncbi_id`, `inat_id`, `rank_order`) are written as R integers, so no
value round-trips with a spurious `.0`.

## D5 common_name precedence — what would change

D5 (the decided plan) orders `taxon.common_name`: **manual choice in
`taxon_common.csv`** > **`swfsc_ichthyo`\'s own vocabulary**
(`dataset_taxon.ds_common_name` where `dataset_key = \'swfsc_ichthyo\'`) >
**WoRMS single vernacular** (`taxon_common.csv` rows auto-filled because WoRMS
returned exactly one English name) > **any other dataset\'s
`ds_common_name`**, first non-empty in `dataset_key` alphabetical order >
**empty**.

`taxon_common.csv` has no literal `"manual"` tag today — every fetched row is
stamped `source = "worms"` whether the value was auto-filled or hand-picked.
`ensure_taxon_common()` only ever auto-fills when `n_candidates_en == 1`, so a
filled row with `n_candidates_en != 1` was necessarily a human edit; that is the
reconstruction used here (`is_manual` = filled & NOT(`source == "worms"` &
`n_candidates_en == 1`)).

**{sum(changed)} of {nrow(m)} taxa ({round(100*sum(changed)/nrow(m), 1)}%) would change**
`common_name` under the D5 order versus what {VERSION} released. Listed
in `common_name_changes.csv` (`taxon_key`, `current`, `proposed`,
`source_rank`) — `source_rank` is one of `manual`, `swfsc_ichthyo`,
`worms_single`, `other:<dataset_key>`, or `empty`.

**Caveat — same-dataset ties.** D5 does not state a tie-break for two rows of
the *same* source dataset resolving to the same `taxon_key` (a taxonomic
revision where an old and new local species code both now resolve to one
accepted AphiaID). This script breaks such ties by `ds_taxon_key` ascending.
13 `swfsc_ichthyo` `taxon_key`s have this shape at {VERSION}; only one
(`worms:126175`, "Sunset rockfish" #3023 vs. "Rockfishes" #683 — genus- and
species-level codes sharing one AphiaID) changes the picked name relative to
the release\'s own undocumented tie order, and is counted above. Phase 1
should state this rule explicitly rather than inherit `dplyr::bind_rows()`
row order.

Regenerate: `Rscript data-raw/build_taxon_parity_fixture.R` from the
`calcofi4db` repo root, with the `workflows` repo checked out as a sibling
(`../workflows`) for `metadata/taxon_common.csv`.
')
writeLines(readme, file.path(FIX_DIR, "README.md"))

message("Wrote fixture to ", FIX_DIR)
