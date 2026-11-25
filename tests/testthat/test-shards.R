# the release concatenates per-dataset core shards instead of re-deriving the
# core. These pin the two things that go wrong when you union shards: surrogate
# ids collide (each ingest numbers from 1) and the same taxon appears in several
# shards and must collapse to one row with its best-sourced fields.

# write a tiny two-dataset shard tree under a temp staging root.
# The bulk parquet lives OUTSIDE the repo now, so the fixtures sit where
# cc_stage_path("parquet") resolves rather than under root/data/parquet — this
# exercises the production default instead of a layout nothing uses.
make_shard_root <- function() {
  root <- withr::local_tempdir(.local_envir = parent.frame())
  withr::local_envvar(c(CALCOFI_STAGE_DIR = root), .local_envir = parent.frame())
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  for (d in c("ds_a", "ds_b")) dir.create(file.path(root, "parquet", d), recursive = TRUE)
  p <- function(d, t) file.path(root, "parquet", d, paste0(t, ".parquet"))

  # the id-block registry: each dataset's own run of numbers (ds_a block 1, ds_b block 2)
  dir.create(file.path(root, "metadata"))
  writeLines(c("dataset_key,id_block,added", "ds_a,1,2026-11-26", "ds_b,2,2026-11-26"),
             file.path(root, "metadata", "id_block.csv"))

  # both shards number obs_id from 1 — the collision the release must resolve
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 1 obs_id, 'ds_a' dataset_key, 'ds_a:tow:1' sample_key, 'g1' grid_key,
           'abundance' measurement_type, 10.0 measurement_value, 'worms:1' taxon_key
    UNION ALL SELECT 2, 'ds_a', 'ds_a:tow:1', 'g1', 'abundance', 20.0, 'worms:2'
  ) TO '{p('ds_a','obs')}' (FORMAT PARQUET)"))
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 1 obs_id, 'ds_b' dataset_key, 'ds_b:tow:1' sample_key, 'g2' grid_key,
           'abundance' measurement_type, 30.0 measurement_value, 'worms:2' taxon_key
  ) TO '{p('ds_b','obs')}' (FORMAT PARQUET)"))

  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'ds_a:tow:1' sample_key, 'tow' sample_type, 'ds_a' dataset_key
  ) TO '{p('ds_a','sample')}' (FORMAT PARQUET)"))
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'ds_b:tow:1' sample_key, 'tow' sample_type, 'ds_b' dataset_key
  ) TO '{p('ds_b','sample')}' (FORMAT PARQUET)"))

  # worms:2 is in both shards; ds_a (higher priority) has the rank, ds_b the name.
  # Both also carry a DIFFERENT provenance note, which must survive the merge —
  # `notes` is append-only, so priority must not silently drop one.
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'worms:1' AS taxon_key, 'Calanus' AS scientific_name,
           'genus' AS taxon_rank, NULL::VARCHAR AS family, NULL::VARCHAR AS notes
    UNION ALL SELECT 'worms:2', NULL::VARCHAR, 'species', 'Euphausiidae', '2026-08-05: from ds_a'
  ) TO '{p('ds_a','taxon')}' (FORMAT PARQUET)"))
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'worms:2' AS taxon_key, 'Euphausia pacifica' AS scientific_name,
           NULL::VARCHAR AS taxon_rank, NULL::VARCHAR AS family,
           '2026-08-05: from ds_b' AS notes
  ) TO '{p('ds_b','taxon')}' (FORMAT PARQUET)"))
  root
}

test_that("obs shards union with obs_id numbered inside each dataset's block", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  n <- assemble_core_table(con, "obs", root, id_col = "obs_id",
                           order_by = "dataset_key, measurement_value")
  expect_equal(n, 3L)

  got <- DBI::dbGetQuery(con, "SELECT obs_id, dataset_key, measurement_value FROM obs ORDER BY obs_id")
  # 1,2 from ds_a and 1 from ds_b -> block 1: 1,2 and block 2: 1, never 1,2,1 or 1,2,3
  expect_equal(got$obs_id, c(1e10 + 1:2, 2e10 + 1))
  expect_equal(got$measurement_value, c(10, 20, 30))
  expect_equal(got$dataset_key, c("ds_a", "ds_a", "ds_b"))
  expect_true(all(got$obs_id < 2^53))
  expect_equal(DBI::dbGetQuery(con, "SELECT typeof(obs_id) t FROM obs LIMIT 1")$t, "BIGINT")
  # the column order of the table is the shards', the id still first
  expect_equal(DBI::dbListFields(con, "obs")[1:2], c("obs_id", "dataset_key"))
})

# regression, v2026.11.25: 17 new CTD casts renumbered every row of fifteen other datasets
# (swfsc_ichthyo's 487,616 rows were identical but for obs_id + 2,354), so each release
# re-uploaded every dataset's obs object and would have rebuilt every download zip
test_that("a dataset gaining rows leaves every other dataset's ids as they were", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  ids <- function() {
    assemble_core_table(con, "obs", root, id_col = "obs_id", order_by = "measurement_value")
    DBI::dbGetQuery(con, "SELECT dataset_key, taxon_key, measurement_value, obs_id FROM obs ORDER BY obs_id")
  }
  before <- ids()
  # ds_a sorts before ds_b and gains two rows, one of them ahead of its existing rows
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT * FROM read_parquet('{file.path(root, 'parquet', 'ds_a', 'obs.parquet')}')
    UNION ALL SELECT 7, 'ds_a', 'ds_a:tow:2', 'g1', 'abundance', 5.0, 'worms:1'
    UNION ALL SELECT 8, 'ds_a', 'ds_a:tow:2', 'g1', 'abundance', 50.0, 'worms:2'
  ) TO '{file.path(root, 'parquet', 'ds_a', 'obs2.parquet')}' (FORMAT PARQUET)"))
  file.rename(file.path(root, "parquet", "ds_a", "obs2.parquet"),
              file.path(root, "parquet", "ds_a", "obs.parquet"))
  after <- ids()
  expect_equal(after[after$dataset_key == "ds_b", ], before[before$dataset_key == "ds_b", ],
               ignore_attr = TRUE)
  expect_equal(after$obs_id[after$dataset_key == "ds_a"], 1e10 + 1:4)
  # and a dataset leaving the release (or a new one arriving) moves nobody either
  assemble_core_table(con, "obs", root, id_col = "obs_id", order_by = "measurement_value",
                      exclude = "ds_a")
  alone <- DBI::dbGetQuery(con, "SELECT obs_id FROM obs")$obs_id
  expect_equal(alone, before$obs_id[before$dataset_key == "ds_b"])
})

test_that("the ids do not depend on the order the rows or the shards arrive in", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  f <- file.path(root, "parquet", "ds_a", "obs.parquet")
  # ties under the leading order (same sample, grid, type): the remaining columns break them
  get <- function() {
    assemble_core_table(con, "obs", root, id_col = "obs_id", order_by = "sample_key, measurement_type")
    DBI::dbGetQuery(con, "SELECT obs_id, taxon_key, measurement_value FROM obs ORDER BY obs_id")
  }
  a <- get()
  DBI::dbExecute(con, glue::glue("COPY (SELECT * FROM read_parquet('{f}') ORDER BY measurement_value DESC)
                                  TO '{f}.tmp' (FORMAT PARQUET)"))
  file.rename(paste0(f, ".tmp"), f)
  b <- get()
  expect_equal(b, a)
  # the shard's own obs_id is never read: renumbering it at ingest changes nothing
  DBI::dbExecute(con, glue::glue("COPY (SELECT * REPLACE (obs_id + 900 AS obs_id) FROM read_parquet('{f}'))
                                  TO '{f}.tmp' (FORMAT PARQUET)"))
  file.rename(paste0(f, ".tmp"), f)
  expect_equal(get(), a)
})

test_that("a dataset with no id block stops the assembly and names the next number", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  writeLines(c("dataset_key,id_block,added", "ds_a,4,2026-11-26"),
             file.path(root, "metadata", "id_block.csv"))
  expect_error(assemble_core_table(con, "obs", root, id_col = "obs_id"),
               "no id block for ds_b.*\\(5 onward\\)")
  unlink(file.path(root, "metadata", "id_block.csv"))
  expect_error(assemble_core_table(con, "obs", root, id_col = "obs_id"), "no id-block registry")
  # a table numbered without ids needs no registry
  expect_equal(assemble_core_table(con, "sample", root), 2)
})

test_that("read_id_blocks() refuses a shared, reused or malformed block", {
  d <- withr::local_tempdir()
  f <- file.path(d, "id_block.csv")
  w <- function(...) writeLines(c("dataset_key,id_block,added", ...), f)
  w("a,2,2026-11-26", "b,1,2026-11-26")
  expect_equal(read_id_blocks(path = f)$dataset_key, c("b", "a"))
  w("a,1,x", "b,1,x");        expect_error(read_id_blocks(path = f), "appears twice")
  w("a,1,x", "a,2,x");        expect_error(read_id_blocks(path = f), "appears twice")
  w("a,0,x");                 expect_error(read_id_blocks(path = f), "whole number from 1 to 900000")
  w("a,1.5,x");               expect_error(read_id_blocks(path = f), "whole number")
  w("a,900001,x");            expect_error(read_id_blocks(path = f), "whole number")
  # the largest id any block can hold is a JavaScript safe integer
  expect_lt(900000 * CC_ID_BLOCK_SIZE + CC_ID_BLOCK_SIZE - 1, 2^53)
})

test_that("check_id_blocks() passes block-numbered ids and names each other way of numbering", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))
  blocks <- read_id_blocks(root)
  assemble_core_table(con, "obs", root, id_col = "obs_id")
  ok <- check_id_blocks(con, blocks)
  expect_equal(ok$table, c("obs", "obs")); expect_true(all(ok$ok))
  expect_equal(ok$n, c(2, 1)); expect_equal(ok$id_min, c(1e10 + 1, 2e10 + 1))
  # one row number over the whole table, as through v2026.11.25
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE obs AS
    SELECT * REPLACE (ROW_NUMBER() OVER (ORDER BY dataset_key, measurement_value) AS obs_id) FROM obs")
  bad <- check_id_blocks(con, blocks)
  expect_false(any(bad$ok))
  expect_match(bad$why[1], "do not start at the block")
  # a gap, a repeat and an unregistered dataset each say so
  assemble_core_table(con, "obs", root, id_col = "obs_id")
  DBI::dbExecute(con, "UPDATE obs SET obs_id = obs_id + 1 WHERE obs_id = 10000000002")
  expect_match(check_id_blocks(con, blocks)$why[1], "not the block's first n numbers")
  DBI::dbExecute(con, "UPDATE obs SET obs_id = 10000000001 WHERE dataset_key = 'ds_a'")
  expect_match(check_id_blocks(con, blocks)$why[1], "repeat")
  expect_match(check_id_blocks(con, blocks[1, ])$why[2], "no block")
  # a table with no id column is not this check's business
  expect_equal(nrow(check_id_blocks(con, blocks, tables = c(sample = "sample_id"))), 0)
})

test_that("assemble_core() rejects a sample_key colliding across shards", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # break the namespacing guarantee: ds_b claims ds_a's key
  dup <- file.path(root, "parquet/ds_b/sample.parquet")
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'ds_a:tow:1' sample_key, 'tow' sample_type, 'ds_b' dataset_key
  ) TO '{dup}' (FORMAT PARQUET)"))

  expect_error(assemble_core(con, root, supplemental = FALSE),
               "duplicate sample_key")
})

test_that("taxon shards collapse on taxon_key, coalescing in dataset order — no priority list", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # the hard-coded dataset priority is gone (taxon plan D5): every shard's
  # authority fields come from the same cached lineage, so a disagreement is
  # settled deterministically by dataset directory name, not by a list an
  # added dataset must be appended to
  expect_error(merge_taxon_shards(con, root, priority = c("ds_a", "ds_b")), "unused argument")
  n <- merge_taxon_shards(con, root)
  expect_equal(n, 2L)

  got <- DBI::dbGetQuery(con,
    "SELECT taxon_key, scientific_name, taxon_rank, family FROM taxon ORDER BY taxon_key")
  expect_equal(got$taxon_key, c("worms:1", "worms:2"))
  # worms:2 appears in both shards -> ONE row, taking each field from the
  # first shard (by name) that actually has it
  expect_equal(got$taxon_rank[2], "species")                  # from ds_a
  expect_equal(got$scientific_name[2], "Euphausia pacifica")  # ds_a is NULL -> ds_b
  expect_equal(got$family[2], "Euphausiidae")           # from ds_a

  # append-only provenance: BOTH shards' notes survive, neither wins
  nt <- DBI::dbGetQuery(con, "SELECT notes FROM taxon WHERE taxon_key = 'worms:2'")$notes
  expect_match(nt, "from ds_a")
  expect_match(nt, "from ds_b")
})

test_that("a missing shard is reported, not fatal", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  expect_message(
    n <- assemble_core_table(con, "obs_attribute", root),
    "no shards found")
  expect_equal(n, 0L)
})

test_that("core_shard_paths() finds both file and hive-partitioned shards", {
  skip_if_not_installed("withr")
  root <- withr::local_tempdir()
  withr::local_envvar(c(CALCOFI_STAGE_DIR = root))
  dir.create(file.path(root, "parquet/ds_a"), recursive = TRUE)
  dir.create(file.path(root, "parquet/ds_b/obs/cruise_key=x"), recursive = TRUE)
  file.create(file.path(root, "parquet/ds_a/obs.parquet"))

  p <- core_shard_paths("obs", root)
  expect_length(p, 2L)
  expect_true(any(grepl("ds_a/obs\\.parquet$", p)))
  expect_true(any(grepl("ds_b/obs/\\*\\*/\\*\\.parquet$", p)))
})

# ---- supplemental discovery --------------------------------------------------

qmd_with <- function(dir, name, provider, dataset, owned) {
  writeLines(c(
    "---", "title: t", "calcofi:",
    glue::glue("  provider: {provider}"),
    glue::glue("  dataset: {dataset}"),
    "  tables_owned:", owned, "---", "", "body"),
    file.path(dir, name))
}

test_that("supplemental tables are discovered from the ingest YAML", {
  d <- withr::local_tempdir()
  qmd_with(d, "ingest_calcofi_ctd-cast.qmd", "calcofi", "ctd-cast", c(
    "    - {table: obs, shared: true}",
    "    - {table: obs_ctd_full, supplemental: true}"))
  qmd_with(d, "ingest_calcofi_mets.qmd", "calcofi", "mets", c(
    "    - {table: obs, shared: true}",
    "    - {table: obs_mets_full, supplemental: true}"))
  qmd_with(d, "ingest_swfsc_ichthyo.qmd", "swfsc", "ichthyo", c(
    "    - {table: obs, shared: true}"))

  expect_equal(supplemental_core_tables(d), c("obs_ctd_full", "obs_mets_full"))
  expect_equal(supplemental_core_tables(d, FALSE), character())
  expect_equal(supplemental_core_tables(d, "obs_only_this"), "obs_only_this")
})

test_that("a non-obs-shaped supplemental is not offered to the assembler", {
  # calcofi_mets used to declare the raw mets_measurement here: no obs_id, no
  # coordinates. assemble_core() numbers obs_id and orders by core columns, so
  # handing it such a table would fail — or worse, publish something unusable.
  d <- withr::local_tempdir()
  qmd_with(d, "ingest_calcofi_mets.qmd", "calcofi", "mets", c(
    "    - {table: obs, shared: true}",
    "    - {table: mets_measurement, supplemental: true}"))
  expect_equal(supplemental_core_tables(d), character())
})

test_that("no ingests means no supplemental tables, not an error", {
  expect_equal(supplemental_core_tables(withr::local_tempdir()), character())
})
