# the release concatenates per-dataset core shards instead of re-deriving the
# core. These pin the two things that go wrong when you union shards: surrogate
# ids collide (each ingest numbers from 1) and the same taxon appears in several
# shards and must collapse to one row with its best-sourced fields.

# write a tiny two-dataset shard tree under a temp root
make_shard_root <- function() {
  root <- withr::local_tempdir(.local_envir = parent.frame())
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  for (d in c("ds_a", "ds_b")) dir.create(file.path(root, "data/parquet", d), recursive = TRUE)
  p <- function(d, t) file.path(root, "data/parquet", d, paste0(t, ".parquet"))

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

  # worms:2 is in both shards; ds_a (higher priority) has the rank, ds_b the name
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'worms:1' AS taxon_key, 'Calanus' AS scientific_name,
           'genus' AS taxon_rank, NULL::VARCHAR AS family
    UNION ALL SELECT 'worms:2', NULL::VARCHAR, 'species', 'Euphausiidae'
  ) TO '{p('ds_a','taxon')}' (FORMAT PARQUET)"))
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'worms:2' AS taxon_key, 'Euphausia pacifica' AS scientific_name,
           NULL::VARCHAR AS taxon_rank, NULL::VARCHAR AS family
  ) TO '{p('ds_b','taxon')}' (FORMAT PARQUET)"))
  root
}

test_that("obs shards union with globally renumbered obs_id", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  n <- assemble_core_table(con, "obs", root, id_col = "obs_id",
                           order_by = "dataset_key, measurement_value")
  expect_equal(n, 3L)

  got <- DBI::dbGetQuery(con, "SELECT obs_id, dataset_key, measurement_value FROM obs ORDER BY obs_id")
  # 1,2 from ds_a and 1 from ds_b -> 1,2,3, not 1,2,1
  expect_equal(got$obs_id, 1:3)
  expect_equal(anyDuplicated(got$obs_id), 0L)
  expect_equal(got$dataset_key, c("ds_a", "ds_a", "ds_b"))
})

test_that("assemble_core() rejects a sample_key colliding across shards", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  # break the namespacing guarantee: ds_b claims ds_a's key
  dup <- file.path(root, "data/parquet/ds_b/sample.parquet")
  DBI::dbExecute(con, glue::glue("COPY (
    SELECT 'ds_a:tow:1' sample_key, 'tow' sample_type, 'ds_b' dataset_key
  ) TO '{dup}' (FORMAT PARQUET)"))

  expect_error(assemble_core(con, root, supplemental = FALSE),
               "duplicate sample_key")
})

test_that("taxon shards collapse on taxon_key, coalescing by source priority", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("withr")
  root <- make_shard_root()
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  n <- merge_taxon_shards(con, root, priority = c("ds_a", "ds_b"))
  expect_equal(n, 2L)

  got <- DBI::dbGetQuery(con,
    "SELECT taxon_key, scientific_name, taxon_rank, family FROM taxon ORDER BY taxon_key")
  expect_equal(got$taxon_key, c("worms:1", "worms:2"))
  # worms:2 appears in both shards -> ONE row, taking each field from the
  # highest-priority shard that actually has it
  expect_equal(got$taxon_rank[2], "species")                  # from ds_a
  expect_equal(got$scientific_name[2], "Euphausia pacifica")  # ds_a is NULL -> ds_b
  expect_equal(got$family[2], "Euphausiidae")           # from ds_a
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
  dir.create(file.path(root, "data/parquet/ds_a"), recursive = TRUE)
  dir.create(file.path(root, "data/parquet/ds_b/obs/cruise_key=x"), recursive = TRUE)
  file.create(file.path(root, "data/parquet/ds_a/obs.parquet"))

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
  # coordinates. assemble_core() renumbers obs_id and orders by core columns, so
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
