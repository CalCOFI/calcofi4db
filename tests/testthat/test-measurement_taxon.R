# `_measurement_taxon` plumbing + the core shard set.
#
# The per-dataset grain rules these used to sit beside (bird_mammal summing across
# behaviors, phyllosoma splitting total from stage bins, cufes decomposing the
# taxon out of the type name, euphausiids keeping species x life-stage, phyto's
# NULL grid_key) now live in — and are asserted by — the ingest notebook that owns
# each dataset. Re-testing them here would mean a second copy of every projection
# in the package, which is exactly the duplication that let the release and
# package arms drift apart. What is left here is the generic machinery.

test_that("an unregistered raw type projects zero rows, not an error", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE cufes_sample AS
    SELECT 1::INTEGER sample_id, 'st1-ln1'::VARCHAR grid_key, '2015-04-33RR'::VARCHAR cruise_key,
           33.0::DOUBLE latitude, -119.0::DOUBLE longitude,
           TIMESTAMP '2015-04-05 12:00:00' datetime_start_utc")
  DBI::dbExecute(con, "CREATE TABLE cufes_measurement AS
    SELECT 1::INTEGER cufes_measurement_id, 1::INTEGER sample_id,
           'sardine_eggs'::VARCHAR measurement_type, 12.0::DOUBLE measurement_value,
           NULL::VARCHAR measurement_qual")

  # an absent registry yields an EMPTY table, so the INNER join drops every row
  # rather than raising a catalog error
  ensure_measurement_taxon(con, NULL, dataset_key = "swfsc_cufes")
  n_sample <- append_sample(con, sample_arm_self(
    "swfsc_cufes", "cufes_sample", "sample_id", "underway"))
  n_obs <- append_obs(con, "
    SELECT 'bio', 'swfsc_cufes', 'swfsc_cufes:underway:' || CAST(c.sample_id AS VARCHAR),
           c.grid_key, c.cruise_key, c.latitude, c.longitude,
           CAST(c.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, 0::DOUBLE,
           mx.taxon_key, mx.life_stage, mx.measurement_type, m.measurement_value,
           m.measurement_qual, NULL::DOUBLE
    FROM cufes_measurement m JOIN cufes_sample c USING (sample_id)
    JOIN _measurement_taxon mx ON mx.raw_measurement_type = m.measurement_type
                              AND mx.target = 'obs'
    WHERE c.grid_key IS NOT NULL")

  expect_equal(n_sample, 1L)
  expect_equal(n_obs, 0L)
})

test_that("_measurement_taxon is restricted to the emitting dataset", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  mt <- data.frame(
    dataset_key = c("swfsc_cufes", "calcofi_phyllosoma"),
    raw_measurement_type = c("sardine_eggs", "total_phyllosoma"),
    target = "obs", measurement_type = "abundance",
    taxon_scientific_name = c("Sardinops sagax", "Panulirus interruptus"),
    worms_id = c(127023L, 396116L), itis_id = NA_integer_,
    life_stage = c("egg", "phyllosoma"), bin_value = NA_real_,
    stringsAsFactors = FALSE)

  ensure_measurement_taxon(con, mt, dataset_key = "swfsc_cufes")
  got <- DBI::dbGetQuery(con,
    "SELECT dataset_key, raw_measurement_type, taxon_key FROM _measurement_taxon")
  expect_equal(nrow(got), 1L)
  expect_equal(got$dataset_key, "swfsc_cufes")
  expect_equal(got$taxon_key, "worms:127023")
})

test_that("ensure_measurement_taxon derives taxon_key, which the raw CSV lacks", {
  # the registry CSV has worms_id/itis_id but NO taxon_key column, so writing it
  # straight to the connection leaves every `mx.taxon_key` reference a binder
  # error — and hand-rolling 'worms:' || worms_id mis-keys ITIS-resolved taxa.
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  mt <- data.frame(
    dataset_key = "farallon_bird-mammal",
    raw_measurement_type = c("unid_bird", "unid_mammal"),
    target = "obs", measurement_type = "count",
    taxon_scientific_name = c("Aves", "Mammalia"),
    worms_id = c(NA_integer_, 1837L), itis_id = c(174371L, NA_integer_),
    life_stage = NA_character_, bin_value = NA_real_, stringsAsFactors = FALSE)

  expect_false("taxon_key" %in% names(mt))
  ensure_measurement_taxon(con, mt, dataset_key = "farallon_bird-mammal")
  got <- DBI::dbGetQuery(con,
    "SELECT raw_measurement_type, taxon_key FROM _measurement_taxon
     ORDER BY raw_measurement_type")
  # worms: where an AphiaID resolves; a TSN keys itis: only once the lineage
  # says the taxon is class Aves (taxon plan D2) — before that it has no key
  expect_equal(got$taxon_key, c(NA_character_, "worms:1837"))

  DBI::dbExecute(con, "CREATE TABLE _taxon_lineage_flat AS
    SELECT 174371 requested_id, 'ITIS' authority, 'Class' AS \"rank\", 914181 parent_id,
           'Aves' scientific_name, 'Animalia' kingdom, 'Chordata' phylum,
           'Aves' AS \"class\", NULL::VARCHAR order_taxon, NULL::VARCHAR AS \"family\"")
  ensure_measurement_taxon(con, mt, dataset_key = "farallon_bird-mammal")
  got <- DBI::dbGetQuery(con,
    "SELECT raw_measurement_type, taxon_key FROM _measurement_taxon
     ORDER BY raw_measurement_type")
  expect_equal(got$taxon_key, c("itis:174371", "worms:1837"))
})

test_that("core_output_tables() lists only non-empty core shards", {
  skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  on.exit(close_duckdb(con))

  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_bottle AS
    SELECT 1::INTEGER bottle_id, '2004-11-33RR'::VARCHAR cruise_key, 'st1-ln1'::VARCHAR grid_key,
           '090.0 060.0'::VARCHAR site_key, 33.4::DOUBLE latitude, -118.1::DOUBLE longitude,
           TIMESTAMP '2004-11-02 18:20:00' datetime_utc, 20.0::DOUBLE depth_m")
  DBI::dbExecute(con, "CREATE TABLE picoplankton_bacteria_measurement AS
    SELECT 1::INTEGER measurement_id, 1::INTEGER bottle_id,
           'synechococcus'::VARCHAR measurement_type, 41000.0::DOUBLE measurement_value")

  append_sample(con, sample_arm_self(
    "cce-lter_picoplankton-bacteria", "picoplankton_bacteria_bottle", "bottle_id",
    "bottle", dt_col = "datetime_utc", site_expr = "site_key",
    depth_min = "depth_m", depth_max = "depth_m"))
  append_obs(con, "
    SELECT 'env', 'cce-lter_picoplankton-bacteria',
           'cce-lter_picoplankton-bacteria:bottle:' || CAST(b.bottle_id AS VARCHAR),
           b.grid_key, b.cruise_key, b.latitude, b.longitude,
           CAST(b.datetime_utc AS TIMESTAMP), b.depth_m, b.depth_m,
           NULL::VARCHAR, NULL::VARCHAR, m.measurement_type, m.measurement_value,
           NULL::VARCHAR, NULL::DOUBLE
    FROM picoplankton_bacteria_measurement m
    JOIN picoplankton_bacteria_bottle b USING (bottle_id)
    WHERE b.grid_key IS NOT NULL")

  tbls <- core_output_tables(con)
  expect_true(all(c("sample", "obs") %in% tbls))
  # env-only dataset: no attribution, no effort, no taxa -> no empty parquet files
  expect_false("obs_attribute" %in% tbls)
  expect_false("sample_measurement" %in% tbls)
  expect_false("taxon" %in% tbls)
})
