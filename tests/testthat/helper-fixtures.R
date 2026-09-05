# tiny synthetic in-memory DuckDB fixtures for the core-model (model.R) tests.
# one site -> one tow -> three nets; one larva species with an abundance headline
# plus a stage distribution (sums to the abundance) and a size distribution (a
# measured subsample, sums <= abundance).

new_ichthyo_fixture <- function() {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  load_duckdb_extension(con, "spatial")

  DBI::dbExecute(con, "CREATE TABLE site AS
    SELECT 'S1'::VARCHAR site_uuid, 'st1-ln1'::VARCHAR grid_key,
           '090.0 060.0'::VARCHAR site_key, 3::SMALLINT order_occ,
           '2020-01-NODC'::VARCHAR cruise_key, 32.0::DOUBLE latitude, -120.0::DOUBLE longitude")
  DBI::dbExecute(con, "CREATE TABLE tow AS
    SELECT 'T1'::VARCHAR tow_uuid, 'S1'::VARCHAR site_uuid,
           TIMESTAMP '2020-01-01 10:00:00' datetime_start_utc,
           'CB'::VARCHAR tow_type_key")
  DBI::dbExecute(con, "CREATE TABLE net AS
    SELECT 'N1'::VARCHAR net_uuid, 'T1'::VARCHAR tow_uuid, 100.0::DOUBLE volume_sampled,
           5.0::DOUBLE standard_haul_factor, 0.5::DOUBLE prop_sorted,
           1.0::DOUBLE smallplankton, 2.0::DOUBLE totalplankton
    UNION ALL SELECT 'N2','T1',110.0,5.0,0.5,1.0,2.0
    UNION ALL SELECT 'N3','T1',120.0,5.0,0.5,1.0,2.0")
  # base (measurement_type NULL) tally = the abundance headline (10);
  # two stage bins (4 + 6 = 10); two size bins (3 + 2 = 5, a subsample)
  DBI::dbExecute(con, "CREATE TABLE ichthyo AS
    SELECT 'N1'::VARCHAR net_uuid, 1::SMALLINT species_id, 'larva'::VARCHAR life_stage,
           NULL::VARCHAR measurement_type, NULL::DOUBLE measurement_value, 10::INTEGER tally
    UNION ALL SELECT 'N1',1,'larva','stage',2.0,4
    UNION ALL SELECT 'N1',1,'larva','stage',3.0,6
    UNION ALL SELECT 'N1',1,'larva','size',12.0,3
    UNION ALL SELECT 'N1',1,'larva','size',15.0,2")
  DBI::dbExecute(con, "CREATE TABLE lookup AS
    SELECT 'larva_stage'::VARCHAR lookup_type, 2::INTEGER lookup_num, 'preflexion'::VARCHAR description
    UNION ALL SELECT 'larva_stage',3,'flexion'")
  con
}

# The per-dataset projections below are FIXTURES, deliberately duplicated from
# ingest_swfsc_ichthyo.qmd / ingest_calcofi_bottle.qmd. They do not live in the
# package: each dataset's projection is owned by the notebook that owns the
# dataset (see the header of R/model.R for why). What these tests pin is the
# GENERIC machinery — append_sample()/append_obs()/append_obs_attribute()/
# append_sample_measurement()/compat_event_sql()/compat_measurement_sql() — with
# a realistic projection driving it. Each notebook asserts its own grain rules.

# the three chained ichthyo `sample` arms: site (root) -> tow -> net (leaf)
ich_sample_sql <- c(
  site = "
    SELECT 'swfsc_ichthyo:site:' || CAST(s.site_uuid AS VARCHAR), 'site',
           NULL::VARCHAR, 'swfsc_ichthyo:site:' || CAST(s.site_uuid AS VARCHAR),
           'swfsc_ichthyo', s.grid_key, s.site_key, s.cruise_key,
           CAST(s.order_occ AS INTEGER), s.latitude, s.longitude,
           CAST(td.dt AS TIMESTAMP), NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR
    FROM site s
    LEFT JOIN (SELECT site_uuid, min(datetime_start_utc) AS dt FROM tow GROUP BY 1) td
           ON td.site_uuid = s.site_uuid",
  tow = "
    SELECT 'swfsc_ichthyo:tow:' || CAST(t.tow_uuid AS VARCHAR), 'tow',
           'swfsc_ichthyo:site:' || CAST(t.site_uuid AS VARCHAR),
           'swfsc_ichthyo:site:' || CAST(t.site_uuid AS VARCHAR),
           'swfsc_ichthyo', s.grid_key, s.site_key, s.cruise_key,
           CAST(s.order_occ AS INTEGER), s.latitude, s.longitude,
           CAST(t.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, NULL::DOUBLE,
           t.tow_type_key
    FROM tow t JOIN site s USING (site_uuid)",
  net = "
    SELECT 'swfsc_ichthyo:net:' || CAST(n.net_uuid AS VARCHAR), 'net',
           'swfsc_ichthyo:tow:' || CAST(n.tow_uuid AS VARCHAR),
           'swfsc_ichthyo:site:' || CAST(t.site_uuid AS VARCHAR),
           'swfsc_ichthyo', s.grid_key, s.site_key, s.cruise_key,
           CAST(s.order_occ AS INTEGER), s.latitude, s.longitude,
           CAST(t.datetime_start_utc AS TIMESTAMP), 0::DOUBLE, NULL::DOUBLE,
           t.tow_type_key
    FROM net n JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)")

# build the ichthyo `sample` shard, one append per event level
build_ich_sample <- function(con) {
  for (arm in ich_sample_sql) append_sample(con, arm)
  invisible(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM sample")$n)
}

# the ichthyo compat VIEW specs, expressed through the exported generic shape
ich_compat_specs <- function(sample_tbl = "sample") list(
  site = compat_event_sql("swfsc_ichthyo", "site", "site_uuid", NULL,
    c(order_occ = "order_occ", longitude = "longitude", latitude = "latitude",
      cruise_key = "cruise_key", geom = "geom", grid_key = "grid_key",
      site_key = "site_key"), sample_tbl = sample_tbl),
  tow = compat_event_sql("swfsc_ichthyo", "tow", "tow_uuid", "site_uuid",
    c(tow_type_key = "tow_type", datetime_start_utc = "datetime"),
    sample_tbl = sample_tbl),
  net = compat_event_sql("swfsc_ichthyo", "net", "net_uuid", "tow_uuid", character(),
    c(standard_haul_factor = "std_haul_factor", volume_sampled = "volume_sampled",
      prop_sorted = "prop_sorted", smallplankton = "small_plankton_biomass",
      totalplankton = "total_plankton_biomass"), sample_tbl = sample_tbl))

# replace tables/views with VIEWs over the core — the loop each ingest notebook
# runs after its projection
make_compat_views <- function(con, specs) {
  for (nm in names(specs)) {
    t <- DBI::dbGetQuery(con, glue::glue(
      "SELECT table_type FROM information_schema.tables WHERE table_name = '{nm}'"))
    if (nrow(t)) {
      kind <- if (grepl("VIEW", t$table_type[1], ignore.case = TRUE)) "VIEW" else "TABLE"
      DBI::dbExecute(con, glue::glue('DROP {kind} IF EXISTS "{nm}"'))
    }
    DBI::dbExecute(con, glue::glue("CREATE OR REPLACE VIEW {nm} AS {specs[[nm]]}"))
  }
  invisible(names(specs))
}

# the ichthyo obs / obs_attribute / sample_measurement projections (kept here so the
# tests exercise the exact SQL the ingest/release will use)
ich_obs_sql <- "
  SELECT 'bio' realm, 'swfsc_ichthyo' dataset_key,
         'swfsc_ichthyo:net:' || CAST(i.net_uuid AS VARCHAR) sample_key,
         s.grid_key, s.cruise_key, s.latitude, s.longitude,
         CAST(t.datetime_start_utc AS TIMESTAMP) datetime,
         0::DOUBLE depth_min_m, NULL::DOUBLE depth_max_m,
         CAST(i.species_id AS VARCHAR) taxon_key, i.life_stage,
         'abundance' measurement_type, CAST(i.tally AS DOUBLE) measurement_value,
         NULL::VARCHAR measurement_qual, NULL::DOUBLE measurement_prec
  FROM ichthyo i JOIN net n USING (net_uuid) JOIN tow t USING (tow_uuid) JOIN site s USING (site_uuid)
  WHERE i.measurement_type IS NULL"

ich_obs_attribute_sql <- "
  SELECT 'swfsc_ichthyo' dataset_key, 'swfsc_ichthyo:net:' || CAST(i.net_uuid AS VARCHAR) sample_key,
         CAST(i.species_id AS VARCHAR) taxon_key, i.life_stage,
         CASE i.measurement_type WHEN 'size' THEN 'body_length' ELSE i.measurement_type END measurement_type,
         i.measurement_value bin_value,
         CASE WHEN i.measurement_type='stage' THEN lk.description ELSE NULL END bin_label,
         i.tally count, NULL::VARCHAR measurement_qual
  FROM ichthyo i
  LEFT JOIN lookup lk ON lk.lookup_type = i.life_stage || '_stage'
                     AND lk.lookup_num = CAST(i.measurement_value AS INTEGER)
  WHERE i.measurement_type IN ('stage','size')"

ich_sample_measurement_sql <- "
  SELECT 'swfsc_ichthyo:net:' || CAST(net_uuid AS VARCHAR) sample_key,
         'swfsc_ichthyo' dataset_key, mt measurement_type, mv measurement_value,
         NULL::VARCHAR measurement_qual
  FROM (
    SELECT net_uuid, 'volume_sampled' mt, volume_sampled mv FROM net UNION ALL
    SELECT net_uuid, 'std_haul_factor', standard_haul_factor FROM net UNION ALL
    SELECT net_uuid, 'prop_sorted', prop_sorted FROM net UNION ALL
    SELECT net_uuid, 'small_plankton_biomass', smallplankton FROM net UNION ALL
    SELECT net_uuid, 'total_plankton_biomass', totalplankton FROM net)
  WHERE mv IS NOT NULL"

# the calcofi_bottle `sample` arms + cast-condition effort arm. NOTE cast_id is
# DOUBLE in cast_condition, so it is cast through BIGINT before the key is built —
# otherwise every key comes out as '…:cast:5.0' and orphans against `sample`.
btl_sample_sql <- c(
  cast = "
    SELECT 'calcofi_bottle:cast:' || CAST(cast_id AS VARCHAR), 'cast',
           NULL::VARCHAR, 'calcofi_bottle:cast:' || CAST(cast_id AS VARCHAR),
           'calcofi_bottle', grid_key, site_key, cruise_key,
           CAST(order_occ AS INTEGER), latitude, longitude,
           CAST(datetime_start_utc AS TIMESTAMP),
           NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR
    FROM casts",
  bottle = "
    SELECT 'calcofi_bottle:bottle:' || CAST(b.bottle_id AS VARCHAR), 'bottle',
           'calcofi_bottle:cast:' || CAST(b.cast_id AS VARCHAR),
           'calcofi_bottle:cast:' || CAST(b.cast_id AS VARCHAR),
           'calcofi_bottle', c.grid_key, b.site_key, c.cruise_key,
           CAST(c.order_occ AS INTEGER), c.latitude, c.longitude,
           CAST(c.datetime_start_utc AS TIMESTAMP),
           b.depth_m, b.depth_m, NULL::VARCHAR
    FROM bottle b JOIN casts c USING (cast_id)")

btl_sample_measurement_sql <- "
  SELECT 'calcofi_bottle:cast:' || CAST(CAST(cast_id AS BIGINT) AS VARCHAR),
         'calcofi_bottle', condition_type, condition_value, NULL::VARCHAR
  FROM cast_condition"

# casts / bottle / cast_condition rebuilt from the core. `sample_tbl` is a
# parameter because a downstream ingest (dic) loads bottle's shard under a
# distinct name and rebuilds these against it, while building its own `sample`.
btl_compat_specs <- function(sample_tbl = "sample") list(
  casts = glue::glue(
    "SELECT CAST(split_part(s.sample_key, ':', 3) AS BIGINT) AS cast_id,
            s.site_key, s.grid_key, s.cruise_key, s.order_occ,
            s.latitude, s.longitude, s.datetime AS datetime_start_utc, s.geom
     FROM {sample_tbl} s
     WHERE s.dataset_key = 'calcofi_bottle' AND s.sample_type = 'cast'"),
  bottle = glue::glue(
    "SELECT CAST(split_part(s.sample_key, ':', 3) AS BIGINT) AS bottle_id,
            CAST(split_part(s.parent_sample_key, ':', 3) AS BIGINT) AS cast_id,
            s.site_key, s.depth_min_m AS depth_m
     FROM {sample_tbl} s
     WHERE s.dataset_key = 'calcofi_bottle' AND s.sample_type = 'bottle'"),
  cast_condition = "
    SELECT sample_measurement_id AS cast_condition_id,
           CAST(split_part(sample_key, ':', 3) AS BIGINT) AS cast_id,
           measurement_type AS condition_type, measurement_value AS condition_value
    FROM sample_measurement
    WHERE dataset_key = 'calcofi_bottle'")

# a tiny synthetic CORE — `sample` / `obs_bio` / `obs_attribute` /
# `sample_measurement` / `taxon` / `sample_root` / `cruise` — for the Darwin Core
# exporter (R/dwc.R). Deliberately NOT built through the ingest arms: what these
# tests pin is the projection FROM the core, so the core is stated directly and a
# change to an ingest cannot quietly move the expected DwC rows.
#
# Shape: one cruise -> one site (root) -> one tow -> two nets.
#   net 1: taxon A larva 10 individuals, taxon B egg 4
#   net 2: taxon A larva 6
# obs_attribute on (net 1, A, larva): two body_length bins (12 mm x3, 15 mm x2) and
# one stage bin (stage 2 = "preflexion", 4 individuals).
# sample_measurement on both nets: volume_sampled, std_haul_factor, prop_sorted.
#
# `zeros = TRUE` adds a zero-valued row (net 2, taxon B, egg) — a dataset that
# RECORDS its absences. `zeros = FALSE` is positive-only: net 2 simply has no B row.
new_core_fixture <- function(zeros = FALSE, dataset_key = "test_ds") {
  testthat::skip_if_not_installed("duckdb")
  con <- get_duckdb_con(":memory:")
  k <- dataset_key
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample AS
    SELECT '{k}:site:1'::VARCHAR sample_key, 'site'::VARCHAR sample_type,
           NULL::VARCHAR parent_sample_key, '{k}:site:1'::VARCHAR root_sample_key,
           '{k}'::VARCHAR dataset_key, '090.0 060.0'::VARCHAR site_key,
           '2020-01-NODC'::VARCHAR cruise_key, 32.0::DOUBLE latitude,
           -120.0::DOUBLE longitude, NULL::TIMESTAMP datetime,
           NULL::DOUBLE depth_min_m, NULL::DOUBLE depth_max_m, NULL::VARCHAR tow_type
    UNION ALL SELECT '{k}:tow:1','tow','{k}:site:1','{k}:site:1','{k}','090.0 060.0',
           '2020-01-NODC',32.0,-120.0,TIMESTAMP '2020-01-02 10:00:00',0.0,210.0,'CB'
    UNION ALL SELECT '{k}:net:1','net','{k}:tow:1','{k}:site:1','{k}','090.0 060.0',
           '2020-01-NODC',32.0,-120.0,TIMESTAMP '2020-01-02 10:00:00',0.0,210.0,'CB'
    UNION ALL SELECT '{k}:net:2','net','{k}:tow:1','{k}:site:1','{k}','090.0 060.0',
           '2020-01-NODC',32.0,-120.0,TIMESTAMP '2020-01-02 10:00:00',0.0,210.0,'CB'"))
  DBI::dbExecute(con, "CREATE TABLE cruise AS
    SELECT '2020-01-NODC'::VARCHAR cruise_key, DATE '2020-01-01' date_min,
           DATE '2020-01-09' date_max, 'RV Test'::VARCHAR ship_name")
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample_root AS
    SELECT 1::INTEGER root_id, '{k}:site:1'::VARCHAR root_sample_key,
           '{k}'::VARCHAR dataset_key, 'site'::VARCHAR sample_type"))
  # `rank`, `class` and `family` are SQL keywords, so this table is written rather
  # than SELECTed into being — the release's own column names, unaliased
  DBI::dbWriteTable(con, "taxon", data.frame(
    taxon_key       = c("worms:1", "worms:2"),
    worms_id        = c(1L, 2L),
    scientific_name = c("Engraulis mordax", "Sardinops sagax"),
    rank            = c("Species", "Species"),
    kingdom         = c("Animalia", "Animalia"),
    phylum          = c("Chordata", "Chordata"),
    class           = c("Teleostei", "Teleostei"),
    order_taxon     = c("Clupeiformes", "Clupeiformes"),
    family          = c("Engraulidae", "Clupeidae"),
    common_name     = c("northern anchovy", "Pacific sardine"),
    stringsAsFactors = FALSE))
  # `value` is a SQL keyword too
  obs <- data.frame(
    obs_id             = 1:3,
    dataset_key        = k,
    root_id            = 1L,
    sample_key         = paste0(k, c(":net:1", ":net:1", ":net:2")),
    taxon_key          = c("worms:1", "worms:2", "worms:1"),
    life_stage         = c("larva", "egg", "larva"),
    measurement_type   = "abundance",
    units              = "count",
    value              = c(10, 4, 6),
    depth_bin          = NA_integer_,
    density_per_10m2   = c(50, 20, 30),
    density_per_1000m3 = c(100, 40, 60),
    stringsAsFactors   = FALSE)
  if (isTRUE(zeros))
    obs <- rbind(obs, data.frame(
      obs_id = 4L, dataset_key = k, root_id = 1L, sample_key = paste0(k, ":net:2"),
      taxon_key = "worms:2", life_stage = "egg", measurement_type = "abundance",
      units = "count", value = 0, depth_bin = NA_integer_,
      density_per_10m2 = 0, density_per_1000m3 = 0, stringsAsFactors = FALSE))
  DBI::dbWriteTable(con, "obs_bio", obs)
  DBI::dbWriteTable(con, "obs_attribute", data.frame(
    dataset_key      = k,
    sample_key       = paste0(k, ":net:1"),
    taxon_key        = "worms:1",
    life_stage       = "larva",
    measurement_type = c("body_length", "body_length", "stage"),
    bin_value        = c(12, 15, 2),
    bin_label        = c(NA, NA, "preflexion"),
    count            = c(3L, 2L, 4L),
    measurement_qual = NA_character_,
    stringsAsFactors = FALSE))
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample_measurement AS
    SELECT '{k}:net:1'::VARCHAR sample_key, '{k}'::VARCHAR dataset_key,
           'volume_sampled'::VARCHAR measurement_type, 100.0::DOUBLE measurement_value,
           NULL::VARCHAR measurement_qual
    UNION ALL SELECT '{k}:net:1','{k}','std_haul_factor',5.0,NULL
    UNION ALL SELECT '{k}:net:1','{k}','prop_sorted',0.5,NULL
    UNION ALL SELECT '{k}:net:2','{k}','volume_sampled',110.0,NULL
    UNION ALL SELECT '{k}:net:2','{k}','std_haul_factor',5.0,NULL"))
  con
}

# the three registries, as small frames with the real column names. `stage` has NO
# units on purpose: that is what makes dwc_emof() treat it as a categorical bin.
core_fixture_registries <- function() list(
  gear = tibble::tibble(
    tow_type = "CB", gear_name = "CalCOFI oblique bongo tow",
    dwc_samplingProtocol = "Oblique tow with a 71-cm bongo net.",
    nerc_l22 = "http://vocab.nerc.ac.uk/collection/L22/current/NETT0176/",
    datasets = "test_ds", note = NA_character_),
  life_stage = tibble::tibble(
    life_stage        = c("larva", "egg", "furcilia F1", "invert"),
    dwc_lifeStage     = c("larva", "egg", NA, NA),
    nerc_s11          = c("http://vocab.nerc.ac.uk/collection/S11/current/S1128/",
                          "http://vocab.nerc.ac.uk/collection/S11/current/S1122/", NA, NA),
    life_stage_parent = c(NA, NA, "furcilia", NA),
    datasets = "test_ds", note = NA_character_),
  measurement_type = tibble::tibble(
    measurement_type = c("abundance", "body_length", "stage", "std_haul_factor",
                         "prop_sorted", "volume_sampled"),
    units       = c("count", "mm", NA, "dimensionless", "dimensionless", "m3"),
    nerc_p01    = c(NA, "http://vocab.nerc.ac.uk/collection/P01/current/OBSINDLX/",
                    NA, NA, NA, NA),
    units_nerc_p06 = c("http://vocab.nerc.ac.uk/collection/P06/current/UCNT/",
                       "http://vocab.nerc.ac.uk/collection/P06/current/UXMM/", NA,
                       "http://vocab.nerc.ac.uk/collection/P06/current/UUUU/",
                       "http://vocab.nerc.ac.uk/collection/P06/current/UUUU/",
                       "http://vocab.nerc.ac.uk/collection/P06/current/MCUB/")))
