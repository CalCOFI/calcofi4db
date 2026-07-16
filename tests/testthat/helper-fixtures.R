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
           '2020-01-NODC'::VARCHAR cruise_key, 32.0::DOUBLE latitude, -120.0::DOUBLE longitude")
  DBI::dbExecute(con, "CREATE TABLE tow AS
    SELECT 'T1'::VARCHAR tow_uuid, 'S1'::VARCHAR site_uuid,
           TIMESTAMP '2020-01-01 10:00:00' datetime_start_utc")
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
