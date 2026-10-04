# hex7 on `sample` and `sample_root` (2026-10-02): ONE definition with obs_bio / obs_env — the
# resolution-7 parent of the resolution-10 cell of the normalised position, never the direct
# resolution-7 cell of the position (the two differ for 7 % of released sample positions).
# Cells are compared as text: a UBIGINT above 2^53 does not survive the trip into an R double.
h7_con <- function(env = parent.frame()) {
  con <- get_duckdb_con(":memory:")
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  con
}
h7_h3 <- function(con)
  tryCatch({ suppressMessages(load_duckdb_extension(con, "h3", from = "community")); TRUE },
           error = function(e) FALSE)
# (32.60, -121.00): res-10 cell 622219414748463103; its res-7 PARENT and the DIRECT res-7 cell of the
# same position are different hexagons — the fixture that tells the two definitions apart
H7_EDGE_PARENT <- "608708615875854335"
H7_EDGE_DIRECT <- "608708615808745471"
H7_SITE        <- "608718500776640511"   # (32.9, -117.3): parent and direct agree
H10_EDGE       <- "622219414748463103"

test_that("add_sample_hex7() stamps the res-7 parent of the res-10 cell; NULL, NaN and Inf positions give NULL", {
  con <- h7_con(); skip_if_not(h7_h3(con), "h3 community extension not available")
  DBI::dbExecute(con, "CREATE TABLE sample AS SELECT * FROM (VALUES
    ('a', 32.60, -121.00), ('b', 32.9, -117.3),
    ('c', NULL, -118.0), ('d', 33.0, NULL), ('e', NULL, NULL),
    ('f', 'NaN'::DOUBLE, -118.0), ('g', 33.0, 'NaN'::DOUBLE), ('h', 'NaN'::DOUBLE, 'NaN'::DOUBLE),
    ('i', 'inf'::DOUBLE, -118.0), ('j', 33.0, '-inf'::DOUBLE)
    ) t(sample_key, latitude, longitude)")
  expect_message(n <- add_sample_hex7(con), "hex7 on sample: 2 of 10 rows")
  expect_equal(n, 2)
  d <- DBI::dbGetQuery(con, "SELECT sample_key, hex7::VARCHAR AS hex7 FROM sample ORDER BY sample_key")
  expect_equal(d$hex7[d$sample_key == "a"], H7_EDGE_PARENT)
  expect_false(d$hex7[d$sample_key == "a"] == H7_EDGE_DIRECT)   # regression: never the direct res-7 cell
  expect_equal(d$hex7[d$sample_key == "b"], H7_SITE)
  expect_true(all(is.na(d$hex7[!d$sample_key %in% c("a", "b")])))
  # and it is what the h3 extension says the parent of the res-10 cell is
  x <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM sample WHERE latitude = 32.60
    AND hex7 = h3_cell_to_parent(h3_latlng_to_cell(latitude, longitude, 10), 7)::UBIGINT
    AND hex7 <> h3_latlng_to_cell(latitude, longitude, 7)::UBIGINT")
  expect_equal(x$n, 1)
  # the non-finite coordinates themselves are left as they were: this function stamps, it does not clean
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM sample WHERE isnan(latitude) OR isinf(latitude)")$n, 3)
})

test_that("add_sample_hex7() appends one UBIGINT column, keeps every other column, type and row, and is idempotent", {
  con <- h7_con(); skip_if_not(h7_h3(con), "h3 community extension not available")
  suppressMessages(load_duckdb_extension(con, "spatial"))
  DBI::dbExecute(con, "CREATE TABLE sample AS
    SELECT sample_key, latitude, longitude, ST_SetCRS(ST_Point(longitude, latitude), 'EPSG:4326') AS geom,
           gen_random_uuid() AS source_uuid, 100.0 AS seafloor_depth_m
    FROM (VALUES ('a', 32.60::DOUBLE, -121.00::DOUBLE), ('b', 32.9, -117.3), ('c', NULL, -118.0)) t(sample_key, latitude, longitude)")
  before  <- DBI::dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sample' ORDER BY ordinal_position")
  uuid_0  <- DBI::dbGetQuery(con, "SELECT sample_key, source_uuid::VARCHAR AS u, ST_AsText(geom) AS g FROM sample ORDER BY 1")
  expect_match(before$data_type[before$column_name == "geom"], "EPSG:4326")
  suppressMessages(add_sample_hex7(con))
  after <- DBI::dbGetQuery(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'sample' ORDER BY ordinal_position")
  expect_equal(after$column_name, c(before$column_name, "hex7"))          # appended, nothing reordered
  expect_equal(after$data_type[seq_len(nrow(before))], before$data_type)   # the CRS tag on geom survives the rebuild
  expect_equal(after$data_type[after$column_name == "hex7"], "UBIGINT")
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key, source_uuid::VARCHAR AS u, ST_AsText(geom) AS g FROM sample ORDER BY 1"), uuid_0)
  h1 <- DBI::dbGetQuery(con, "SELECT sample_key, hex7::VARCHAR AS hex7 FROM sample ORDER BY 1")
  # again: same columns once, same cells; no scratch table left behind
  suppressMessages(add_sample_hex7(con))
  expect_equal(DBI::dbListFields(con, "sample"), after$column_name)
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key, hex7::VARCHAR AS hex7 FROM sample ORDER BY 1"), h1)
  expect_equal(DBI::dbListTables(con), "sample")
  # a stale hex7 (wrong values, wrong place) is recomputed and moved last, never trusted
  DBI::dbExecute(con, "CREATE TABLE s2 AS SELECT sample_key, 1::UBIGINT AS hex7, latitude, longitude FROM sample")
  suppressMessages(add_sample_hex7(con, "s2"))
  expect_equal(DBI::dbListFields(con, "s2"), c("sample_key", "latitude", "longitude", "hex7"))
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key, hex7::VARCHAR AS hex7 FROM s2 ORDER BY 1"), h1)
  # a view (how the release loads an ingest shard) becomes a table carrying the column
  DBI::dbExecute(con, "CREATE VIEW v AS SELECT sample_key, latitude, longitude FROM sample")
  suppressMessages(add_sample_hex7(con, "v"))
  expect_equal(DBI::dbGetQuery(con, "SELECT table_type FROM information_schema.tables WHERE table_name = 'v'")$table_type, "BASE TABLE")
  expect_equal(DBI::dbGetQuery(con, "SELECT sample_key, hex7::VARCHAR AS hex7 FROM v ORDER BY 1"), h1)
  # no position columns, no stamp
  DBI::dbExecute(con, "CREATE TABLE nopos AS SELECT 1 AS id")
  expect_error(add_sample_hex7(con, "nopos"), "latitude")
})

test_that("build_sample_root() carries sample.hex7, last; without it the column is NULL and it says so", {
  con <- h7_con()   # no h3 needed: the root table carries the cell, it never computes one
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample AS SELECT * FROM (VALUES
    ('x:site:1', 'site', NULL,       'x:site:1', 'x', 'g', '2019-04-33UD', 1, 32.60::DOUBLE, -121.00::DOUBLE, TIMESTAMP '2019-04-02 22:00', NULL, NULL, NULL, {H7_EDGE_PARENT}::UBIGINT),
    ('x:net:1',  'net',  'x:site:1', 'x:site:1', 'x', 'g', '2019-04-33UD', 1, 32.9,  -117.3,  TIMESTAMP '2019-04-02 22:10', 0.0, 210.0, 'CB', {H7_SITE}::UBIGINT),
    ('x:cast:2', 'cast', NULL,       'x:cast:2', 'x', 'g', '2019-04-33UD', 2, NULL,  -117.3,  TIMESTAMP '2019-04-02 23:00', 0.0, 500.0, NULL, NULL::UBIGINT)
    ) t(sample_key, sample_type, parent_sample_key, root_sample_key, dataset_key, grid_key, cruise_key, order_occ,
        latitude, longitude, datetime, depth_min_m, depth_max_m, tow_type, hex7)"))
  expect_no_message(n <- build_sample_root(con))
  expect_equal(n, 2)
  flds <- DBI::dbListFields(con, "sample_root")
  expect_equal(flds, c("root_id", "root_sample_key", "dataset_key", "sample_type", "grid_key", "cruise_key", "order_occ",
                       "latitude", "longitude", "datetime", "depth_min_m", "depth_max_m", "tow_type", "seafloor_depth_m", "hex7"))
  r <- DBI::dbGetQuery(con, "SELECT root_sample_key, hex7::VARCHAR AS hex7 FROM sample_root ORDER BY root_id")
  expect_equal(r$hex7, c(NA, H7_EDGE_PARENT))            # the root's own cell, not its net's
  expect_equal(DBI::dbGetQuery(con, "SELECT data_type FROM information_schema.columns WHERE table_name = 'sample_root' AND column_name = 'hex7'")$data_type, "UBIGINT")
  # root for root, the two tables agree
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n FROM sample s JOIN sample_root r ON r.root_sample_key = s.sample_key
    WHERE s.hex7 IS DISTINCT FROM r.hex7")$n, 0)
  # a `sample` that was never stamped: same schema, NULL cells, and a message naming the fix
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE sample AS SELECT * EXCLUDE (hex7) FROM sample")
  expect_message(build_sample_root(con), "add_sample_hex7")
  expect_equal(DBI::dbListFields(con, "sample_root"), flds)
  expect_equal(DBI::dbGetQuery(con, "SELECT count(hex7) AS n, any_value(typeof(hex7)) AS t FROM sample_root"), data.frame(n = 0, t = "UBIGINT"))
})

test_that("hex7 on a sample is the hex7 of an observation at the same position, through the real append_*() path", {
  con <- h7_con(); skip_if_not(h7_h3(con), "h3 community extension not available")
  # a root cast with two bottles at its own position, an underway root with a NaN latitude
  arm <- "SELECT * FROM (VALUES
    ('x:cast:1',   'cast',   NULL,       'x:cast:1', 'x', 'g', NULL, '2019-04-33UD', 1, 32.60::DOUBLE, -121.00::DOUBLE, TIMESTAMP '2019-04-02 23:00', 0.0, 500.0, NULL),
    ('x:bottle:1', 'bottle', 'x:cast:1', 'x:cast:1', 'x', 'g', NULL, '2019-04-33UD', 1, 32.60, -121.00, TIMESTAMP '2019-04-02 23:00', 10.0, 10.0, NULL),
    ('x:bottle:2', 'bottle', 'x:cast:1', 'x:cast:1', 'x', 'g', NULL, '2019-04-33UD', 1, 32.9,  -117.3,  TIMESTAMP '2019-04-02 23:00', 250.0, 250.0, NULL),
    ('x:u:1',      'underway', NULL,     'x:u:1',    'x', NULL, NULL, '2019-04-33UD', NULL, 'NaN'::DOUBLE, -118.0, TIMESTAMP '2019-04-03 01:00', 0.0, 0.0, NULL))"
  suppressMessages(append_sample(con, arm))
  suppressMessages(append_obs(con, "SELECT * FROM (VALUES
    ('env', 'x', 'x:bottle:1', 'g', '2019-04-33UD', 32.60, -121.00, TIMESTAMP '2019-04-02 23:00', 10.0, 10.0, NULL, NULL, 'temperature', 15.5, NULL, NULL::DOUBLE),
    ('env', 'x', 'x:bottle:2', 'g', '2019-04-33UD', 32.9,  -117.3,  TIMESTAMP '2019-04-02 23:00', 250.0, 250.0, NULL, NULL, 'temperature', 8.1, NULL, NULL),
    ('env', 'x', 'x:u:1',      NULL, '2019-04-33UD', 'NaN'::DOUBLE, -118.0, TIMESTAMP '2019-04-03 01:00', 0.0, 0.0, NULL, NULL, 'temperature', 16.0, NULL, NULL))"))
  DBI::dbExecute(con, "CREATE TABLE sample_measurement (sample_measurement_id BIGINT, sample_key VARCHAR, dataset_key VARCHAR,
    measurement_type VARCHAR, measurement_value DOUBLE, measurement_qual VARCHAR)")
  DBI::dbExecute(con, "CREATE TABLE measurement_type AS SELECT 'temperature' AS measurement_type, 'degC' AS units")
  suppressMessages(add_sample_hex7(con))
  build_sample_root(con)
  build_obs_slim(con, "env", "TRUE", "NULL::DOUBLE AS density_per_10m2, NULL::DOUBLE AS density_per_1000m3, NULL::VARCHAR AS effort_class")
  d <- DBI::dbGetQuery(con, "
    SELECT e.sample_key, s.hex7::VARCHAR AS sample_hex7, e.hex7::VARCHAR AS obs_hex7,
           h3_cell_to_parent(o.hex_id, 7)::UBIGINT::VARCHAR AS obs_parent7, r.hex7::VARCHAR AS root_hex7
    FROM obs_env e JOIN obs o USING (obs_id) JOIN sample s ON s.sample_key = e.sample_key
    LEFT JOIN sample_root r USING (root_id) ORDER BY 1")
  expect_equal(d$sample_hex7, c(H7_EDGE_PARENT, H7_SITE, NA))
  expect_equal(d$sample_hex7, d$obs_hex7)        # the sample's cell IS its observation's cell
  expect_equal(d$sample_hex7, d$obs_parent7)     # which is h3_cell_to_parent(obs.hex_id, 7)
  # the root's cell is the cast's own position: bottle 2 sits elsewhere, and its root says so
  expect_equal(d$root_hex7, c(H7_EDGE_PARENT, H7_EDGE_PARENT, NA))
  # sample.hex7 = sample_root.hex7 on every root, NULL included
  expect_equal(DBI::dbGetQuery(con, "SELECT count(*) AS n, count(*) FILTER (WHERE s.hex7 IS DISTINCT FROM r.hex7) AS n_diff
    FROM sample s JOIN sample_root r ON r.root_sample_key = s.sample_key"), data.frame(n = 2, n_diff = 0))
  ck <- check_sample_hex7(con, obs_tbls = "obs_env")
  expect_true(all(ck$status != "fail"))
  expect_equal(ck$n[ck$check == "obs_same_position"], 2); expect_equal(ck$n_bad[ck$check == "obs_same_position"], 0)
})

test_that("build_obs_slim()'s hex7 is still the same SQL: one fragment for the observation and the sample side", {
  expect_equal(calcofi4db:::.hex7_sql("o.hex_id"),
               paste0("CASE WHEN o.hex_id IS NULL THEN NULL ELSE ", h3_parent_sql("o.hex_id", 7), " END"))
  # the position fragment refuses a non-finite coordinate before h3 ever sees it
  expect_match(as.character(calcofi4db:::.hex_expr()), "isnan\\(latitude\\)")
  expect_match(as.character(calcofi4db:::.hex_expr()), "isinf\\(longitude\\)")
  expect_match(as.character(calcofi4db:::.hex_expr()), "h3_latlng_to_cell\\(latitude, longitude, 10\\)")
})

# check_sample_hex7(): the release gate. Fixtures carry hard-coded cells, so no extension is needed.
h7_gate_fixture <- function(con) {
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample AS SELECT * FROM (VALUES
    ('x:cast:1',   NULL,       32.60::DOUBLE, -121.00::DOUBLE, {H7_EDGE_PARENT}::UBIGINT),
    ('x:bottle:1', 'x:cast:1', 32.60, -121.00, {H7_EDGE_PARENT}::UBIGINT),
    ('x:bottle:2', 'x:cast:1', 32.9,  -117.3,  {H7_SITE}::UBIGINT),
    ('x:u:1',      NULL,       NULL,  -118.0,  NULL::UBIGINT),
    ('x:u:2',      NULL,       NULL,  NULL,    NULL::UBIGINT)
    ) t(sample_key, parent_sample_key, latitude, longitude, hex7)"))
  DBI::dbExecute(con, glue::glue("CREATE TABLE sample_root AS SELECT * FROM (VALUES
    (1, 'x:cast:1', 32.60::DOUBLE, -121.00::DOUBLE, {H7_EDGE_PARENT}::UBIGINT),
    (2, 'x:u:1',    NULL,  -118.0,  NULL::UBIGINT),
    (3, 'x:u:2',    NULL,  NULL,    NULL::UBIGINT)
    ) t(root_id, root_sample_key, latitude, longitude, hex7)"))
  # obs 1 at its bottle's position; obs 2 a scan that drifted into the neighbouring cell; obs 3 unplaced
  DBI::dbExecute(con, glue::glue("CREATE TABLE obs_env AS SELECT * FROM (VALUES
    (1, 'x:bottle:1', 32.60::DOUBLE, -121.00::DOUBLE, {H7_EDGE_PARENT}::UBIGINT),
    (2, 'x:bottle:1', 32.61, -121.02, {H7_EDGE_DIRECT}::UBIGINT),
    (3, 'x:u:1',      NULL,  NULL,    NULL::UBIGINT)
    ) t(obs_id, sample_key, latitude, longitude, hex7)"))
}

test_that("check_sample_hex7() passes a consistent release and reports, without failing, an obs in another cell", {
  con <- h7_con(); h7_gate_fixture(con)
  ck <- check_sample_hex7(con, obs_tbls = "obs_env")
  expect_named(ck, c("check", "table", "n", "n_bad", "status"))
  expect_equal(ck$check, c("position_has_hex7", "hex7_has_position", "hex7_is_res7",
                           "position_has_hex7", "hex7_has_position", "hex7_is_res7",
                           "root_equals_sample", "obs_same_position", "obs_other_cell"))
  expect_equal(ck$table, c(rep("sample", 3), rep("sample_root", 3), "sample_root", "obs_env", "obs_env"))
  # count(hex7) = the count of finite positions, on both tables: 3 of 5 samples, 1 of 3 roots
  expect_equal(ck$n, c(3, 3, 3, 1, 1, 1, 3, 1, 2))
  expect_equal(ck$n_bad, c(0, 0, 0, 0, 0, 0, 0, 0, 1))
  expect_equal(ck$status, c(rep("ok", 8), "report"))
  # absent obs tables are skipped, not an error (a scratch database holding only the sample tables)
  expect_equal(nrow(check_sample_hex7(con, obs_tbls = c("obs_bio", "nope"))), 7)
})

test_that("check_sample_hex7() fails each rule by name", {
  bad <- function(sql, obs_tbls = "obs_env") {
    con <- h7_con(); h7_gate_fixture(con); DBI::dbExecute(con, sql)
    ck <- check_sample_hex7(con, obs_tbls = obs_tbls)
    ck[ck$status == "fail", , drop = FALSE]
  }
  # a positioned sample left without a cell
  f <- bad("UPDATE sample SET hex7 = NULL WHERE sample_key = 'x:bottle:2'")
  expect_equal(paste(f$check, f$table), "position_has_hex7 sample"); expect_equal(f$n_bad, 1)
  # a cell on a row with half a position; on a NaN; on an infinity
  f <- bad(glue::glue("UPDATE sample SET hex7 = {H7_SITE}::UBIGINT WHERE sample_key = 'x:u:1'"))
  expect_equal(paste(f$check, f$table), c("hex7_has_position sample", "root_equals_sample sample_root"))
  f <- bad(glue::glue("UPDATE sample SET latitude = 'NaN'::DOUBLE WHERE sample_key = 'x:bottle:2'"))
  expect_equal(paste(f$check, f$table), "hex7_has_position sample")
  f <- bad(glue::glue("UPDATE sample_root SET longitude = 'inf'::DOUBLE WHERE root_id = 1"))
  expect_equal(paste(f$check, f$table), "hex7_has_position sample_root")
  # a res-10 cell where a res-7 cell belongs
  f <- bad(glue::glue("UPDATE sample SET hex7 = {H10_EDGE}::UBIGINT WHERE sample_key = 'x:bottle:2'"))
  expect_equal(paste(f$check, f$table), "hex7_is_res7 sample")
  # the root table disagrees with its own sample: another cell, a missing root, a root that is no root
  f <- bad(glue::glue("UPDATE sample_root SET hex7 = {H7_SITE}::UBIGINT WHERE root_id = 1"))
  expect_equal(paste(f$check, f$table), "root_equals_sample sample_root"); expect_equal(f$n_bad, 1)
  f <- bad("DELETE FROM sample_root WHERE root_id = 3")
  expect_equal(paste(f$check, f$table), "root_equals_sample sample_root"); expect_equal(f$n_bad, 1)
  f <- bad(glue::glue("INSERT INTO sample_root VALUES (4, 'x:bottle:1', 32.60, -121.00, {H7_EDGE_PARENT}::UBIGINT)"))
  expect_equal(paste(f$check, f$table), "root_equals_sample sample_root"); expect_equal(f$n_bad, 1)
  # an observation AT its sample's position in another cell: the two definitions have drifted
  f <- bad(glue::glue("UPDATE obs_env SET hex7 = {H7_EDGE_DIRECT}::UBIGINT WHERE obs_id = 1"))
  expect_equal(paste(f$check, f$table), "obs_same_position obs_env"); expect_equal(f$n_bad, 1)
  f <- bad("UPDATE obs_env SET hex7 = NULL WHERE obs_id = 1")
  expect_equal(paste(f$check, f$table), "obs_same_position obs_env")
})

test_that("check_sample_hex7() refuses a table that was never stamped", {
  con <- h7_con(); h7_gate_fixture(con)
  DBI::dbExecute(con, "CREATE OR REPLACE TABLE sample_root AS SELECT * EXCLUDE (hex7) FROM sample_root")
  expect_error(check_sample_hex7(con), "sample_root.*hex7")
  expect_error(check_sample_hex7(con, sample_tbl = "nope"), "nope")
})
