# Browser-shaped release objects (CalCOFI Explorer plan D4, 2026-08-28).
#
# A browser can hold the whole bio realm (~1.3 M rows) or one env variable (~1 M rows) in memory and
# aggregate it to any grain in milliseconds — what it cannot do is chase footers over the 200 MB `obs`
# twin or join `sample`, `sample_measurement` and `taxon` on every query. So the release cuts, once:
#   sample_root     one row per root sampling event with a dense integer `root_id` (the join key the
#                   browser objects share; `root_sample_key` is the string it stands for)
#   obs_bio         the bio realm, slim: root_id, hex7 (one UBIGINT H3 cell at res 7; coarser parents
#                   are bit arithmetic, see h3_parent_sql()), depth_bin, qual_ok, the gear and effort
#                   of the observation's own sample, and the D8 densities + effort_class
#   obs_env         the env realm with the same columns, hive-partitioned by measurement_type so one
#                   variable is one object
#   sample_spatial  exact per-root-sample polygon membership for every layer of `spatial`, computed
#                   once here (chunked per layer) instead of per app
#   coverage.json   n obs / samples by dataset x station x year and by dataset x variable — the first
#                   paint before any WASM wakes up, and Task 14's inventory backbone
# The quality predicate and the density expression are calcofi4r's (cc_qual_ok_sql(), cc_density_sql())
# and are passed in as SQL, so this package never carries a second copy of either.

#' H3 parent of a cell as plain SQL (no extension)
#'
#' An H3 index stores its resolution in bits 52–55 and one 3-bit digit per resolution, unused digits
#' set to 7. The parent at resolution `res` is therefore the same index with the resolution field
#' rewritten and every finer digit set to 7 — pure bit arithmetic, which a browser without the `h3`
#' extension can run. Verified against `h3_cell_to_parent()` in the tests.
#'
#' @param hex SQL expression for a `UBIGINT` H3 cell.
#' @param res target resolution (coarser than the cell's).
#' @return A SQL expression string; `NULL` cells stay `NULL`.
#' @examples
#' h3_parent_sql("hex7", 5)
#' @export
#' @concept release
h3_parent_sql <- function(hex, res) {
  stopifnot(is.numeric(res), length(res) == 1, res >= 0, res <= 15)
  res <- as.integer(res)
  sprintf("(((%s & ~(15::UBIGINT << 52)) | (%d::UBIGINT << 52)) | ((1::UBIGINT << %d) - 1))",
          hex, res, 3L * (15L - res))
}

#' Root sampling events with a dense integer id
#'
#' One row per `sample` with no parent, numbered by `dense_rank()` over `sample_key` so the id is
#' deterministic across runs; carries the root's position, time, cruise, gear and seafloor depth.
#' Every browser object joins on `root_id`; `root_sample_key` is what it stands for.
#'
#' @param con DuckDB connection holding `sample`.
#' @param tbl name of the table to (re)create.
#' @return Invisibly, the row count.
#' @export
#' @concept release
#' @importFrom DBI dbExecute dbGetQuery dbListFields
#' @importFrom glue glue
build_sample_root <- function(con, tbl = "sample_root") {
  seafloor <- if ("seafloor_depth_m" %in% dbListFields(con, "sample")) "seafloor_depth_m" else "NULL::DOUBLE AS seafloor_depth_m"
  dbExecute(con, glue("
    CREATE OR REPLACE TABLE {tbl} AS
    SELECT dense_rank() OVER (ORDER BY sample_key)::INTEGER AS root_id, sample_key AS root_sample_key,
           dataset_key, sample_type, grid_key, cruise_key, order_occ, latitude, longitude, datetime,
           depth_min_m, depth_max_m, tow_type, {seafloor}
    FROM sample WHERE parent_sample_key IS NULL ORDER BY sample_key"))
  n <- dbGetQuery(con, glue("SELECT count(*) AS n, count(DISTINCT root_id) AS n_id FROM {tbl}"))
  stopifnot(n$n == n$n_id)
  invisible(n$n)
}

#' The bio or env realm of `obs`, browser-shaped — and, since 3.31.0, its physical store
#'
#' Slims `obs` to the columns a lens needs, joins the gear and effort of the observation's own sample
#' (`sample.tow_type`; `std_haul_factor`, `prop_sorted`, `volume_sampled` from `sample_measurement`),
#' stamps `root_id`, `year`, `quarter`, `depth_bin` (10 m), `hex7`, `qual_ok` (from `qual_ok_sql`)
#' and the D8 densities + `effort_class` (from `density_sql`). Depth is the observation's, falling back
#' to its sample's and then its root's, so a net tow carries its integrated span. Both realms get the
#' same schema (effort and taxon columns are NULL for env — a NULL column costs nothing in parquet), so
#' one set of SQL templates serves both.
#'
#' Since 3.31.0 (pre-release plan D-S1) the pair is a **strict superset of `obs` under a name
#' mapping**: each row also carries `sample_key` (the observation's own sampling event — without it
#' a consumer reaches only the root and loses the net / bottle grain), `measurement_prec` and
#' `hex_id` (the res-10 H3 cell `hex7` is the parent of); `realm` is implied by the table and
#' `measurement_value` is `value`. [obs_view_sql()] is the UNION ALL that reconstructs `obs` from the
#' pair under its original 18 column names, and [check_obs_pair_parity()] asserts the pair holds
#' exactly `obs`'s rows. The one deliberate difference is the depth fallback above: where `obs`
#' has no depth for a bio row (a net tow whose span lives on `sample`), the pair — and therefore the
#' view — carries the sample's span; a non-NULL `obs` depth is never changed.
#'
#' @param con DuckDB connection holding `obs`, `sample`, `sample_measurement`, `measurement_type` and
#'   the `sample_root` built by [build_sample_root()].
#' @param realm `"bio"` or `"env"`.
#' @param qual_ok_sql the quality predicate over alias `o` — `calcofi4r::cc_qual_ok_sql("o")`.
#' @param density_sql the density select-list over the unaliased effort columns —
#'   `calcofi4r::cc_density_sql()`.
#' @param tbl output table (default `obs_{realm}`).
#' @return Invisibly, the row count.
#' @export
#' @concept release
build_obs_slim <- function(con, realm = c("bio", "env"), qual_ok_sql, density_sql, tbl = NULL) {
  realm <- match.arg(realm)
  tbl   <- tbl %||% paste0("obs_", realm)
  stopifnot(is.character(qual_ok_sql), length(qual_ok_sql) == 1, is.character(density_sql), length(density_sql) == 1)
  dbExecute(con, glue("
    CREATE OR REPLACE TABLE {tbl} AS
    WITH eff AS (
      SELECT sample_key,
             max(measurement_value) FILTER (WHERE measurement_type = 'std_haul_factor') AS std_haul_factor,
             max(measurement_value) FILTER (WHERE measurement_type = 'prop_sorted')     AS prop_sorted,
             max(measurement_value) FILTER (WHERE measurement_type = 'volume_sampled')  AS volume_sampled_m3
      FROM sample_measurement
      WHERE measurement_type IN ('std_haul_factor', 'prop_sorted', 'volume_sampled') GROUP BY 1),
    x AS (
      SELECT o.obs_id, o.dataset_key, r.root_id, o.sample_key, o.grid_key, o.cruise_key,
             o.latitude, o.longitude, o.datetime,
             year(o.datetime)::SMALLINT AS year, quarter(o.datetime)::TINYINT AS quarter,
             COALESCE(o.depth_min_m, s.depth_min_m, r.depth_min_m) AS depth_min_m,
             COALESCE(o.depth_max_m, s.depth_max_m, r.depth_max_m) AS depth_max_m,
             o.taxon_key, o.life_stage, o.measurement_type, m.units, o.measurement_value,
             o.measurement_qual, o.measurement_prec, ({qual_ok_sql}) AS qual_ok,
             s.tow_type, e.std_haul_factor, e.prop_sorted, e.volume_sampled_m3,
             o.hex_id,
             CASE WHEN o.hex_id IS NULL THEN NULL ELSE {h3_parent_sql('o.hex_id', 7)} END AS hex7
      FROM obs o
      LEFT JOIN sample s USING (sample_key)
      LEFT JOIN sample_root r ON r.root_sample_key = COALESCE(s.root_sample_key, s.sample_key)
      LEFT JOIN eff e USING (sample_key)
      LEFT JOIN measurement_type m USING (measurement_type)
      WHERE o.realm = '{realm}')
    SELECT obs_id, dataset_key, root_id, sample_key, grid_key, cruise_key, latitude, longitude, datetime, year, quarter,
           depth_min_m, depth_max_m, (floor(depth_min_m / 10) * 10)::INTEGER AS depth_bin,
           taxon_key, life_stage, measurement_type, units, measurement_value AS value, measurement_qual, measurement_prec, qual_ok,
           tow_type, std_haul_factor, prop_sorted, volume_sampled_m3,
           {density_sql},
           hex_id, hex7
    FROM x"))
  n <- dbGetQuery(con, glue("SELECT count(*) AS n FROM {tbl}"))$n
  invisible(n)
}

#' The 18 columns of `obs`, in order
#'
#' The public shape of `obs` (v2026.02 → v2026.09) that [obs_view_sql()] reconstructs from
#' `obs_bio` + `obs_env`. Order matters: a consumer that `UNION`s or reads positionally sees the
#' view exactly as it saw the table.
#' @export
#' @concept release
OBS_VIEW_COLUMNS <- c(
  "obs_id", "realm", "dataset_key", "sample_key", "grid_key", "cruise_key",
  "latitude", "longitude", "datetime", "depth_min_m", "depth_max_m",
  "taxon_key", "life_stage", "measurement_type", "measurement_value",
  "measurement_qual", "measurement_prec", "hex_id")

#' `obs` as a view over `obs_bio` + `obs_env`
#'
#' The UNION ALL that reconstructs `obs` — its 18 columns, in [OBS_VIEW_COLUMNS] order, under their
#' original names — from the bifurcated pair (pre-release plan D-S1): `realm` is the constant each
#' branch contributes, `value` becomes `measurement_value`. The default sources are the **tokens**
#' `{{obs_bio}}` / `{{obs_env}}`, which is how the SQL is stored in a release's `catalog.json`
#' (`views.obs`): every resolver — `calcofi4r::cc_get_db()`, `calcofi4py.cc_get_db()`, db-query's
#' `__TBL:obs__` — substitutes its own way of reading each table ([substitute_view_tables()]), a
#' quoted table name inside a connection or a `read_parquet(...)` over the catalog's objects.
#'
#' @param bio,env what to put after `FROM` for each realm: a token, a quoted table name, or a
#'   `read_parquet(...)` expression.
#' @return A length-one SQL string (no trailing semicolon; wrap in parentheses to use it in a
#'   `FROM`).
#' @examples
#' cat(obs_view_sql())
#' cat(obs_view_sql('"obs_bio"', '"obs_env"'))
#' @export
#' @concept release
obs_view_sql <- function(bio = "{{obs_bio}}", env = "{{obs_env}}") {
  stopifnot(is.character(bio), length(bio) == 1, is.character(env), length(env) == 1)
  branch <- function(realm, src) paste0(
    "SELECT obs_id, '", realm, "' AS realm, dataset_key, sample_key, grid_key, cruise_key,\n",
    "       latitude, longitude, datetime, depth_min_m, depth_max_m,\n",
    "       taxon_key, life_stage, measurement_type, value AS measurement_value,\n",
    "       measurement_qual, measurement_prec, hex_id\n",
    "FROM ", src)
  paste(branch("bio", bio), branch("env", env), sep = "\nUNION ALL\n")
}

#' The tables a catalog view reads, and the SQL with them resolved
#'
#' A view in `catalog.json` names its source tables as `{{table}}` tokens so that the SQL is
#' storage-agnostic. `release_view_tables()` lists them; `substitute_view_tables()` replaces each
#' with `rp(table)` — a quoted identifier by default, or whatever the caller reads a table through.
#'
#' @param sql a view's SQL carrying `{{table}}` tokens.
#' @param rp `function(table) -> character(1)`; default quotes the name.
#' @return `release_view_tables()`: the distinct table names, in order of first appearance;
#'   `substitute_view_tables()`: the SQL with every token replaced.
#' @examples
#' release_view_tables(obs_view_sql())
#' @export
#' @concept release
release_view_tables <- function(sql) {
  m <- regmatches(sql, gregexpr("\\{\\{([A-Za-z0-9_]+)\\}\\}", sql))[[1]]
  unique(gsub("^\\{\\{|\\}\\}$", "", m))
}

#' @rdname release_view_tables
#' @export
substitute_view_tables <- function(sql, rp = function(table) paste0('"', table, '"')) {
  stopifnot(is.function(rp))
  for (t in release_view_tables(sql))
    sql <- gsub(paste0("{{", t, "}}"), rp(t), sql, fixed = TRUE)
  sql
}

#' Assert that `obs_bio` + `obs_env` hold exactly the rows of `obs`
#'
#' The gate behind D-S1: before `obs` can be served as [obs_view_sql()] over the pair, the pair
#' must reproduce it. Per `(realm, dataset_key)` this compares the row count, the number of
#' distinct `obs_id`s and an order-independent signature (`bit_xor(hash(...))`) of every column
#' except depth between `obs` and the view run over the pair, and — joining the two on `obs_id` —
#' counts the rows whose depth the pair **filled** (NULL in `obs`, the sample's span in the pair:
#' the documented fallback of [build_obs_slim()]) and the rows whose non-NULL depth it **changed**
#' (never allowed). Any group on one side only, any count / signature mismatch, or any changed
#' depth is an error naming the group.
#'
#' @param con DuckDB connection holding `obs`, `bio` and `env`.
#' @param obs,bio,env table names.
#' @return Invisibly, a tibble with one row per `(realm, dataset_key)`: `n_obs`, `n_pair`,
#'   `n_id_pair`, `sig_ok`, `n_depth_filled`, `n_depth_changed`, `ok`.
#' @export
#' @concept release
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
check_obs_pair_parity <- function(con, obs = "obs", bio = "obs_bio", env = "obs_env") {
  q <- function(x) paste0('"', x, '"')
  view <- obs_view_sql(q(bio), q(env))
  sig_cols <- setdiff(OBS_VIEW_COLUMNS, c("depth_min_m", "depth_max_m"))
  h <- paste0("hash(", paste(sig_cols, collapse = ", "), ")")
  agg <- function(src) dbGetQuery(con, glue("
    SELECT realm, dataset_key, count(*)::BIGINT AS n, count(DISTINCT obs_id)::BIGINT AS n_id,
           bit_xor({h}) AS sig
    FROM ({src}) GROUP BY 1, 2"))
  a <- agg(glue("SELECT * FROM {q(obs)}"))
  b <- agg(view)
  d <- dbGetQuery(con, glue("
    SELECT o.realm, o.dataset_key,
           count(*) FILTER (WHERE o.depth_min_m IS NULL AND p.depth_min_m IS NOT NULL)::BIGINT AS n_depth_filled,
           count(*) FILTER (WHERE (o.depth_min_m IS NOT NULL AND o.depth_min_m IS DISTINCT FROM p.depth_min_m)
                               OR (o.depth_max_m IS NOT NULL AND o.depth_max_m IS DISTINCT FROM p.depth_max_m))::BIGINT AS n_depth_changed
    FROM (SELECT obs_id, realm, dataset_key, depth_min_m, depth_max_m FROM {q(obs)}) o
    JOIN (SELECT obs_id, depth_min_m, depth_max_m FROM ({view})) p USING (obs_id)
    GROUP BY 1, 2"))
  key <- function(x) paste(x$realm, x$dataset_key, sep = "|")
  groups <- sort(union(key(a), key(b)))
  ia <- match(groups, key(a)); ib <- match(groups, key(b)); id <- match(groups, key(d))
  out <- tibble::tibble(
    realm           = sub("\\|.*$", "", groups),
    dataset_key     = sub("^[^|]*\\|", "", groups),
    n_obs           = as.numeric(a$n[ia]),
    n_pair          = as.numeric(b$n[ib]),
    n_id_pair       = as.numeric(b$n_id[ib]),
    sig_ok          = !is.na(ia) & !is.na(ib) & a$sig[ia] == b$sig[ib],
    n_depth_filled  = as.numeric(d$n_depth_filled[id]),
    n_depth_changed = as.numeric(d$n_depth_changed[id]))
  out$n_depth_filled[is.na(out$n_depth_filled)] <- 0
  out$n_depth_changed[is.na(out$n_depth_changed)] <- 0
  out$ok <- !is.na(out$n_obs) & !is.na(out$n_pair) & out$n_obs == out$n_pair &
    out$n_pair == out$n_id_pair & out$sig_ok & out$n_depth_changed == 0
  if (!all(out$ok)) {
    bad <- out[!out$ok, , drop = FALSE]
    stop("obs_bio + obs_env do not reproduce obs for ", nrow(bad), " (realm, dataset_key) group(s): ",
         paste(sprintf("%s/%s (obs %s, pair %s%s%s)", bad$realm, bad$dataset_key,
                       ifelse(is.na(bad$n_obs), "absent", bad$n_obs),
                       ifelse(is.na(bad$n_pair), "absent", bad$n_pair),
                       ifelse(bad$sig_ok %in% TRUE, "", ", values differ"),
                       ifelse(bad$n_depth_changed > 0, sprintf(", %s depths changed", bad$n_depth_changed), "")),
               collapse = "; "), call. = FALSE)
  }
  invisible(out)
}

#' Exact polygon membership of every root sample, one layer at a time
#'
#' `ST_Intersects` between the root samples' points and each layer's polygons, chunked per layer so
#' the join never holds more than one layer in memory (the spatial join that OOM-ed the 16 GB server
#' when an app ran it over every layer at once). CRS tags are stripped on both sides through WKB
#' (`ST_Point` tags `OGC:CRS84`, `ST_Read` tags `EPSG:4326`, and DuckDB refuses to intersect across
#' them). Only polygon geometries take part: the maritime-limit layers are boundary *lines* and the
#' ports are points, and a point never intersects either — a layer with no polygons is skipped and
#' reported with `n_polys = 0`. Asserts per layer that no `(root_id, spatial_key)` pair repeats.
#'
#' @param con DuckDB connection with the spatial extension, `sample_root` and `spatial`.
#' @param layers layers to compute (default: every layer in `spatial`).
#' @param tbl output table.
#' @return A tibble with one row per layer: `layer`, `n_polys`, `n_roots`, `n_memberships`.
#' @export
#' @concept release
build_sample_spatial <- function(con, layers = NULL, tbl = "sample_spatial") {
  if (is.null(layers)) layers <- dbGetQuery(con, "SELECT DISTINCT layer FROM spatial ORDER BY layer")$layer
  dbExecute(con, glue("CREATE OR REPLACE TABLE {tbl} (root_id INTEGER, root_sample_key VARCHAR, layer VARCHAR, spatial_key VARCHAR, spatial_name VARCHAR)"))
  dbExecute(con, "CREATE OR REPLACE TEMP TABLE _ss_pts AS
    SELECT root_id, root_sample_key, ST_GeomFromWKB(ST_AsWKB(ST_Point(longitude, latitude))) AS geom
    FROM sample_root
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL AND isfinite(longitude) AND isfinite(latitude)")
  n_pts <- dbGetQuery(con, "SELECT count(*) AS n FROM _ss_pts")$n
  out <- lapply(layers, function(ly) {
    dbExecute(con, glue("CREATE OR REPLACE TEMP TABLE _ss_polys AS
      SELECT spatial_key, layer, name, ST_GeomFromWKB(ST_AsWKB(geom)) AS geom FROM spatial
      WHERE layer = {DBI::dbQuoteString(con, ly)} AND ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')"))
    n_polys <- dbGetQuery(con, "SELECT count(*) AS n FROM _ss_polys")$n
    if (n_polys == 0) return(tibble::tibble(layer = ly, n_polys = 0L, n_roots = 0L, n_memberships = 0L))
    dbExecute(con, glue("INSERT INTO {tbl}
      SELECT p.root_id, p.root_sample_key, y.layer, y.spatial_key, y.name
      FROM _ss_pts p JOIN _ss_polys y ON ST_Intersects(y.geom, p.geom)"))
    s <- dbGetQuery(con, glue("SELECT count(*) AS n, count(DISTINCT root_id) AS n_roots,
      count(*) - count(DISTINCT (root_id, spatial_key)) AS n_dup FROM {tbl} WHERE layer = {DBI::dbQuoteString(con, ly)}"))
    if (s$n_dup > 0) stop(glue("sample_spatial: {s$n_dup} duplicate (root_id, spatial_key) pairs in layer '{ly}'"))
    if (s$n > n_pts * n_polys) stop(glue("sample_spatial: layer '{ly}' has more memberships than points x polygons"))
    tibble::tibble(layer = ly, n_polys = n_polys, n_roots = s$n_roots, n_memberships = s$n)
  })
  dbExecute(con, "DROP TABLE IF EXISTS _ss_polys"); dbExecute(con, "DROP TABLE IF EXISTS _ss_pts")
  dplyr::bind_rows(out)
}

#' The coverage cube behind the explorer's first paint
#'
#' n observations and root samples by dataset, by dataset x station x year, by dataset x year and by
#' dataset x measurement type (with year and depth spans, and — when the `measurement_type` table
#' carries them — the registry's `category` and `variable`); the per-station year x month detail is
#' [build_coverage_stations()], a second sidecar fetched on demand — small enough to paint the grid before
#' DuckDB-WASM wakes up, and the variable-based inventory Task 14 asks for. Since 3.25.0 also `taxa[]`
#' (explorer UI plan D14): one row per taxon of the bio realm — key, names, rank, class, n_obs, year
#' span, life stages and its datasets with n_obs each — so the organism list opens before the engine is
#' warm and *Browse* can list organisms by category or dataset. Deterministic: no wall clock, so a
#' re-run over unchanged inputs writes identical bytes.
#'
#' @param con DuckDB connection holding `obs` (with `dataset_key`, `grid_key`, `datetime`, `taxon_key`,
#'   `life_stage`) and `sample_root`; `taxon` (for `taxa[]`) and `measurement_type` (for the two
#'   variable fields) when present.
#' @param version the release version string.
#' @return A list ready for `jsonlite::write_json(auto_unbox = TRUE)`.
#' @export
#' @concept release
build_coverage <- function(con, version) {
  q <- function(sql) dbGetQuery(con, sql)
  # obs -> root via sample (obs.sample_key may be a child of the root)
  has <- function(t) t %in% DBI::dbListTables(con)
  ocols <- DBI::dbListFields(con, "obs")
  tx_col <- if ("taxon_key" %in% ocols) "o.taxon_key" else "NULL::VARCHAR AS taxon_key"
  ls_col <- if ("life_stage" %in% ocols) "o.life_stage" else "NULL::VARCHAR AS life_stage"
  dbExecute(con, glue::glue("CREATE OR REPLACE TEMP VIEW _cov AS
    SELECT o.dataset_key, o.realm, o.measurement_type, o.grid_key, year(o.datetime) AS year,
           o.depth_min_m, o.depth_max_m, {tx_col}, {ls_col}, r.root_id
    FROM obs o LEFT JOIN sample s USING (sample_key)
    LEFT JOIN sample_root r ON r.root_sample_key = COALESCE(s.root_sample_key, s.sample_key)"))
  datasets <- q("SELECT dataset_key, any_value(realm) AS realm, count(*) AS n_obs, count(DISTINCT root_id) AS n_roots,
                        min(year) AS year_min, max(year) AS year_max
                 FROM _cov GROUP BY dataset_key ORDER BY dataset_key")
  station_ds <- q("SELECT grid_key, dataset_key, count(*) AS n_obs, count(DISTINCT root_id) AS n_roots,
                          min(year) AS year_min, max(year) AS year_max
                   FROM _cov WHERE grid_key IS NOT NULL GROUP BY grid_key, dataset_key ORDER BY grid_key, dataset_key")
  years <- q("SELECT dataset_key, year, count(*) AS n_obs, count(DISTINCT root_id) AS n_roots
              FROM _cov WHERE year IS NOT NULL GROUP BY dataset_key, year ORDER BY dataset_key, year")
  variables <- q("SELECT dataset_key, realm, measurement_type, count(*) AS n_obs, count(DISTINCT root_id) AS n_roots,
                         min(year) AS year_min, max(year) AS year_max,
                         min(depth_min_m) AS depth_min_m, max(depth_max_m) AS depth_max_m
                  FROM _cov GROUP BY dataset_key, realm, measurement_type ORDER BY dataset_key, measurement_type")
  # the registry's category + variable onto variables[] (explorer UI plan D14) — only when the table carries them
  if (has("measurement_type")) {
    mcols <- intersect(c("category", "variable"), DBI::dbListFields(con, "measurement_type"))
    if (length(mcols)) {
      mt <- q(glue::glue("SELECT measurement_type, {paste(mcols, collapse = ', ')} FROM measurement_type"))
      for (cl in mcols) variables[[cl]] <- as.character(mt[[cl]][match(variables$measurement_type, mt$measurement_type)])
    }
  }
  # taxa[]: one row per taxon of the bio realm, its datasets nested, names/rank/class from the taxon reference
  taxa <- list()
  if ("taxon_key" %in% ocols) {
    tx_ds <- q("SELECT taxon_key, dataset_key, count(*) AS n_obs, count(DISTINCT root_id) AS n_roots,
                       min(year) AS year_min, max(year) AS year_max,
                       string_agg(DISTINCT life_stage, '|' ORDER BY life_stage) AS life_stages
                FROM _cov WHERE realm = 'bio' AND taxon_key IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2")
    tx <- if (has("taxon")) {
      tcols <- intersect(c("scientific_name", "common_name", "rank", "class"), DBI::dbListFields(con, "taxon"))
      q(glue::glue("SELECT taxon_key, {paste(tcols, collapse = ', ')} FROM taxon"))
    } else data.frame(taxon_key = character())
    taxa <- lapply(split(tx_ds, tx_ds$taxon_key), function(d) {
      k <- d$taxon_key[1]; j <- match(k, tx$taxon_key)
      get <- function(cl) if (!is.na(j) && cl %in% names(tx)) tx[[cl]][j] else NA_character_
      ls <- sort(unique(unlist(strsplit(d$life_stages[!is.na(d$life_stages)], "|", fixed = TRUE))))
      list(taxon_key = k, scientific_name = get("scientific_name"), common_name = get("common_name"), rank = get("rank"), class = get("class"),
           n_obs = sum(d$n_obs), n_roots = sum(d$n_roots), year_min = suppressWarnings(min(d$year_min, na.rm = TRUE)), year_max = suppressWarnings(max(d$year_max, na.rm = TRUE)),
           life_stages = as.list(ls), datasets = d[, c("dataset_key", "n_obs", "n_roots", "year_min", "year_max")])
    })
    taxa <- unname(taxa)
  }
  dbExecute(con, "DROP VIEW IF EXISTS _cov")
  stations <- lapply(split(station_ds, station_ds$grid_key), function(d)
    list(grid_key = d$grid_key[1], datasets = d[, setdiff(names(d), "grid_key")]))
  list(version = version, datasets = datasets, stations = unname(stations), years = years, variables = variables, taxa = taxa)
}

#' The per-station coverage card: n obs by dataset x year and by dataset x month, for one station
#'
#' What db-viz-station draws when a station is clicked — every dataset sampled there, its rows per
#' year (`years`: `[[year, n], …]`) and per month (`months`: twelve counts) — for all 218 stations.
#' Kept out of `coverage.json` so the first paint stays small; fetched when a station is selected.
#'
#' @inheritParams build_coverage
#' @return A list `{version, stations: [{grid_key, datasets: [{dataset_key, n_obs, year_min, year_max, years, months}]}]}`.
#' @export
#' @concept release
build_coverage_stations <- function(con, version) {
  d <- dbGetQuery(con, "SELECT grid_key, dataset_key, count(*) AS n_obs, min(year(datetime)) AS year_min, max(year(datetime)) AS year_max
                        FROM obs WHERE grid_key IS NOT NULL AND datetime IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2")
  # per (station, dataset): [[year, n]] and the 12 month counts — two small grouped queries, joined in R
  yrs <- dbGetQuery(con, "SELECT grid_key, dataset_key, year(datetime) AS year, count(*) AS n FROM obs
                          WHERE grid_key IS NOT NULL AND datetime IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  mos <- dbGetQuery(con, "SELECT grid_key, dataset_key, month(datetime) AS month, count(*) AS n FROM obs
                          WHERE grid_key IS NOT NULL AND datetime IS NOT NULL GROUP BY 1, 2, 3 ORDER BY 1, 2, 3")
  key <- function(x) paste(x$grid_key, x$dataset_key, sep = "\r")
  yrs_by <- split(yrs[, c("year", "n")], key(yrs)); mos_by <- split(mos[, c("month", "n")], key(mos))
  stations <- lapply(split(d, d$grid_key), function(g) list(
    grid_key = g$grid_key[1],
    datasets = lapply(seq_len(nrow(g)), function(i) {
      k <- paste(g$grid_key[i], g$dataset_key[i], sep = "\r")
      m <- integer(12); mm <- mos_by[[k]]; if (!is.null(mm)) m[mm$month] <- as.integer(mm$n)
      yy <- yrs_by[[k]]
      list(dataset_key = g$dataset_key[i], n_obs = g$n_obs[i], year_min = g$year_min[i], year_max = g$year_max[i],
           years = if (is.null(yy)) list() else lapply(seq_len(nrow(yy)), function(j) c(yy$year[j], yy$n[j])),
           months = m)
    })))
  list(version = version, stations = unname(stations))
}

#' The explorer's boundary-layer sidecar: the registry joined with the release's `spatial` table
#'
#' `metadata/spatial_layers.csv` is the registry of the boundary layers (Erin's sheet: one row per
#' drawable layer with its PMTiles group, default symbology, filter expression and provenance), and
#' the archives at `{pmtiles_base}{dataset_group}.pmtiles` carry the features. The explorer must not
#' hard-code that list nor fetch the CSV from GitHub at runtime (plan 2026-08-31 D23), so each release
#' ships `spatial_layers.json`: the registry verbatim **plus what only the release knows** — each
#' layer's feature count, bbox, its sorted distinct `name`s when there are at most `names_max` (the
#' by-name palette, D24; `NULL` above that, and the app falls back to an id-hash palette), and
#' `n_memberships` (distinct root samples in `sample_spatial`, so the Regions lens can list exactly
#' the layers that can summarize something).
#'
#' @param con DuckDB connection holding `spatial` (and, if built, `sample_spatial`).
#' @param registry_csv Path to `metadata/spatial_layers.csv`.
#' @param version Release version string, stamped into the sidecar.
#' @param pmtiles_base URL prefix of the PMTiles archives (source-layer = `dataset_group`).
#' @param built When the archives were last built (the `ingest_spatial` manifest's mtime) — version
#'   skew between releases and archives is accepted but must be visible.
#' @param names_max Above this many distinct names a layer's `names` is `NULL`.
#' @return A list ready for `jsonlite::write_json(auto_unbox = TRUE)`: `version`, `pmtiles_base`,
#'   `built`, and `layers[]` with `id` (the registry `dataset_id`), `group`, `name` (the human
#'   layer name), `source`, `geom`, `filter` (the registry expression verbatim, as parsed JSON),
#'   the symbology defaults, `name_field`, `description`, `attribution`, `n_features`, `bbox`,
#'   `names`, `n_memberships`.
#' @export
#' @concept explore
build_spatial_layers <- function(con, registry_csv, version, pmtiles_base,
                                 built = NULL, names_max = 200) {
  reg <- readr::read_csv(registry_csv, show_col_types = FALSE, na = c("", "NA"))
  need <- c("dataset_id", "dataset_group", "layer", "group", "geom_type", "filter_expr",
            "line_color", "fill_color", "line_width", "fill_opacity", "default_visible",
            "name_field", "description", "attribution")
  stopifnot("spatial_layers registry is missing columns" = all(need %in% names(reg)))
  sp <- DBI::dbGetQuery(con, "
    SELECT layer, count(*) AS n_features,
           min(ST_XMin(geom)) AS w, min(ST_YMin(geom)) AS s,
           max(ST_XMax(geom)) AS e, max(ST_YMax(geom)) AS n
    FROM spatial GROUP BY 1")
  nm <- DBI::dbGetQuery(con, "SELECT layer, name FROM spatial WHERE name IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2")
  mem <- if ("sample_spatial" %in% DBI::dbListTables(con))
    DBI::dbGetQuery(con, "SELECT layer, count(DISTINCT root_id) AS n FROM sample_spatial GROUP BY 1")
  else data.frame(layer = character(), n = integer())
  missing <- setdiff(reg$layer, sp$layer)
  if (length(missing))
    warning("spatial_layers registry rows with no features in `spatial`: ",
            paste(missing, collapse = ", "), call. = FALSE)
  blank <- function(x) length(x) != 1 || is.na(x) || identical(as.character(x), "")
  chr <- function(x) if (blank(x)) NULL else as.character(x)
  num <- function(x) if (blank(x)) NULL else as.numeric(x)
  layers <- lapply(seq_len(nrow(reg)), function(i) {
    r <- reg[i, ]
    j <- match(r$layer, sp$layer)
    nms <- nm$name[nm$layer == r$layer]
    list(
      id = r$dataset_id, group = r$group, name = r$layer,
      source = r$dataset_group, geom = r$geom_type,
      # the filter expression reaches the style verbatim (a MapLibre expression the registry owns)
      filter = if (blank(r$filter_expr)) NULL else jsonlite::fromJSON(r$filter_expr, simplifyVector = FALSE),
      line_color = chr(r$line_color), fill_color = chr(r$fill_color),
      line_width = num(r$line_width), fill_opacity = num(r$fill_opacity),
      default_visible = isTRUE(as.logical(r$default_visible)),
      name_field = chr(r$name_field), description = chr(r$description), attribution = chr(r$attribution),
      n_features = if (is.na(j)) 0L else as.integer(sp$n_features[j]),
      bbox = if (is.na(j)) NULL else round(c(sp$w[j], sp$s[j], sp$e[j], sp$n[j]), 4),
      names = if (length(nms) >= 1 && length(nms) <= names_max) as.list(nms) else NULL,
      n_memberships = { k <- match(r$layer, mem$layer); if (is.na(k)) 0L else as.integer(mem$n[k]) })
  })
  list(version = version, pmtiles_base = pmtiles_base,
       built = if (is.null(built)) NULL else as.character(built), layers = layers)
}
