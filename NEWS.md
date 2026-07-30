# calcofi4db 2.19.0

- **Fix: shared registries could be silently corrupted by their own round trip**
  (`R/registry.R`, new). `readr::write_csv()` defaults to `na = "NA"`, so an empty
  cell in `metadata/measurement_type.csv` came back as the two-character string
  `"NA"`. That is invisible from R — `read_csv()` reads `"NA"` straight back to
  `NA` — but *not* from DuckDB's `read_csv_auto`, whose default `nullstr` is the
  empty string only. `release_database.qmd` loaded the registry that way, so the
  released `measurement_type` table shipped literal `"NA"` values: 161 rows of
  `_qual_column`, 192 of `_prec_column`, plus `units`, `is_canonical`, `grain` and
  `_source_column`. Nine ingest notebooks wrote the file without `na = ""`; only
  one did it correctly.

  - `check_registry_na_strings()` (new, exported) rejects sentinel strings
    (`"NA"`, `"NaN"`, `"NULL"`, `"N/A"`, `"na"`) in a registry, naming the columns
    and rows and pointing at the cause.
  - `read_measurement_type()` (new, exported) reads the registry **strictly**
    (`na = ""`, so only genuinely empty cells become `NA`) and validates. The
    strict read is load-bearing: a default `read_csv()` converts `"NA"` back to
    `NA`, so no validator downstream of one could ever see the corruption.
  - `register_measurement_types()` (new, exported) replaces the read /
    `bind_rows` / `write_csv` cycle each ingest hand-rolled: appends only genuinely
    new types, never overwrites an existing row, refuses to widen the registry with
    unknown columns, and always writes `na = ""`.
  - `build_metadata_json()` and `collect_measurement_type_mismatches()` now read
    the registry through `read_measurement_type()`, so a corrupted file fails
    rather than reaching the schema site's `metadata.json`.

# calcofi4db 2.18.0

- **New: hold an in-progress ingest out of the release** — an ingest notebook can
  now declare `in_release: false` in its `calcofi:` YAML block. It still runs in
  the pipeline and writes its full `data/parquet/{provider}_{dataset}/` outputs
  (tables, `manifest.json`, `relationships.json`, `metadata.json`), but every
  release-side discovery step skips it, so a dataset under review cannot leak
  into a frozen release.

  The flag is **opt-out**: a notebook with no `in_release:` key is in the release,
  so existing ingests are unaffected.

  - `release_excluded_datasets()` (new, exported) resolves the flagged-out
    `provider_dataset` labels from the notebooks in a workflows directory.
  - `read_ingest_yaml()` gains `in_release_only` (default `FALSE`).
  - `parse_qmd_frontmatter()` returns a new `in_release` logical column.
  - `build_release_table_registry()` omits flagged-out ingests entirely.
  - `core_shard_paths()`, `assemble_core_table()`, `merge_taxon_shards()` and
    `assemble_core()` gain `exclude`, defaulting to `release_excluded_datasets()`,
    so a flagged-out dataset's core shards are never unioned into the release.
  - `build_targets_list()` no longer makes a flagged-out ingest an `[auto]`
    dependency of the release caboose. The release ignores its outputs, so the
    edge only served to invalidate and re-freeze the whole release whenever an
    in-progress ingest changed. The ingest still runs as its own target.

# calcofi4db 2.17.0

- **New: dataset-agnostic netCDF planning** — `discover_sample_levels()`,
  `plan_dataset_netcdf()` and `summarise_netcdf_plan()` (`R/netcdf.R`). These
  recover a dataset's sampling hierarchy by walking the core `sample`
  `parent_sample_key` adjacency list, and decide whether it publishes as a flat
  CF Discrete Sampling Geometry profile (`featureType=profile`) or as nested
  netCDF-4 groups — with no per-dataset configuration.

  This replaces judgement that was previously hardcoded once per dataset in each
  `publish_{dataset}_to-netcdf.qmd`. The old rationale ("the nesting differs per
  dataset, which is why these are notebooks rather than one generic script")
  predates the consolidated core: now that every ingest emits `sample` with
  `sample_type` + `parent_sample_key`, the nesting is *data* rather than code, so
  one generic publish step can serve every dataset.

  `plan_dataset_netcdf()` returns `measurement_types` as the union across the
  whole dataset, which is a fix as much as a feature: the published
  `ctd-cast_full.nc` declared 32 of 54 measurement types because that notebook
  inferred its variable list from a single cruise partition (bottle nutrients were
  not folded into the CTD files until 2008, so the alphabetically-first 1998
  cruise had no column for them, and every later-introduced type — including all
  `btl_*` nutrients — was silently absent from a file advertised as full
  resolution).

  Failure modes are surfaced rather than swallowed: unresolved parents are counted
  as `n_orphan` instead of being dropped, a level's parent is a majority vote so a
  single mislabelled row cannot invent a level, self-referential (within-level)
  chains are not treated as nesting, and a genuine cycle errors instead of
  hanging.

# calcofi4db 2.16.1

- **Fix: drop the redundant `bottom_depth` arm** added to
  `.sample_measurement_arm_sql("calcofi_bottle")` in 2.14.0. `bottom_depth`
  already reaches `sample_measurement`: the bottle ingest pivots the source
  `Bottom_D` column into `cast_condition` (33,363 rows) and drops it from
  `casts`, so the extra `UNION ALL SELECT ... bottom_depth_m FROM casts` was both
  duplicative and a binder error against a column that no longer exists by the
  time the arm runs. `create_compat_views()` no longer filters `bottom_depth` out
  of the rebuilt `cast_condition` either — it is a genuine cast condition, and
  excluding it silently dropped a real row.

  Registering `bottom_depth` in `metadata/measurement_type.csv` (workflows) was
  still required and is unaffected: the vocabulary genuinely lacked it, which the
  release FK check on `sample_measurement.measurement_type` now catches.

# calcofi4db 2.16.0

- **`create_compat_views()` rebuilds `casts` and `bottle`** from the core, and
  gained a `sample_tbl` argument. `calcofi_dic` matches its samples against
  `calcofi_bottle`'s cast/bottle event tables (`match_by_site_datetime()` then
  `match_nearest_by_depth()`), which stopped existing once bottle began
  publishing the core — the one ingest that depends on another's *event* tables
  rather than just the shared references. `cast_id`/`bottle_id` come back from
  the namespaced `sample_key` and the cast FK from `parent_sample_key`.

  `sample_tbl` matters for correctness, not convenience: dic builds its own
  `sample` later in `emit_core_tables()`, so loading bottle's shard as plain
  `sample` would have it replaced mid-render and the views would break. dic
  loads it as `_bottle_sample` and points the views there.

# calcofi4db 2.15.0

- **`sync_to_gcs()` transfers in parallel by default** (`parallel = TRUE`). It
  previously spawned one `gcloud storage cp` process per file and one
  `gcloud storage rm` per stale object, so an ingest with a Hive-partitioned
  table serialised its whole upload — `obs_ctd_full` is 96 partitions / 4.9 GB,
  and on a slow link that upload ran at ~1.3 MiB/s and dominated the ingest's
  wall clock. The default path now issues a single `gcloud storage rsync -r`,
  which transfers concurrently and applies `delete_stale` via
  `--delete-unmatched-destination-objects`. The per-file path remains at
  `parallel = FALSE` for callers that need the per-file action tibble; its stale
  deletes are now batched into one `rm` invocation.

# calcofi4db 2.14.0

- **`site_key` and `order_occ` promoted onto the core `sample` table.** Both are
  event-level and cross-dataset — `site_key` appears on 13 of the 18 source event
  tables and is the station natural key (`grid_key` is the *derived* grid cell,
  not the source's own id); `order_occ` is the order of station occupation.
  Previously both were dropped by consolidation, which made `site`, `casts` and
  `ctd_cast` unreconstructable from the core. Source spelling varies (`order_occ`
  vs `ord_occ`) and CTD stores it as text, so it is normalised to `INTEGER`.
  `tow`/`net` inherit both from their parent site, as they already do for
  `grid_key`/`cruise_key`.

- **`bottom_depth_m` now projects into `sample_measurement`** as `bottom_depth`
  on the cast event — it describes the sampling event (how deep the water was),
  not an observation, so it belongs with the other event-level effort measures
  rather than in `obs`. `create_compat_views()` excludes it when rebuilding
  `cast_condition`, so no phantom condition row appears.

- **`create_compat_views()`** rebuilds the retired per-dataset tables as VIEWs
  over the core: the source id from the namespaced `sample_key`, the containment
  FK from `parent_sample_key`, event effort by pivoting `sample_measurement` out
  of long form, and the measurement triples from `obs`. Verified against the
  shipped data — `net` (76,512), `tow` (75,506) and `site` (61,104) round-trip
  identically for every column the core models. It is **exact for those columns
  and lossy for the rest**; see `?create_compat_views` for what does not come
  back (notably CTD scan-grain columns, since `sample` holds one row per physical
  cast).

- Fixed `.sample_arm_self()` emitting `site_key AS site_key`, which DuckDB
  resolves against the alias being defined in the same `SELECT` (lateral column
  alias) rather than the source column; all caller-supplied expressions are now
  table-qualified.

# calcofi4db 2.13.0

- **`emit_core_tables()` is now the authoritative core projection.** It gains
  `measurement_taxon` / `overrides` / `taxa` arguments and builds this dataset's
  slice of `taxon` / `dataset_taxon` / `taxon_group`, so `obs.taxon_key` resolves
  at ingest time. Each ingest can now emit the consolidated core as its parquet
  output instead of per-dataset tables that `release_database.qmd` re-derives.

- **Realigned four `obs` arms that had drifted from the release projection.**
  The projection existed twice — here and inline in `release_database.qmd` — and
  the copies had separated:
  - `calcofi_bird_mammal_census`: the headline is one row per (transect, species)
    with `count` SUMmed across behaviors, and the behavior breakdown moves to
    `obs_attribute` (with `bin_label` from `bird_mammal_behavior`). Previously
    behavior rode on the headline's `life_stage`, counting the same birds once
    per behavior code.
  - `calcofi_phytoplankton`: **new arm** — the region-pooled `obs` projection
    existed only in the release, so the per-ingest projection emitted no
    phytoplankton observations at all.
  - `swfsc_cufes` / `calcofi_phyllosoma`: decompose the taxon out of the
    measurement type name via the new `_measurement_taxon` registry, yielding a
    real `taxon_key` + canonical type + `life_stage` (and, for phyllosoma,
    routing the per-stage counts to `obs_attribute` rather than the headline).
  - `cce-lter_euphausiids`: unchanged here, but a regression test now pins the
    species x life-stage grain. The release arm still decomposed via
    `measurement_taxon`, which collapses all 37 BTEDB species to family
    Euphausiidae and drops `life_stage`.

- **`core_output_tables()`** returns the non-empty core shards an ingest should
  write to parquet, so datasets without attribution/effort/taxa do not emit
  empty files.

# calcofi4db 2.12.0

- **`calcofi_mets` projects into the core model.** Underway TSG/meteorology now
  emits `sample` at the existing `underway` grain (the one `swfsc_cufes` already
  uses) and an `env`-realm `obs` fed by `mets_thin` — the same thinned-table
  pattern `calcofi_ctd-cast` uses, where `obs` carries `ctd_thin` rather than the
  full scan set. `sample` is restricted to the samples `mets_thin` references, so
  the event dimension stays proportionate to `obs` instead of carrying the full
  ~1-minute series; that remains a supplemental parquet output. Depth is recorded
  as surface pending the hull-intake depth (workflows questions.csv mets_25).

# calcofi4db 2.11.0

- **`derive_cruise_key_on_casts()` gains `table_name =`.** It previously required
  a table literally named `casts`; any other dataset had to rename its table or
  hand-roll the same SQL. It now annotates whichever table you name (default
  `"casts"`, so existing calls are unchanged), needing only a `ship_code` column
  and `datetime_col`. A `ship_name` column is used for the unmatched-ship report
  when present and treated as NULL when absent, so bottle/underway-grain tables
  that carry only an embedded ship code work directly. Interpolated ship values
  are now quoted with `DBI::dbQuoteString()`.
- **Core arms for two new datasets.** `ucsd_sio_mesopelagic-fish` (MOHT trawl,
  self-leaf `tow` grain, `bio` realm, `taxon_key` crosswalked from the source's
  scientific names via a new `mesopelagic_fish_taxon` arm in
  `build_dataset_taxon()`) and `cce-lter_picoplankton-bacteria` (self-leaf
  `bottle` grain, `env` realm — the four flow-cytometry counts are a measurement
  vocabulary, not taxa) now project into `sample` + `obs`, so both reach the
  frozen release instead of stopping at per-dataset parquet.
- **`emit_core_tables()` no longer requires `dataset_taxon` to pre-exist.** Every
  bio arm `LEFT JOIN`s `dataset_taxon`, but that crosswalk is built centrally by
  the release (`build_dataset_taxon()`), so calling `emit_core_tables()` from an
  ingest raised `Catalog Error: Table with name dataset_taxon does not exist` for
  ichthyo / zoodb / zooscan / bird_mammal / euphausiids. An empty stub is now
  created when absent: the ingest-local projection runs with `taxon_key` NULL and
  the release resolves it for real.
- **`euphausiids` projects into the core with real taxonomy.** The species- and
  life-stage-resolved BTEDB export replaces the old single-`Abundance` column, so
  `.obs_arm_sql("cce-lter_euphausiids")` now resolves `taxon_key` through
  `dataset_taxon` and carries `life_stage` on the `obs` headline (as zoodb /
  zooscan do) instead of leaving both NULL. `build_dataset_taxon()` /
  `build_taxon_reference()` gained a `euphausiids_taxon` source arm, so the 37
  BTEDB species crosswalk to WoRMS AphiaIDs rather than resolving through
  `metadata/measurement_taxon.csv`.

# calcofi4db 2.10.0

- **`tow_type` (net gear) promoted into the core `sample` table.**
  `build_sample_reference()` / `append_sample()` now carry a `tow_type` column
  (added to the `sample` schema): the CalCOFI ichthyo net gear code
  (`C1`/`CB`/`CV`/`PV` oblique & vertical tows, `MT` manta surface tows),
  denormalized onto both the `tow` and `net` sample rows and `NULL` for gears /
  datasets without one. Consumers (e.g. `db-viz-hex` CPUE) can now read net gear
  straight from `sample` instead of re-deriving it from per-dataset ingest tables.

# calcofi4db 2.9.0

- **Unified taxon model** (new `R/taxa.R`): `build_taxon_reference()`,
  `build_dataset_taxon()`, `build_taxon_group()`, and `taxon_key_of()` collapse the
  per-dataset taxon tables (`species`, the `taxon` hierarchy, `phyto_taxon`,
  `zoodb_taxon`, `zooscan_taxon`, `bird_mammal_species`) into a single `taxon`
  reference keyed by an authority-prefixed `taxon_key` (`worms:<worms_id>`, or
  `itis:<itis_id>` for birds), a `dataset_taxon` crosswalk (per-dataset vocabulary
  → `taxon_key`), and a `taxon_group` grouping table. Cross-dataset duplicates
  (same AphiaID) collapse to one row. Coarse/composite taxa resolve to real
  WoRMS/ITIS ids via caller-supplied `measurement_taxon` / `overrides` registries.
- **`append_obs_freq()` → `append_obs_attribute()`** (table `obs_freq` →
  `obs_attribute`): generalizes the (bin, count) frequency table to any
  sub-occurrence attribution — length-/stage-frequency plus categorical breakdowns
  such as seabird behavior. Columns unchanged (`bin_value`/`bin_label`/`count`).
- **`obs.taxon_id` → `obs.taxon_key`** in the `obs` / `obs_attribute` DDL and the
  `append_*` helpers; the bio `emit_core_tables()` arms resolve the global
  `taxon_key` via `dataset_taxon` instead of emitting dataset-local ids.

# calcofi4db 2.8.2

- **`merge_metadata_json()`** adds each dataset's `workflow_url` (from the ingest `calcofi:` YAML) to its `datasets[]` entry, so the schema site can link the rendered ingest notebook next to the calcofi.org / data-source links.

# calcofi4db 2.8.1

- **Content-hash dedup ignores provenance columns** — the per-table/partition signature now always excludes `_source_file`, `_source_row`, `_source_uuid`, and `_ingested_at` (even when `strip_provenance = FALSE`). Otherwise `_ingested_at` (set to the current time on every ingest) made every table look changed, defeating the dedup for tables exported with provenance.

# calcofi4db 2.8.0

*Content-hash dedup of parquet uploads + Parquet V2 / zstd defaults*

- **`write_parquet_outputs()` content-hash dedup** — computes an order-independent content signature per table (and per partition for partitioned tables), stored in `manifest.json` as `data_hash`. On re-run, unchanged tables/partitions are **reused from the previous run** instead of being re-written and re-uploaded. A few new cruises (or a metadata-only change) now rewrite only the affected partitions, not all 15 GB of `ctd_measurement`. Replaces the previous coarse row-count check that forced a full-table rewrite whenever any partition value changed.
- **Parquet V2 + zstd defaults** — `COPY TO` now writes `PARQUET_VERSION V2` and defaults `compression = "zstd"` (was `"snappy"`) for better compression at minimal cost. Native DuckDB GEOMETRY (v1.5+) round-trips correctly under both. The encoding is recorded in `manifest.json` as `parquet_format`; a format change forces a one-time full rewrite so the new encoding actually applies (content hashes track data, not file bytes). `ROW_GROUP_SIZE_BYTES` is intentionally not set on these writes because it requires `preserve_insertion_order=false`, which conflicts with ordered output.
- **`primary_keys` parameter** — optional named list (table → PK column) appended as a final `ORDER BY` tiebreaker for a stable total order (better row-group statistics; byte-stable single-file outputs).
- **`sync_to_gcs()` crc32c fix** — `gcloud storage hash` is now called without the removed `--crc32c` flag (rejected by gcloud ≥ 5xx), which had silently degraded change detection to a size-only comparison.

# calcofi4db 2.7.1

- **`parse_qmd_frontmatter()`** now reads the whole file when locating the YAML front matter delimiters instead of only the first 50 lines, so workflows with long `calcofi:` blocks (e.g. `dataset_meta` + `additional_datasets`) are parsed and not silently dropped from the targets pipeline / release registry.

# calcofi4db 2.7.0

*YAML-authoritative dataset metadata, per-dataset contributions, and richer release sidecars*

- **`read_ingest_yaml()` / `read_calcofi_meta()`** read the `calcofi:` YAML block from `ingest_*.qmd` workflows — the authoritative source for `provider`/`dataset`, `dataset_meta`, `tables_owned`, `workflow_url`, and `erd.color`. Replaces `metadata/dataset.csv`.
- **`ingest_yaml_to_dataset_df()`** rebuilds the in-database `dataset` registry table from the ingest YAML (including `additional_datasets:` folded into one ingest, e.g. `swfsc_invert`), so ingests no longer read `dataset.csv`.
- **`build_metadata_json()`** gains `tables_owned` — emits a `contributions` block (per-table `COUNT(*)`, `owned`/`shared` flags) for owned tables only, avoiding mis-attribution of reference tables loaded from prior ingests. Per-ingest schema bumped to `"1.1"`.
- **`merge_metadata_json()`** now (a) builds the `datasets` block from `ingest_yaml=` (authoritative; `dataset_csv=` kept as deprecated fallback), (b) propagates each table's `workflow` link, (c) aggregates a release-level `contributions` block (rows + `pct` per dataset, with `over_attributed` flag and `table_rows=` denominators), (d) adds `erd_legend`, `datasets[].tables`, and `measurement_types[].datasets` (from `_source_datasets`). Release schema bumped to `"1.2"`. All new fields are additive.

# calcofi4db 2.6.2

*Invert consolidation, pipeline exclusions, and missing species corrections*

- **`consolidate_ichthyo_tables()`** gains `invert_tbl` parameter — folds Ed Weber's `inverts.csv` into the unified `ichthyo` table with `life_stage = "invert"`.
- **`build_targets_list()`** gains `exclude` parameter — skip targets by name (e.g., `exclude = "ingest_calcofi_ctd-cast"`). Excluded targets are also stripped from other targets' dependency lists. Normalizes hyphens to underscores for matching.
- **`apply_data_corrections()`** adds 6 missing invert species (including Market squid, *Doryteuthis opalescens*) sourced from ERDDAP `erdCalCOFIinvcnt`. Dynamically matches columns to avoid errors when `gbif_id` hasn't been added yet.

# calcofi4db 2.6.1

*Sorted parquet output with ST_Hilbert spatial ordering*

- **`sort_by` parameter** `write_parquet_outputs()` gains a `sort_by` named list to specify row ordering per table. Sorted row groups enable predicate pushdown (min/max statistics skip irrelevant chunks).
- **Hilbert spatial sort** Use `"hilbert:lon_col,lat_col"` syntax in `sort_by` to order rows by `ST_Hilbert()` curve position — clusters spatially nearby records for fast bounding-box queries.
- **`paste0()` in COPY TO** SQL construction in `write_parquet_outputs()` uses `paste0()` instead of `glue::glue()` to prevent cli `{variable}` interpolation errors when propagating through targets.
- **sort_by in manifest.json** Sort specifications recorded alongside `partition_by` for downstream consumers.

# calcofi4db 2.6.0

*Native GEOMETRY storage via DuckDB v1.5 — removes spatial workaround*

- **`storage_compatibility_version = 'latest'`** `get_duckdb_con()` now sets this in the default config, enabling DuckDB v1.5's native built-in GEOMETRY type. This fixes the "Buffer overflow" / "Skipping beyond end of binary data" spatial serialization bug that occurred with the old v0.10.2 storage format.
- **Removed geom_wkb workaround** `assign_grid_key()` no longer refreshes grid geometry from a stored WKB column — native GEOMETRY storage is reliable.
- **Requires `duckdb >= 1.5.1`** Added minimum version constraint in DESCRIPTION to ensure the native GEOMETRY type is available.
- **Avoid glue in spatial.R** `assign_grid_key()` uses `paste0()` instead of `glue::glue()` to prevent cli from intercepting `{variable}` patterns in error messages propagated through targets.

# calcofi4db 2.5.6 (superseded)

*Grid geometry refresh workaround for DuckDB spatial bug (removed in 2.6.0)*

# calcofi4db 2.5.5

*Server-side GCS copy for archives & sync_to_gcs replaces put_gcs_file loops*

- **Server-side archive copy** `.sync_to_gcs_archive()` now checks `_sync/{provider}/{dataset}/` on GCS before uploading from local. If a file exists with matching MD5, uses `copy_gcs_file()` for instant server-side copy — no local I/O or GD mount needed.
- **`copy_gcs_file(src, dst)`** New helper for server-side GCS-to-GCS copy via `gcloud storage cp`.
- **Bottle & DIC uploads** replaced `put_gcs_file()` loops in QMDs with `sync_to_gcs()` for hash-based deduplication (idempotent re-renders).

# calcofi4db 2.5.4

*Consolidated `sync_to_gcs()` with archive mode, exclude patterns & GCS logging*

- **Unified sync function** `sync_to_gcs()` gains `archive`, `exclude`, and `log_to_gcs` parameters. When `archive = TRUE`, creates timestamped immutable snapshots (replacing `sync_to_gcs_archive()` internals). When `FALSE` (default), standard mirror mode.
- **Exclude patterns** New `exclude` parameter accepts glob patterns (e.g., `c(".DS_Store", "*.tmp")`) to skip files during sync.
- **GCS action logging** `log_to_gcs = TRUE` writes a timestamped JSON log to `gs://{bucket}/{prefix}/_logs/sync_YYYY-MM-DD_HHMMSS.json` documenting every upload, skip, and delete.
- **Richer results** Sync results tibble now includes `size` and `reason` columns (e.g., "checksum match", "new file", "crc32c changed").
- **`sync_to_gcs_archive()` deprecated** Now a thin wrapper calling `sync_to_gcs(archive = TRUE)`. Existing callers work unchanged.

# calcofi4db 2.5.3

*DuckDB driver lifecycle, idempotent ingestion & defensive ALTER TABLE*

- **DuckDB driver lifecycle** `get_duckdb_con()` now creates a named driver via `duckdb::duckdb(dbdir=...)` and stores it as an attribute; `close_duckdb()` calls `duckdb_shutdown()` for proper WAL flush. Also sets `autoload_known_extensions = "true"` so the spatial extension loads during WAL replay.
- **Idempotent DuckLake ingestion** `ingest_to_working()` checks `_source_file` before appending — skips if rows from the same source already exist, making notebook re-renders safe.
- **Defensive `ADD COLUMN IF NOT EXISTS`** All `ALTER TABLE … ADD COLUMN` calls across `load_prior_tables()`, `load_gcs_parquet_to_duckdb()`, `standardize_species_local()`, `standardize_species()`, `finalize_ingest()`, `create_cruise_key()`, `propagate_natural_key()`, `assign_sequential_ids()`, and `replace_uuid_with_id()` now use `IF NOT EXISTS` to prevent errors on re-runs.
- **Better duplicate-key warnings** `create_cruise_key()` now shows top-10 examples with counts in the warning message.

# calcofi4db 2.5.2

*VIEWs for dependencies, GCS server-side copy, crc32c sync & spatial consolidation*

- **VIEW-based dependency loading** `load_prior_tables()` gains `as_view` parameter — creates VIEWs instead of TABLEs for zero-copy parquet reads. Dependency tables no longer duplicated across ingests.
- **`calcofi.modifies` frontmatter** New YAML field declares which dependency tables an ingest modifies (e.g., `ship`). `parse_qmd_frontmatter()` parses it; `build_release_table_registry()` discovers `_new` delta sidecars from the filesystem.
- **GCS server-side copy for releases** `release_database.qmd` copies parquet from `ingest/` to `releases/` on GCS via `gcloud storage cp` instead of re-uploading from local. Only derived/merged tables exported locally.
- **crc32c hash comparison** `sync_to_gcs()` uses `gcloud storage ls --json` for crc32c hashes; `list_gcs_files()` returns `crc32c` column. Unchanged files skipped entirely.
- **Stale file cleanup** `sync_to_gcs()` gains `delete_stale` parameter to remove orphaned GCS files after partition key or table renames.
- **`export_parquet()`** New helper using DuckDB native `COPY TO PARQUET` — handles GEOMETRY columns (as WKB), preferred over `arrow::write_parquet()`.
- **`build_release_table_registry()`** Auto-discovers table-to-ingest mapping from manifests with canonical source marking for duplicates.
- **Archive listing fix** `get_latest_archive_timestamp()` uses non-recursive `gcloud storage ls` instead of recursive `--json` scan that was hanging on large archives.

# calcofi4db 2.5.1

*Mismatch tracking, supplemental table support, targets integration & bug fix*

- **New mismatch collectors** Added `collect_ship_mismatches()`, `collect_measurement_type_mismatches()`, and `collect_cruise_key_mismatches()` to detect unresolved entities and populate `manifest.json` mismatches section.
- **Supplemental table support** `write_parquet_outputs()` gains `mismatches` and `supplemental` parameters; `load_prior_tables()` and `finalize_ingest()` gain `include_supplemental` to exclude supplemental tables (e.g. wide-format ERDDAP outputs) by default.
- **New spatial manifest** Added `write_spatial_manifest()` to generate `manifest.json` for spatial parquet directories.
- **New ship helper** Added `ensure_interim_ships()` to insert placeholder ship entries for unmatched codes so downstream FK joins can proceed.
- **Targets integration** Added `parse_qmd_frontmatter()` and `build_targets_list()` to build a `targets` pipeline from `calcofi:` YAML frontmatter in `.qmd` workflow files. Added `yaml` to Imports and `targets` to Suggests.
- **Relationships refactor** `build_relationships_json()` now accepts a `rels` list as an alternative to a `dm` object, removing the hard dependency on the `dm` package.
- **Partition change detection** `write_parquet_outputs()` now detects when partition values change and forces a re-write.
- **Bug fix** Fixed `print_csv_change_stats()` using `fields_added` instead of `fields_removed` when counting removed fields.

# calcofi4db 2.5.0

*Simplified provider/dataset naming, taxonomy & workflow improvements*

- **Dataset renaming** Renamed dataset providers from URL-style to short names (e.g., `swfsc.noaa.gov/calcofi-db` -> `swfsc/ichthyo`, `calcofi.org/bottle-database` -> `calcofi/bottle`); moved corresponding `inst/ingest/` config files to match.
- **New taxonomy functions** Added `standardize_species_local()` for fast local species standardization via `spp.duckdb` with optional WoRMS API fallback. Added `build_taxon_hierarchy()` to build taxonomic hierarchies from local `spp.duckdb` using recursive CTEs.
- **New workflow function** Added `finalize_ingest()` high-level function to push parquet tables to Working DuckLake with provenance tracking.
- **New cloud helpers** Added GCS cleanup helpers: `delete_gcs_prefix()`, `cleanup_gcs_obsolete()`.
- **New display helper** Added `dt()` display helper for interactive DataTables with CSV export.
- **New wrangle helpers** Added relationship JSON helpers: `build_relationships_json()`, `merge_relationships_json()`, `read_relationships_json()`. Added `assign_deterministic_uuids_md5()` using DuckDB-native md5.
- **Improved `sync_to_gcs()`** to support recursive/hive-partitioned subdirectories.

# calcofi4db 2.4.0

*Use _uuid over _id, smarter sync with GCS*

* Revert from int `_id` to `_uuid` preferred unique identifiers for SWFSC icthyo db
* Use smarter synchronizing with GCS using md5 hash checks and modified time filenaming

# calcofi4db 2.3.0

*Addition of ship, taxonomy functions*

Added helper functions for processing:

- ships: `fetch_ship_ices()`, `match_ships()`, `add_ship_info()`.
- taxonomy: `build_taxon_table()`, `standardize_species()`

# calcofi4db 2.2.1

*Addition of spatial, parquet, viz helper functions*

- Added functions to help with spatial data processing including: `add_point_geom()`, `assign_grid_key()`.
- Added parquet helper function: `load_gcs_parquet_to_duckdb()`.
- Added ingest workflow helper visualzation of table function: `preview_tables()`.

# calcofi4db 2.2.0

*Improvements to cloud plan functions*

Workflow ingest_swfsc.noaa.gov_calcofi-db.qmd now fully automates ingestion of 
CalCOFI database from SWFSC NOAA archive to parquet files in Google Cloud Storage.
Many new functions added.

# calcofi4db 2.1.0

*Addition of functions for phase 2 of cloud plan*

- Added ducklake and freeze functions. Updated documentation with concepts.

# calcofi4db 1.2.0

*Addition of functions for phase 1 of cloud plan*

# calcofi4db 1.1.0

*Addition of CalCOFI Bottle Database*

# calcofi4db 1.0.0

*Initial production release with NOAA CalCOFI Database*

* Complete NOAA CalCOFI Database ingestion with spatial features
* Add synchronized versioning system for package and database
* Create master ingestion workflow with integrity checks
* Implement comprehensive metadata management

# calcofi4db 0.1.1

* Fix `detect_csv_changes()` to compare CSV files with `read_csv_files()` output.
  * Add type mismatch checks for fields in the CSV files.
* Add `print_csv_change_stats()` functions for textual summary of changes.
* Add `display_csv_changes()` to display changes in a color-coded table and 
    * Ensure compatibility with multiple output formats: interactive DataTable, static kable, or raw tibble.
* Expand documentation for `read_csv_files()` and `detect_csv_changes()`.

