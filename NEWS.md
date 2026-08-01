# calcofi4db 3.3.0

## The QA/QC rule engine moves into the package

The engine that runs `workflows/metadata/qc_rules/` (`rules.csv` + `sql/*.sql`)
was a private copy inside `apps/ctd-qaqc/R/rules.R` while it had one caller. It
now has two — the app, and `ingest_calcofi_ctd-cast.qmd`, which reports the
condition of the data it just published — so it lives here instead, with tests.
Two copies of a scientific rule is the same drift that the per-dataset
core-projection `switch()` arms produced.

- **`qc_read_rules()`** — read a rule registry, attaching each rule's SQL text and
  parsed `params`. Refuses a registry it cannot execute: an active rule with no
  `sql_file`, or one pointing at an absent file, errors at read time rather than
  becoming a rule that quietly checks nothing.
- **`qc_run_rule()` / `qc_run_all()`** — execute rules. An unmet precondition
  (`requires_types` absent from `obs`, or a `scope = "cruise"` rule with no cruise)
  returns `skipped`, **never** a zero-row pass. A rule that reports green without
  having checked anything is worse than no rule.
- **`qc_summarize()`** — one row per rule with a `status` of `pass` / `flag` /
  `FAIL` / `ERROR` / `skip`.
- **`qc_parse_params()` / `qc_render_sql()`** — the `k=v;k=v` params cell and
  `{{placeholder}}` substitution. An unsupplied placeholder errors.
- **`qc_stage_reference()`** — stage the reference data the rules join against
  (`measurement_type` from the workflows registry, `measurement_qual`, the
  Access-master climatology/station tables, and a GEBCO-derived `sample_seafloor`)
  onto one connection. A missing input is left as a **missing table**, so its rules
  error rather than returning zero rows and reading as clean.

New `Suggests: terra` (only for `qc_stage_reference(gebco_tif = )`).

## Input fingerprinting — skip an ingest's heavy path when nothing changed

An ingest is re-rendered for reasons that have nothing to do with its inputs: a
narrative edit, a new diagnostic, a fixed typo. Re-running an hour of download →
parse → pivot for those buys nothing, and it stops the notebook being usable as a
living document — you do not add a paragraph to something that takes an hour to
check. Same idea as `write_parquet_outputs()`'s per-table content hash, lifted one
level up to the whole ingest.

- **`input_fingerprint()`** — hash the source list and metadata registries an
  ingest's outputs depend on. A **missing** file is recorded as `"<missing>"`
  rather than skipped, so deleting a registry invalidates the outputs.
- **`write_input_fingerprint()` / `read_input_fingerprint()`** — record and recall
  it beside the parquet. An absent or corrupt state file reads as `NULL`, which
  falls through to a full run rather than erroring.
- **`changed_inputs()`** — name *which* inputs moved, so a rebuild says why.

## Also in this release

- **`supplemental_core_tables()`** — reads every ingest's `calcofi.tables_owned`
  and returns the tables flagged `supplemental: true` (`obs_ctd_full`,
  `obs_mets_full`): the full-resolution products hosted alongside the thinned core
  but hidden from the default table list and the ERD. Only `obs`-shaped tables are
  returned, since [assemble_core()] renumbers `obs_id` and orders by the core's
  columns — `calcofi_mets` previously declared the raw `mets_measurement` here,
  which carried neither an `obs_id` nor a coordinate. `release_database.qmd`
  depends on it.

## Provider renames (breaking: three `dataset_key` values change)

Provider is the **curating organization** — not the hosting portal, and not a
collection or lab within the org. Three keys were wrong on that test and are now
corrected, in `.taxon_norm_sources()`, `merge_taxon_shards()`'s priority vector
and the tests:

| from | to | why |
|---|---|---|
| `calcofi_bird_mammal_census` | `farallon_bird-mammal` | Farallon Institute (William Sydeman, PI) — CalCOFI is the sampling program, not the curator |
| `pic_zooplankton` | `sio_pic-zooplankton` | the Pelagic Invertebrate Collection is the *dataset*; SIO is the org |
| `ucsd_sio_mesopelagic-fish` | `sio_mesopelagic-fish` | redundant prefix |

`dataset_key` is stamped on every `obs` / `sample` row and is what consumers
filter on, so this changes the released data, not just file names. See the
workflows repo for the matching notebook/registry/GCS renames.

# calcofi4db 3.1.0

## Taxon lineage: taxa no longer reach the release as bare keys

`build_taxon_reference()` takes `rank` / `parent_taxon_key` / classification from
a DwC-shaped hierarchy table named `taxon` in the connection. **Exactly one ingest
ever built one** — `swfsc_ichthyo`, via `build_taxon_hierarchy()` over its own
species list. Every other dataset's taxa therefore reached the release with a
`taxon_key` and a `scientific_name` and nothing else:

| dataset | taxa | with `rank` | with `parent_taxon_key` |
|---|---|---|---|
| `swfsc_ichthyo` | 1687 | 1687 | 1686 |
| `calcofi_bird_mammal_census` | 128 | 32 | **0** |
| `ucsd_sio_mesopelagic-fish` | 90 | 90 | **0** |
| `cce-lter_euphausiids` | 38 | **0** | **0** |
| `cce-lter_zoodb` | 33 | 33 | **0** |
| `calcofi_phytoplankton` | 26 | 11 | **0** |
| `swfsc_cufes` | 6 | **0** | **0** |
| `cdfw_dungeness-crab` | 3 | **0** | **0** |
| `calcofi_phyllosoma` | 1 | **0** | **0** |

So a hierarchy rollup — "all Decapoda" — silently returned nothing for the
*Metacarcinus magister* records, and **no error was raised anywhere along the
way**. `family` was populated by no dataset at all, ichthyo included.

**New: `ensure_taxon_lineage(con, measurement_taxon, overrides, cache_csv)`.**
Resolves every authority id this dataset's vocabulary reaches — from its own taxon
tables *and* from `measurement_taxon.csv`, which is where the wholly-bare taxa came
from — fetches each one's WoRMS classification (or ITIS, for the Aves-keyed
seabirds), and stages it as that same `taxon` hierarchy table. Call it before the
three builders; `build_taxon_reference()` needs no new argument, because it already
reads that table as its authority. An existing hierarchy is **merged, not
replaced**, so `swfsc_ichthyo` keeps what it builds and gains only what is missing.

**New: `fetch_taxon_lineage()`**, the cache underneath it. One row per (requested
taxon, ancestor-or-self), written to a reviewable CSV
(`metadata/taxon_lineage.csv` in the workflows repo) so a re-run costs no API calls
and works offline. A taxon the authority cannot resolve stays bare rather than
aborting the other three hundred. The cache is **global and shared**, but the
return value is scoped to the ids asked for — returning the whole cache put every
dataset's lineage into every shard (`calcofi_phyllosoma`: 1 taxon becoming 2,101),
and only `swfsc_ichthyo` looked right, because it prunes afterwards. Pinned by a
regression test.

Two things follow from the shape of that cache:

- **Ancestors become `taxon` rows.** Descendant expansion walks
  `parent_taxon_key`, so a chain with a missing link is a broken rollup;
  `prune_taxon_shard()` correspondingly keeps the transitive parent closure.
- **`kingdom` / `phylum` / `class` / `order_taxon` / `family` are flattened** onto
  each taxon from its own chain, at the highest coalesce priority — WoRMS is the
  authority, and it is the only source that ever populated `family`.

### `parent_taxon_key` is carried, not pasted

It used to be derived at the end as `paste0("worms:", parent_worms_id)`. That is
wrong for an ITIS-keyed taxon: `taxon_key_of()` keys Aves on `itis:`, so a seabird
whose parent was minted `worms:<tsn>` got a key resolving to nothing. It is now a
carried column on the normalized taxon frame, coalesced like every other field,
with the old paste kept only as the fallback for sources that supply just
`parent_worms_id`.

### `ncbi_id` / `inat_id`

Populated by no source we have. Kept as declared-but-NULL columns rather than
dropped, so the release schema does not change under consumers the day one does.

# calcofi4db 3.0.0

## Breaking: the per-dataset core projections now live in the ingest notebooks

`R/model.R` carried **~600 lines of dataset-specific SQL across six functions** — a
`switch(dataset_key, ...)` per core table. Every arm is gone. Each dataset's
projection into `sample` / `obs` / `obs_attribute` / `sample_measurement` is now
declared in the ingest notebook that owns the dataset (`ingest_{provider}_{dataset}.qmd`,
"Emit Core Tables"), which is where its grain rules belong and where they can be
asserted against the real data.

**Removed** (no replacement — write the projection in the notebook):

| removed | arms it held |
|---|---|
| `build_sample_reference()` | 18 |
| `emit_core_tables()` | the wrapper |
| `create_compat_views()` | 16 compat specs |
| `.obs_arm_sql()` (private) | 14 |
| `.obs_attribute_arm_sql()` (private) | 3 |
| `.sample_measurement_arm_sql()` (private) | 2 |

Two things made this necessary. First, reading a migrated notebook made the switch
look mandatory, so a new ingest (`cdfw_dungeness-crab`) was written without emitting
the core at all. Second and worse: **a projection that exists twice drifts.**
`release_database.qmd` re-derived the core using its own inline copy of every arm,
and by the time the two were compared they had separated in four places, each a
silent data error — euphausiids flattened all 37 species to `worms:110513`
(Euphausiidae) and nulled `life_stage`; bird_mammal summed every unresolved species
on a transect into one NULL-taxon row; phytoplankton emitted **zero** observations;
cufes and phyllosoma lost their taxa entirely. The release is now a pure union of
parquet shards precisely so there is only one copy to keep correct.

Nothing was lost from the package that a notebook cannot say for itself: every
`append_*` helper always took an arbitrary `SELECT`, and `emit_core_tables()` was
only ever a convenience wrapper over them.

## New: the generic shapes those notebooks declare against

The arms were mostly *declarative calls to private helpers*, which is **why** the
projections had to live here — you cannot declare a projection from a notebook if
the vocabulary for declaring one is private. Now exported:

- **`compat_event_sql()`** — rebuild a per-dataset event table as a VIEW over the
  core: source id from the namespaced `sample_key`, containment FK from
  `parent_sample_key`, effort columns pivoted back out of `sample_measurement`.
  Was `.compat_event_sql()`; `.compat_specs()` was nothing but 16 calls to it.
- **`prune_taxon_shard(con, dataset_key)`** — trim `taxon` / `dataset_taxon` /
  `taxon_group` to one dataset's shard, **keeping the transitive parent closure**
  (descendant expansion walks `parent_taxon_key`, so dropping an ancestor breaks
  the chain). This was the load-bearing half of the private `.build_taxa_slices()`,
  and it matters for `swfsc_ichthyo`, whose WoRMS lineage table is broader than the
  taxa its own observations reach.

Joining `sample_arm_self()`, `compat_measurement_sql()`, `ns_key()` and
`ensure_measurement_taxon()` (2.20.0/2.21.0), a migrated notebook now reads as a
declaration rather than copied SQL.

## Tests

The per-dataset grain tests went with the arms — re-testing them here would mean a
second copy of every projection in the package, which is the exact duplication that
let the two copies drift. Each notebook asserts its own rules instead (grain, row
parity, FK integrity, `taxon_key` *resolution* rather than mere non-NULLness, and
regression guards for all four historical divergences). What remains, and grew, is
coverage of the generic machinery: `test-append_sample.R`, `test-compat_views.R`,
`test-measurement_taxon.R`, plus `prune_taxon_shard()` cases in `test-taxa.R`.
375 tests, all passing.

# calcofi4db 2.21.0

- **New: `scan_metadata_gaps()`, called automatically by `build_metadata_json()`.**
  Empty table/column descriptions and missing units travel from an ingest's
  `metadata.json` into the release sidecar and out through
  `calcofi4r::cc_describe_table()` / `cc_db_catalog()`, where they render as blank
  documentation — and nothing surfaced them. The check existed only as a snippet in
  the `ingest-new` skill that a human was expected to run once, by hand, after the
  first render; it appeared in **no** notebook. Running it inside
  `build_metadata_json()` (rather than `finalize_ingest()`) covers the three ingests
  that still hand-roll their outputs too. Across the 16 current sidecars it finds
  **29 tables and 395 columns with no description, and 223 unit-less measurement
  columns**.

  A missing `units` is reported only where a unit could exist: keys, names, flags,
  timestamps, vocabulary columns and the long-format `measurement_value` /
  `measurement_prec` are exempt. The last of those matters — the unit lives in
  `measurement_type`, one per row, so flagging the value column would tell a
  maintainer to do something actively wrong. Un-exempted, the same scan reported
  484 "gaps", more than half of them noise.
- **`plan_dataset_netcdf()` now distinguishes four shapes, not two.** The old rule
  — one sampling level plus a depth axis is a CF profile — held for the two
  datasets that had publish notebooks, and broke as soon as it was applied to all
  15. *Every* CalCOFI dataset carries a depth on its observations, but only
  `calcofi_ctd-cast` has many depths per event (median **74**); a tow, a transect,
  an underway record and a region pool each carry exactly one. The rule therefore
  stamped `featureType=profile` on **10 of 15** datasets that are nothing of the
  kind, and a file claiming a feature type it does not have is worse than one
  claiming none, because CF-aware tools act on the claim.

  | levels | depths/instance | `sample_type` | shape |
  |---|---|---|---|
  | 1 | > 1 | any | `profile` (ragged array) |
  | 1 | <= 1 | `underway` | `trajectory` (ragged array per cruise) |
  | 1 | <= 1 | other | `point` (one flat dimension) |
  | > 1 | any | any | `groups` (netCDF-4 + `parent_index`, no CF claim) |

  New `depths_per_instance` in the plan and in `summarise_netcdf_plan()` makes the
  discriminator visible rather than implicit. `moving_sample_types` (default
  `"underway"`) names the vocabulary terms that mean "moving platform" — that is
  not inferable from row counts, since an underway series looks exactly like
  scattered points until you know the ship was under way between them.
- **Fix: `discover_sample_levels()` crashed on a cross-dataset parent.** The
  parent join was not dataset-scoped, and `sample_key` is globally unique, so
  `calcofi_dic` — which parents 6 of its bottles onto `calcofi_bottle` **casts**,
  the mechanism behind the DIC/bottle dedup — resolved to a `sample_type` that
  `calcofi_dic` does not have. The depth walk then indexed a name that was not
  there: `subscript out of bounds`, mid-loop over all 15 datasets. Such a parent is
  now reported in a new `n_external_parent` column and the level is treated as a
  root *of that file*, since the parent's rows are not part of the dataset and so
  cannot be one of its groups. An unresolved parent (`n_orphan`) and an external
  one are counted separately; every row stays accounted for.
- **New: `obs_wide_sql()`** builds the long→wide pivot at the **occurrence grain**
  (`sample_key`, `depth_min_m`, `taxon_key`, `life_stage`), not the event grain.
  Grouping by `sample_key` alone collapses every taxon in a sample into one row —
  on `cce-lter_zooscan` that is 34,109 occurrences over 23 taxa reduced to 1,483
  rows, 96% of the data gone, with `MAX()` silently choosing one taxon's value and
  the resulting file still well-formed and plausible. Also rejects a
  `measurement_type` that cannot be a netCDF variable name or that collides with a
  coordinate the writers create, and takes an optional `count_col` so a caller can
  assert that no value was silently discarded at the grain.
- **The DSG writers cover trajectory and point, not just profile.** CF profile and
  CF trajectory are the *same* contiguous ragged array and differ only in which
  dimension the coordinates sit on, so `nc_profile_def()`/`nc_profile_write()`
  gained `obs_cols` (default `"depth"` = profile; `c("time","latitude",
  "longitude","depth")` = trajectory) and `nc_profile_atts()` gained
  `feature_type`, which selects `cf_role` (`profile_id` vs `trajectory_id`) and
  omits the ragged-array attributes for a point collection. `nc_level_vars()` /
  `nc_level_put()` accept `group = ""` to write at the file root, which is exactly
  what a `featureType=point` file is — so the point shape needed no new writer.
  Declaring one column on both dimensions is now an error.
- **Exported `ensure_measurement_taxon()`** (was `.`-internal). Staging the
  `_measurement_taxon` crosswalk is part of a per-dataset projection, and the
  derived `taxon_key` is the piece that must not be hand-rolled: it is
  `taxon_key_of()` over `worms_id`/`itis_id`, so a `'worms:' || worms_id` string
  built inline in SQL silently mis-keys any ITIS-resolved taxon.

# calcofi4db 2.20.0

- **The netCDF writers moved into the package**, joining the planner added in
  2.19.x, so one generic publish notebook can serve every dataset: `nc_level_vars()`
  / `nc_level_put()` (netCDF-4 groups, previously in `workflows/libs/publish_netcdf.R`),
  plus the CF profile half that each notebook had hand-rolled —
  `nc_profile_def()` / `nc_profile_write()` / `nc_profile_atts()` — and the two
  metadata derivations, `measurement_var_meta()` (registry → per-variable
  units/long_name/standard_name) and `nc_global_atts()` (ingest `dataset_meta` →
  CF/ACDD globals). New assertions the notebooks could not make:
  - `nc_profile_write()` takes write offsets, so the single-shot and
    chunked-by-partition paths are the same tested code (the 216M-row
    `obs_ctd_full` is written one cruise at a time); a test asserts the two agree
    value-for-value.
  - **Non-contiguous profile rows are now an error.** A contiguous ragged array
    encodes each profile as a run of `rowSize` consecutive rows, so unordered
    input produced a file that read cleanly and assigned depths to the wrong casts.
  - **An identifier longer than `strlen` is an error** rather than a silent
    truncation, and `NA` in a character column is written as empty rather than as
    the literal string `"NA"`.
  - `valid_min`/`valid_max` are emitted only when `measurement_type.csv` actually
    carries them.
- **`nc_global_atts()` dates a file by its release, not by wall clock.** A
  `Sys.time()` `date_created` put a fresh timestamp inside every build, so no
  rebuild could ever be byte-identical to an earlier release and the publisher's
  "bytes written once" sha256 check silently degraded to "always re-upload".
- **Exported the generic core-projection shape builders**: `sample_arm_self()`,
  `compat_measurement_sql()` and `ns_key()` (were `.`-internal). A dataset's
  projection belongs in the ingest notebook that owns it, and these are what keep
  that a short declaration rather than copied SQL — most `build_sample_reference()`
  arms are a single `sample_arm_self()` call with a few column expressions. No
  behaviour change; internal call sites renamed.

# calcofi4db 2.19.1

- **Fix: `derive_measurement_type_datasets()` attributed every measurement type
  to every dataset sharing its table.** It took `SELECT DISTINCT
  measurement_type` per table and then unioned *all* of that table's datasets
  onto *each* type — a cross product. Since `obs` holds 14 dataset_keys and 116
  distinct types, all 116 types inherited all 14 datasets.

  This shipped in `v2026.07.30`'s `metadata.json`, and because that map
  supersedes the CSV `_source_datasets` hint when present, it drove the
  calcofi.io/db-schema Measurements tab: `abundance` ("specimen count per net
  tow") was listed as belonging to `calcofi_ctd-cast`, `euphausiid_abundance` to
  all 14 datasets, and filtering on `calcofi_ctd-cast` returned 116 types instead
  of its actual 54. The parquet `_source_datasets` column was correct throughout —
  only the derived sidecar was wrong.

  Now grouped by `(dataset_key, measurement_type)`. A table carrying
  `measurement_type` but no `dataset_key` (a shared reference rather than a
  per-dataset shard) still falls back to table-level attribution, which is the
  best available there. 6 regression assertions, including the exact
  `abundance`-must-not-claim-ctd-cast case.

  Regenerating `metadata.json` requires re-running the release metadata step;
  released parquet is unaffected.

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

