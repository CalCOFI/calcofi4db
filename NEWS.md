# calcofi4db 3.12.0

## `check_cruise_coverage()` — the cruise that leaves `obs` and keeps its samples

Release `v2026.08.08` shipped 10 `calcofi_ctd-cast` cruises that had lost **every
one of their 874,000 observations** while keeping all 1,186 of their casts. The
CTD transects app went from 142 cruises to 132 overnight; nothing in the pipeline
said a word.

Nothing was going to. PK/FK validation runs child -> parent, so every `obs` row
that remained still had a parent cast — and a parent with **no children** violates
no constraint. The bounds backstop only inspects `obs`, which these cruises had
entirely left. There was no check anywhere that looked at the parent side.

`check_cruise_coverage(con)` is that check: one row per `dataset_key` with
`cruises`, `cruises_no_obs` and `orphan_samples`, halting when a dataset exceeds
its allowance. Three things it gets right that a first cut would not:

- **The grain is the cruise, not the sample.** A CTD `sample` row is one physical
  cast *per direction* while `obs` keeps one direction, so ~half of
  `calcofi_ctd-cast`'s cast rows legitimately carry no observations. A per-sample
  assertion is wrong on arrival; a whole cruise with none never is.
- **It joins through `sample_key`, never `obs.cruise_key`.** That denormalized
  column is NULL on 59,274 `swfsc_cufes` rows and 14,170 euphausiid ones, which
  would invent orphans that do not exist.
- **A dataset emitting no observations at all is exempt.** `sio_pic-zooplankton`
  is a net-tow registry whose biovolumes are pending from the provider, so
  `sample`-only is its designed state — 587 cruises that must not fail. The rule
  is relative ("if a dataset contributes observations, every one of its cruises
  must"), so it needs no allowlist to say so.

`max_orphan_cruises` takes a named per-dataset vector so the release can ratchet a
documented backlog while a *new* orphan still fails; an ingest that knows its own
correct answer passes `0`.

# calcofi4db 3.11.0

## `build_targets_list()` refuses a directory `output:`

Every pipeline target is `format = "file"`, so `targets` hashes whatever path the
command returns. If that path is a **directory**, anything later written
underneath it — by a downstream target, or by hand — moves the hash and leaves
the owning target reported outdated forever.

`release_database` shipped in that state: it declared `data/releases`, and
`test_release` writes `data/releases/{version}/test_results.json`. On
v2026.08.08 the release's own files landed 16:46-17:06 and `test_results.json` at
17:08:47, so the target went stale the moment the pipeline finished, and every
subsequent `tar_make()` on it or anything downstream re-ran a ~40 minute freeze
and a multi-GB re-upload of an already-promoted release.

`check_nested_outputs()` now fails the build on any directory `output:`, and on
one declared output nested inside another. Worth knowing why it checks what it
checks: `test_release` declares `_output/test_release.html` and writes into
`data/releases` as a **side effect**, so no comparison of the `output:` fields
could ever have related the two — a first cut that only compared declared paths
passed the real broken configuration. What *is* statically visible is that a
target claimed a directory at all, and that is what is enforced.

The fix for such a target is a single small file it alone writes — for the
release, `data/releases/_release_stamp.json` carrying the version and a digest of
the frozen catalog, deterministic so a no-op re-run does not cascade.

# calcofi4db 3.10.0

## `declare_measurement_bounds()` — put a bound on a type that already exists

`register_measurement_types()` only ever *appends*, by design, so an ingest cannot
silently rewrite a type another dataset depends on. That left no way to do the
thing the bounds convention asks for most often: declare `valid_min`/`valid_max`
on a type that is **already registered without one** — which was all 73 unbounded
types. "Declare it with `register_measurement_types()`" was advice that could not
be followed.

`declare_measurement_bounds()` is the narrow counterpart: it touches only the four
bound columns, only on rows that already exist, and errors on an unknown
`measurement_type` rather than inserting a bound-carrying orphan no observation
would ever match. Re-declaring the same value is a no-op, so a re-run stays
idempotent; changing an already-declared bound requires `overwrite = TRUE`,
because an agreed bound is a commitment to a provider and not something an ingest
should move as a side effect.

## Declared measurement bounds are checked, by every dataset

`metadata/measurement_type.csv` has carried `valid_min` / `valid_max` since the
CTD registry was built. They were emitted as netCDF variable attributes and shown
on the schema site, which made them look enforced; nothing compared a value to
them. v2026.08.07 shipped ~31k impossible CTD values as a result (pH to -10,
`oxygen_ml_l_1` to -79.5, `temperature_ave` to -47.6), and the fix landed as
inline SQL in one notebook.

New, and called by every ingest plus `release_database.qmd`:

* **`check_measurement_bounds()`** — compares a long-format measurement table
  against the registry and returns a per-type tally. Read-only. Works on a
  per-dataset `{dataset}_measurement`, on `obs`, or on `sample_measurement`;
  takes the registry as a data.frame, a path, or the `measurement_type` table in
  the connection.
* **`bounds_datatable()`** — the standard render, with `status` coloured.
* **`drop_out_of_bounds()`** — the enforcement, deliberately a *separate* call so
  a bound must be agreed before it is allowed to delete data.

Three things the check does that the inline version did not:

* **Reports `undeclared` types as findings.** The bigger problem was never
  "bounds declared and unchecked" — it was bounds not declared at all. At
  v2026.08.07, 73 of 98 (dataset, `measurement_type`) pairs in `obs` and 17.6M of
  26.3M rows (67%) had neither bound, and only `calcofi_ctd-cast` had more than
  one. A violations-only report on that data reads as clean. The `finding` column
  is prose ready to paste into a `questions.csv` `context` cell, so an
  unanswerable range becomes a provider question rather than a silent gap.
* **Supports one-sided bounds.** `valid_min = 0` with no ceiling is the useful
  declaration for counts, abundances and biomasses — agreeable without knowing
  the maximum, and it is what catches a negative sentinel. The inline version
  required both bounds and skipped the type otherwise.
* **Splits `n_low` / `n_high`.** Too-low and too-high usually have different
  causes (an unconverted sentinel vs a scaling error).

Applied to the released `obs`, this immediately found a live defect the CTD-only
guard could not see: `calcofi_mets.sw_ph` holds 494 values (16.6% of the type) at
`-99`, outside its declared 6..9 — an unconverted sentinel, with the bound
present and unread since the type was registered.

Counts are returned as `double`, not `integer`: `obs_ctd_full` is ~216M rows and
`as.integer()` goes `NA`-with-a-warning past 2^31, which would blank a real
violation count on the largest table.

`check_measurement_bounds()` also takes an optional `depth_col` to enforce
`valid_depth_min_m` / `valid_depth_max_m` — the depth over which a type is
*defined* — so a value emitted where the registry says the type does not exist is
a finding rather than data.

# calcofi4db 3.9.3

## `qc_run_rule()` skips cleanly when a scope value is absent in any of its forms

A cruise-scoped rule guards on whether a `cruise_key` was supplied, via
`scope_values$cruise_key %||% ""`. `%||%` only replaces `NULL`, and a scope value
goes missing in more ways than that: a caller whose "which cruise?" query
returned no rows passes `character(0)`, and one whose lookup missed passes `NA`.

Both reached `nzchar()`. `character(0)` yields `logical(0)`, which makes `&&`
evaluate to `NA`; `NA` yields `NA` directly. Either way the guard hit
`if (NA)` and stopped with **"missing value where TRUE/FALSE needed"** — thrown
from inside a rule loop, naming neither the rule nor the cruise.

It now treats a zero-length, `NA`, or empty `cruise_key` as "no cruise given" and
skips, which is what the guard was for.

Worth recording how it surfaced: the CTD ingest scopes its profile rules to the
cruise with the most out-of-range values. Once the two-sensor average repair and
the bounds guard landed there were **no** out-of-range values anywhere, so that
query returned nothing — and the render aborted precisely because the data had
become clean. A guard that fails when its subject disappears is worse than no
guard, because the failure looks like a bug in whatever ran last.

# calcofi4db 3.9.2

## A dropped object no longer costs the whole ingest

`sync_to_gcs()` retries its parallel `rsync` (default 3 attempts, 15s/30s
backoff, via the new `gcs_retries`). rsync compares before it transfers, so a
retry re-sends only what is missing — a transient network failure should cost the
remaining bytes, not the hours of compute that produced them.

It cost the hours: ctd-cast's 3.2 GB mirror crawled at 540 kiB/s, dropped one
object near the end, and took a 2 h 45 m ingest down at its final step with every
table already written correctly. The identical command run by hand succeeded a
minute later at 3.6 MiB/s.

The failure message was also unusable. It reported `tail(out, 20)` of a log whose
last twenty lines are all successful `Copying ...` entries, so the actual cause
had scrolled past — the error showed nothing but successes. It now reports the
lines that are *not* routine progress.

# calcofi4db 3.9.1

## sync_to_gcs() no longer dies on its own sidecar guard

3.9.0 added an `--exclude ^<name>$` per sidecar so that
`--delete-unmatched-destination-objects` could not delete a release's schema
record. The escape it built that pattern with used R's default TRE engine, which
reads the `{}` inside the character class as an interval quantifier and rejects
the whole pattern: **every** ingest aborted at the upload step with "invalid
regular expression ... reason 'Invalid contents of {}'" — after an hour or more
of successful work, with parquet already written.

The escape is now `re_escape()`, an internal helper using `perl = TRUE`, where
`{`, `}`, `[` and `]` are literal inside a character class. Reordering the class
so `]` comes first — the usual TRE workaround — does not help; TRE then reads
`[][` as a collating element and fails differently. Regression-tested in
`test-cloud.R`, including an assertion that the old TRE form still errors, so the
test cannot quietly become vacuous.

# calcofi4db 3.9.0

## Coverage is measured, not asserted

`observed_coverage()` derives each dataset's real temporal and spatial extent
from the assembled core (`sample` + `obs`), replacing the `coverage_temporal` /
`coverage_spatial` strings each ingest hand-wrote into its `calcofi.dataset_meta`
YAML. `format_bbox()` renders a bounding box the way a catalog writes one —
unsigned magnitudes with a hemisphere suffix, in geographic order
(`"29.8–37.8°N, 126.5–117.3°W"`), labelling both ends when a span crosses the
equator or prime meridian.

An asserted extent cannot help going stale: it is authored once and the data
grows underneath it. Checked against release `v2026.08.06`, **7 of 15 were
wrong** — `cce-lter_zoodb` claimed coverage through 2021-05 when its data ends
2015-04, `calcofi_phyllosoma` stopped a year short of its own rows, and three
said `"present"` while in fact stalling in 2019, 2022 and 2023.

Two things the implementation is deliberate about:

- **`NaN` is not `NULL`.** A `NaN` coordinate survives `IS NOT NULL` and
  `min()`/`max()` propagate it, so one poisoned row would blow a dataset's whole
  bounding box out to `NaN` with every nullity check still passing. The filter
  is `isfinite()`.
- **The halves are measured independently.** `calcofi_phytoplankton` is
  region-pooled: real coordinates, no `datetime` anywhere. It measures spatially
  and returns `NA` temporally rather than inventing a range, so a caller can
  fall back to a declared value for that half alone.

## netCDF no longer asserts a license nobody confirmed

`nc_global_atts()` defaulted `license` to `"CC-BY 4.0"`. Only two ingests
(`calcofi_dic`, `sio_mesopelagic-fish`) ever declared a license, so the other 14
published netCDFs claiming terms for other people's data on no authority at all.
An undeclared license now **omits the attribute** — the same rule
`valid_min`/`valid_max` already followed, and for the same reason: a plausible
default is indistinguishable from a real one downstream.

## Bulk parquet stages outside the repo

`cc_stage_dir()` / `cc_stage_path()` resolve a local staging root from
`CALCOFI_STAGE_DIR`, defaulting to `~/_big/calcofi`. An ingest's output now
splits across two roots:

- **bulk parquet** → the staging root, on its way to `gs://calcofi-db/`;
- **JSON sidecars** (`manifest.json`, `metadata.json`, `relationships.json`) →
  the repo, where they are small, diffable, and reviewable.

24 GB of parquet sat inside a git working tree, which forced a blanket ignore
rule that swept the sidecars out of version control as collateral — the schema
and provenance record for every dataset was untracked.

- `write_parquet_outputs()` gains `parquet_dir` (default: the staging root);
  `output_dir` now means the sidecar directory. Pass `parquet_dir = output_dir`
  to restore the old single-directory layout.
- `write_spatial_manifest()` gains `output_dir` for the same split.
- `sync_to_gcs()` gains `sidecar_dir`, so both roots mirror to one `gcs_prefix`.
  Sidecars are **exempted from `delete_stale`**: they are not under `local_dir`,
  so an unguarded `--delete-unmatched-destination-objects` would have deleted
  the release's entire schema record on every sync.
- `core_shard_paths()` / `assemble_core_table()` / `assemble_core()` default
  `parquet_dir` to the staging root and accept an absolute path (previously it
  was always pasted onto `root`).
- `build_release_table_registry()` gains `manifest_dir` alongside `parquet_dir`,
  which used to mean both the manifest location and the byte location.

**Manifest paths are now relative.** `files$path` recorded an absolute path, so
committing a manifest would bake one machine's home directory into the repo.
Where the bytes live is a property of the environment, not of the release.

# calcofi4db 3.8.0

## Every taxon gets its classification, not just the ones that were asked for

`.lineage_flat()` emitted one row per **requested** id, so a taxon that entered
the release only as somebody else's lineage ancestor arrived with a key, a name
and a rank and **no classification at all**. In v2026.08.06 that was 430 of
`swfsc_ichthyo`'s 1,553 taxa at or below family rank carrying neither `family`
nor `kingdom`, and it was not an ITIS quirk — 44% of ITIS ancestors and 34% of
WoRMS ancestors alike. An ancestor is a real taxon a consumer can select and roll
up on; it should not be a second-class row because of how it happened to be
fetched.

It now emits **one row per distinct taxon across every chain**, with the five
headline ranks derived from that taxon's own ancestors-or-self. No API call is
involved: every chain passing through a node already contains its ancestors, so
this is a re-read of data the cache holds.

Two details that matter:

- **The walk follows parent pointers, not row order.** `fetch_taxon_lineage()`
  sorts by `(authority, requested_id, taxonID)`, which destroys the root→self
  ordering the fetchers produce — so anything positional (like the old "the last
  row is the taxon itself") was reading an arbitrary row. A single parent map is
  built across all chains and climbed level by level.
- **A deprecated requested id still gets a row.** ITIS answers 174553
  (*Puffinus griseus*) with 1255050 (*Ardenna grisea*), so no node matches what
  was asked for. `ensure_taxon_xref()` normally re-keys onto the accepted id
  first, but cannot when `taxize` is unavailable; an alias row now points such a
  taxon at its chain's leaf rather than letting it lose its classification.

Ranks above family correctly keep `family = NA` — a phylum has no family — so
coverage is asserted by rank position, not as a blanket non-NULL.

`taxa_rank_reference()` also gained **`Section` and `Subsection`**, which WoRMS
nests *below* Infraorder for decapods (Brachyura > Eubrachyura > Heterotremata >
Cancroidea) rather than between order and family as in botany. They were the last
two ranks in the release with no `rank_order`; a new test asserts the vocabulary
covers every rank the shards actually carry, since a rank it lacks releases as a
silent NULL — which is exactly how 100% of ITIS taxa went unnoticed.

## Not fixed, deliberately

`gbif_id` and `ncbi_id` stay as they are. WoRMS rejects `type = "gbif"` outright
(HTTP 400) and returns no content for `ncbi` on the taxa we carry, so there is no
crosswalk to make; filling them would mean a third authority's API and a separate
sweep. `ncbi_id`/`inat_id` remain declared-but-NULL by design.

# calcofi4db 3.7.0

## `rank_order` for every taxon, not just one dataset's

`build_taxon_reference()` takes `rank_order` from a `taxa_rank` lookup in the
connection. That lookup was built in exactly one place — an inline vector inside
`build_taxon_hierarchy()`, which only `swfsc_ichthyo` calls — so it existed in
that one connection and nowhere else, and the left join produced `NA` for
everybody else. In release v2026.08.06 that was **100% of ITIS-keyed taxa** (all
169: every seabird and marine mammal) plus 252 WoRMS-keyed ones — 172 species,
83 genera and 49 families with no sortable rank, in a column whose entire job is
sorting a hierarchy.

New exported **`taxa_rank_reference()`** is that vocabulary, promoted to the
package and covering **both** authorities. Eight ranks the release actually
carries were missing from the old vector — `Gigaclass`, `Infrakingdom`,
`Megaclass`, `Parvphylum`, `Phylum (Division)`, `Subphylum (Subdivision)`,
`Subterclass`, `Superdomain` — so those taxa had no `rank_order` even where the
lookup was present. WoRMS and ITIS do not share a rank set, and a vocabulary
derived from one of them cannot order the other.

- `build_taxon_reference()` now uses the connection's `taxa_rank` where it has an
  answer and the package reference for the rest, so **no notebook changes**: every
  ingest gets `rank_order` by re-running.
- It also **dedups the lookup to one row per rank**. A rank carrying both an order
  and a NULL (which is what a partially-populated `taxa_rank` looks like) fans the
  left join out and silently doubles every taxon of that rank.
- `build_taxon_hierarchy()` reads the same reference instead of its own copy.

## Lineage ancestors are no longer second-class

`ensure_taxon_xref()` runs *before* `ensure_taxon_lineage()` — it has to, so the
lineage fetch asks about the accepted id rather than the deprecated one — which
means it only ever sees the dataset's own vocabulary. The ancestors are
discovered afterwards, so nothing cross-referenced them: 657 of 732 ancestor rows
released with no `itis_id` and 198 with no `taxonomic_status`, while every one of
those answers was already sitting in the xref cache.

`ensure_taxon_lineage()` now tops up the staged `_taxon_xref` for the ancestors
it just fetched (new `xref_cache_csv` argument, defaulting to `taxon_xref.csv`
beside the lineage cache — the layout every ingest already uses, so again no
notebook change). Cached ids cost no API call.

`.apply_xref()` gained `rekey = FALSE` for this path: an ancestor's key comes
from the classification chain it was fetched in, so its ids may be *filled* but
never *replaced* — swapping one would break the parent links that chain just
established.

# calcofi4db 3.6.0

## Taxa reach the release with BOTH authorities' ids, and a status that is checked

`taxon_key_of()` keys birds on `itis:<TSN>` because WoRMS bird taxonomy lags —
it still calls these *Oceanodroma*, *Puffinus*, *Phalacrocorax*. That rule is
right and is unchanged. What was missing is that nothing ever populated the
`worms_id` **column** for those taxa, and the key authority and the
cross-reference columns are different questions. A consumer joining on
`worms_id` (`db-viz-hex::get_sp()`) therefore matched **zero rows for every
seabird and marine mammal**: 59,858 of the Farallon census's 64,956 `obs` rows,
92.2% of the dataset, unreachable with no error anywhere.

### New: `fetch_taxon_xref()` / `ensure_taxon_xref()` (`R/xref.R`)

A cache-backed authority cross-reference, built on the same contract as
`ensure_taxon_lineage()`: it stages a `_taxon_xref` table that
`.taxon_norm_sources()` reads, so every builder picks it up and a re-run is free
and offline.

- **ITIS TSN → WoRMS AphiaID** via `worrms::wm_record_by_external(type = "tsn")`
  — an *exact id crosswalk*, not a name match. 91 of the 92 Farallon bird TSNs
  resolve through it (the miss is a trinomial subspecies); 7 need
  `valid_AphiaID` synonym-following.
- **WoRMS AphiaID → ITIS TSN** via `wm_external()`, backfilling `itis_id` on the
  753 `worms:`-keyed taxa that had none — including 34 Farallon mammals whose
  source TSN the override registry was discarding. Batched 50 at a time
  (`wm_record()` and `wm_external_()` both take a vector), which turns ~2,000
  sequential request pairs into ~40 calls; a chunk that errors falls back to
  one-at-a-time so a single bad id costs only itself.
- **name → AphiaID** via `wm_records_name()` as the last resort, for taxa
  carrying neither id.

Two invariants the module enforces: a **key** must be an *accepted* id, so a
deprecated one is re-keyed; a **cross-reference** is whatever the authority
links, stored verbatim.

### New: `clean_taxon_name()`

Strips open-nomenclature and qualifier noise (`" sp."`, `" spp."`, `" cf "`,
`"indistinguished "`, parenthetical authorship, trailing variant letters) so a
source column header reaches WoRMS in a form it can match. `"Bathophilus sp."`
→ `"Bathophilus"`; this is the whole reason 6 `sio_mesopelagic-fish` taxa had no
id. It generalizes the hand-maintained `name_query` column that one dataset's
cache already carried.

**The cleaned name is the lookup query only.** `ds_taxa_code` is left verbatim —
for mesopelagic fish the code *is* the spreadsheet header and is the join key
from `obs`, so rewriting it would orphan every observation.

### `taxon` gains `status_checked` and an append-only `notes`

`taxonomic_status` used to be the literal string `"accepted"`, stamped by
`ensure_taxon_lineage()` onto all 2,090 released taxa — including 28 whose ITIS
TSN is demonstrably deprecated, and override rows whose own note reads "WoRMS
status: unaccepted". It is now **fetched**, and carries `status_checked`: a
status with no date is not a fact.

`notes` accumulates datestamped lines and is never rewritten, recording how each
id was resolved and any re-key, e.g.

```
2026-08-05: worms_id 137202 via WoRMS TSN crosswalk (status accepted);
            itis:174553 deprecated in ITIS -> itis:1255050 (Ardenna grisea)
```

Both columns are additive; the dataset's own original code and name remain in
`dataset_taxon`.

### `taxon_override.csv` is now actually generic

The registry's schema was always dataset-agnostic, but its **`match_column` was
never read anywhere in `R/`**: `.apply_overrides()` was called from exactly two
hardcoded sites passing a literal dataset name and a literal match vector, so a
row added for any of the other five arms was parsed and then dropped without a
word. Every arm now consults it, dispatching on the declared `match_column`, and
a row naming an unknown `dataset_key` or a `match_column` the source does not
expose **errors** — a typo must fail the ingest, not vanish. Same failure class
as the unregistered-provider bug.

### `.fetch_itis_chain()` follows `acceptedTSN`

ITIS returns *no* classification for a TSN it has deprecated, and an empty
result is indistinguishable from "no such taxon". That is why 28 Farallon birds
reached the release with no rank, no parent and no classification at all. The
fetcher now resolves to the accepted TSN and retries, and `.lineage_flat()`
falls back to the chain's leaf when no row matches the requested id.

### New: `check_taxon_ids()`

Reports, per dataset, the taxa and observations with no `worms_id`, no authority
key, or no rank — and **fails** on a dataset-local `taxon_key` that is not in an
explicit `allow` list. The 19 genuinely non-taxonomic classes (zooscan
eggs/multiples/nauplii/others, phytoplankton "other"/"undefined code") are
declared one key at a time, in the open, so a *new* unresolved taxon can never
hide among the known ones.

# calcofi4db 3.5.0

## `match_cruise_by_track()` recovers `cruise_key` from where the platform was

New exported helper (`R/spatial.R`, `@concept spatial`) that assigns `cruise_key`
to rows carrying a date and a position but no cruise FK, by finding the nearest
station occupation in a reference *track* — any table of `cruise_key` + datetime
+ lon/lat, such as the `sample` shard of an already-ingested dataset.

The motivating case is the bird/mammal census (workflows#74), whose 60,715
transects shipped with `cruise_key` NULL on every row — and therefore on all
66,272 `obs` rows — because the source records a survey label (`CAC1987_05`)
rather than a cruise. Parsing year-month out of that label is not good enough:
it is ambiguous whenever several ships sailed in one month (1998-10 had four),
and it is simply wrong for a survey that straddles a month boundary
(`CAC2014_01` ran 2014-01-29 → 02-04 and belongs to `2014-02-3322`).

With `group_col` set the match is a **consensus** rather than a per-row
assignment: every row of a group votes with its own nearest-station match, the
modal `cruise_key` wins if it holds at least `min_share` of the votes, and the
winner is written to *all* rows of the group — including rows too far from any
station to have voted. One survey is one cruise, so this is both more robust (a
transect that strays near another ship's station cannot mis-assign itself) and
higher-yield (32,599 voting transects resolve all 60,010). A group whose vote is
too split, or that has no vote at all, is left NULL rather than guessed.

Notes on the implementation:

- Distance is the cosine-corrected equirectangular approximation, well under 1%
  error at the separations that matter and far cheaper than
  `ST_Distance_Sphere()` over the candidate join. Candidates are pre-filtered to
  a `max_km` bounding box, so an antipodal row can never become the "nearest"
  one — matching against the full release track produced exactly that, a
  21,982 km match.
- `NaN`/`Inf` coordinates are excluded explicitly on both sides. They survive
  `IS NOT NULL`, so filtering on NULL alone is not enough (cf. 3.4.2).
- Only cruises present in the reference track can be assigned, so pointing
  `ref_tbl` at a track whose keys all exist in the `cruise` reference table
  guarantees the emitted FK resolves.

# calcofi4db 3.4.3

## `append_sample()` tags geometry EPSG:4326

`ST_Point()` alone tags `OGC:CRS84`, while `ST_Read()` over GeoJSON — which is
how `ingest_spatial.qmd` builds the polygon layers — tags `EPSG:4326`. Both label
the same WGS 84 lon/lat coordinates, but **DuckDB refuses `ST_Intersects` across
differing CRS tags**, so joining `sample` to `spatial` errored outright rather
than returning a wrong answer.

`ST_SetCRS` relabels without transforming; nothing is reprojected. EPSG:4326 is
the conventional label, is what `calcofi4r::cc_tbl()` assigns to consumers, and
is what the ingests already document.

`release_database.qmd` additionally normalises **every** geometry column to
EPSG:4326 immediately before the freeze, so the guarantee holds for the release
without re-running all 16 ingests, and a future ingest minting geometry a third
way cannot reintroduce the mismatch.

# calcofi4db 3.4.2

## `append_sample()` normalises non-finite coordinates

`NaN` is not `NULL`, and that difference shipped. A `NaN` latitude survives an
`IS NOT NULL` check, so it passed validation and reached release v2026.08.02 —
**1,590 rows** (`swfsc_cufes` 1,583, `calcofi_mets` 7, all `sample_type =
'underway'`). Worse, `ST_Point(NaN, NaN)` produces a real, non-NULL `GEOMETRY`,
so `WHERE geom IS NOT NULL` did not filter it either: any consumer doing a
spatial join silently carried a point that is nowhere. It also poisons
aggregates — a single `NaN` makes `MAX(longitude)` `NaN` for the entire column,
which is how it was found.

`NaN`/`Inf` latitude and longitude are now normalised to `NULL` before the
geometry is minted, so no geometry is created for them. Done here rather than in
each ingest because it fixes every dataset at once and belongs where the geometry
is created. Reported with a count rather than silent — a coordinate quietly
becoming `NULL` is its own kind of surprise.

# calcofi4db 3.4.1

## `flag_invalid_rows()` no longer rewrites a file that did not change

Flagged-row CSVs are committed and reviewed in diffs, but `_ingested_at` is
stamped per row at read time, so re-running an ingest over unchanged source data
rewrote every row with a new timestamp. `data/flagged/invalid_egg_stages.csv`
churned the same 790 rows on every run — noise that hides the diff that would
matter.

The write is now skipped when the new rows match the file on disk apart from
`volatile_cols` (default `"_ingested_at"`; pass `character()` to force a
rewrite). The comparison is done on **character** values on both sides: the
on-disk copy has been through a CSV round trip and the in-memory tibble has not,
so a typed comparison would see integer `1` against the string `"1"` and rewrite
forever. Column order is normalised too. `append = TRUE` never takes the skip
path — it is additive by definition.

Also writes with `na = ""`, for the reason the metadata registries do: DuckDB's
`read_csv_auto` does not treat `"NA"` as NULL, so readr's default would ship a
literal two-character value to anything reading these files.

# calcofi4db 3.4.0

## `data_stage` on core `sample` — optional, trailing, opt-in

The source CTD files mark preliminary cruises **"for non-publication use"** and
warn that oxygen, nitrate and chlorophyll may change significantly after
post-cruise calibration. `ingest_calcofi_ctd-cast.qmd` has always known which
cruises are which, and the released `sample` had nowhere to put it — so the
caveat stopped at the notebook (question `calcofi_ctd-cast_14`).

- **`append_sample()` now accepts 15 **or** 16 columns.** The 16th, trailing, is
  `data_stage`; a 15-column arm gets `NULL`. `select_sql` is bound positionally
  and 16 ingests call it, so inserting the column into the contract would have
  broken all 16 at once — trailing and optional means only the dataset that has a
  meaningful stage changes, and the rest opt in later.
- A 14- or 17-column arm now fails with a named error rather than DuckDB's
  "table function has N columns but M names were given".
- `sample` gains `data_stage VARCHAR`. `.ensure_sample_schema()` also ALTERs an
  existing table, since each ingest's wrangling DB survives across runs and
  `CREATE TABLE IF NOT EXISTS` alone would leave a pre-3.4.0 `sample` a column
  short.

Release assembly needed no change: `assemble_core_table()` unions the shards
`BY NAME`, so a shard written before this release simply reads `NULL`.

## The provider-question registry gets one reader and one vocabulary

`metadata/{provider}/{dataset}/questions.csv` — 136 questions across 17 files —
was read by each of the 16 ingest notebooks with its own `read_csv()` +
`arrange(factor(priority, …))` + `select(…)`. The level vectors disagreed, so a
status nobody listed sorted silently to the bottom and was never seen again;
`ingest_calcofi_mets.qmd` ranked by a vector containing `"blocker"` and
`"asked"`, neither of which is a status. Four spellings of "done" and two of
"normal" had accumulated.

- **`read_questions()`** — the one validated read. Strict (`na = ""`, everything
  character, so an id suffix of `01` is never retyped to `1`), checks the
  controlled vocabulary, and returns the questions ranked `blocker` → `low`.
  An unknown `status`/`priority`, a duplicate `label` or a missing column is an
  error naming the value, not a silent drop.
- **`questions_datatable()`** — the standard render every notebook now calls.
  Columns empty for every question are dropped, so a dataset with no answers yet
  does not show two blank columns.
- **`question_statuses()` / `question_priorities()`** — the vocabulary itself:
  `open | proposed | answered | wontfix` and `blocker | high | normal | low`.

**`proposed` is the new state and the point of the exercise**: we have already
built or reasoned an answer and want it *confirmed*. `proposed_answer` carries
it, so the provider approves a solution rather than being handed a problem.

## The measurement registry can now state a depth range and a derivation

`merge_metadata_json()` carries three more `measurement_type.csv` columns into
the release sidecar's `measurement_types` block, alongside the existing
`valid_min`/`valid_max`:

- **`valid_depth_min_m` / `valid_depth_max_m`** — the depth range over which the
  type is *defined*. `est_chlorophyll_a_*` is computed by applying the
  fluorometer regression to 0–200 m alone, so a null at 300 m is by construction,
  not missing data, and a completeness check had no way to know that.
- **`derivation`** — free text on how a derived type was produced. The CTD files
  publish every property three times (SBE-processed, `_CruiseCorr`, `_StaCorr`)
  and the suffix was the only thing distinguishing them.

Every one of these is **omitted** from the sidecar when the registry cell is
empty. An emitted `"valid_max": null` reads as "no upper bound" — an assertion
the registry never made.

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

## Uploads: shipboard files -> the core model

An uploaded cast can now be checked before it ever reaches a release. The design
principle that makes it cheap: every rule targets `obs` / `sample`, so projecting
a file into that shape runs the whole registry unchanged.

- **`read_ctd_upload()`** — dispatches on extension: `.csv` (CalCOFI cast file),
  `.cnv`, `.asc`, `.btl`. `.hex` is **refused with its reason** — it is raw A/D
  counts and needs the `.xmlcon` calibration file, so any conversion without it
  would be invented numbers.
- **`sbe_split_header()`** — the trap that makes `.asc` hard: the header is
  fixed-width and adjacent names run together (`Sbeox0ML/LSbeox0Mm/Kg`) in **179
  of 200** CalCOFI files, so a whitespace split mis-assigns every column after the
  collision. Names and numbers are right-aligned, so columns are cut at the data
  rows' stop positions — and when the result is not self-consistent it **errors
  rather than guessing**, asking for the `.cnv` whose header is unambiguous.
  Measured: ~86% of `.asc` and ~47% of `.btl` read cleanly; the rest say why.
- **`read_sbe_cnv()` / `read_sbe_asc()` / `read_sbe_btl()` / `read_sbe_header()`**
  — including `bad_flag` → `NA`, and the `.btl` quirks (one `Date` header word over
  three data fields; several tagged statistic rows per bottle).
- **`ctd_map_columns()`** — CalCOFI names map through `measurement_type.csv`
  `_source_column`; Sea-Bird names through the new
  `metadata/sbe_name_map.csv`. **Unmapped columns are a result, not an error** —
  they are where a format change announces itself.
- **`ctd_upload_to_core()`** — the projection, applying the same `-99` /
  `-9.99e-29` sentinel deletion and `"9.0"` → `"9"` quality-code repair the
  pipeline already knows, because a new file is exactly where those arrive.
- **`qc_upload_con()`** — an in-memory connection where the upload *is*
  `obs` / `sample` / `obs_ctd_full`. Nothing touches a release; it dies with the
  session.

## Cast profiles for review

- **`qc_cast_profile()`** — the full-resolution scans for the physical cast a
  `sample_key` belongs to, **both** directions, since the point of plotting a
  profile during review is to see them overlaid. Two traps are why this is
  packaged rather than inline in an app callback: `cruise_key` is a performance
  *precondition* (`obs_ctd_full` is hive-partitioned by it, so an unscoped fetch
  scans ~212M rows) and is looked up when not supplied; and the direction suffix
  must be stripped without eating the `d` in `calcofi_ctd-cast`.
- **`qc_cast_base()` / `qc_cast_direction()`** — that suffix, handled once.

The rule contract gained two columns, documented in `R/qc.R`: a finding about a
particular scan now returns `depth_min_m` **and** `measurement_type`, which is
what lets a reviewer click a finding and land on the right profile at the right
depth without the app knowing anything about the rule that produced it.

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

