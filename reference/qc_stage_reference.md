# Stage the QC reference tables a rule registry expects

The rules join against reference data that is deliberately NOT part of
the release: the quality-code vocabulary, the harmonic climatology and
station bottom depths mined from the CalCOFI hydrographic Access master,
and a seafloor depth per cast derived from bathymetry. This puts all of
them on one connection so both the app and the ingest notebook get the
same reference inputs.

## Usage

``` r
qc_stage_reference(
  con,
  dir_workflows,
  gebco_tif = NULL,
  sample_tbl = "sample",
  quiet = FALSE
)
```

## Arguments

- con:

  a DBI connection to stage into

- dir_workflows:

  root of the `CalCOFI/workflows` checkout

- gebco_tif:

  optional path to a positive-down bathymetry GeoTIFF (the one
  `apps/ctd-viz` crops from GEBCO 2025). When supplied — and when
  `terra` is installed — `sample_seafloor` is built by extracting it at
  each cast position.

- sample_tbl:

  table or view holding `sample_key` / `longitude` / `latitude`

- quiet:

  suppress the per-table progress lines

## Value

character vector of the tables actually staged, invisibly

## Details

`measurement_type` comes from the WORKFLOWS REGISTRY, not from a
release: the registry is the source of truth and moves ahead of the
release (`valid_min` / `valid_max` existed there before any release
carried them), so sourcing it from a release would silently disable
every range rule.

A missing input is left as a MISSING TABLE rather than an empty one. An
empty reference table makes its rules return zero rows, which reads as
"clean"; a missing table makes them error, which reads as "not checked".
The second is the truth.
