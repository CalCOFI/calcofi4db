# Rebuild a per-dataset event table as a VIEW over the core `sample`

The source id is recovered from the namespaced `sample_key`
(`'<dataset_key>:<sample_type>:<id>'` -\> field 3), the containment FK
from `parent_sample_key`, and the event-level effort columns by pivoting
`sample_measurement` back out of long form.

## Usage

``` r
compat_event_sql(
  dataset_key,
  sample_type,
  id_col,
  parent_col = NULL,
  cols = character(),
  measures = character(),
  sample_tbl = "sample"
)
```

## Arguments

- dataset_key:

  provider_dataset the rows carry

- sample_type:

  the `sample_type` the rows carry (`site`, `tow`, `net`, …)

- id_col:

  name for the recovered source id column

- parent_col:

  optional name for the recovered parent FK column

- cols:

  named character vector `c(<out name> = <sample column>)` of straight
  passthrough columns

- measures:

  named character vector `c(<out name> = <measurement_type>)` of effort
  columns to pivot back out of `sample_measurement`

- sample_tbl:

  name of the core `sample` table to read

## Value

a SQL SELECT string

## Details

Exported because each dataset's compat VIEWs are declared in its own
ingest notebook — this is the reusable *shape*, not a per-dataset
projection.

## Examples

``` r
compat_event_sql("swfsc_ichthyo", "tow", "tow_uuid", "site_uuid",
                 c(tow_type_key = "tow_type", datetime_start_utc = "datetime"))
#> SELECT split_part(s.sample_key, ':', 3) AS tow_uuid,
#>     split_part(s.parent_sample_key, ':', 3) AS site_uuid,
#>     s.tow_type AS tow_type_key,
#>     s.datetime AS datetime_start_utc
#> FROM sample s
#> WHERE s.dataset_key = 'swfsc_ichthyo' AND s.sample_type = 'tow'
```
