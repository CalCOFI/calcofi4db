# Rebuild a per-dataset measurement table as a VIEW over `obs`

Rebuild a per-dataset measurement table as a VIEW over `obs`

## Usage

``` r
compat_measurement_sql(dataset_key, sample_type, fk_col, id_col)
```

## Arguments

- dataset_key:

  provider_dataset

- sample_type:

  the `sample_type` its rows carry

- fk_col:

  name for the recovered event FK column

- id_col:

  name for the recovered measurement id column

## Value

a SQL SELECT string
