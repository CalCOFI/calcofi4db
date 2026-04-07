# Convert WKB BLOB columns to GEOMETRY for spatial tables

Convert WKB BLOB columns to GEOMETRY for spatial tables

## Usage

``` r
.convert_wkb_to_geometry(con, tbl_name, geom_tables)
```

## Arguments

- con:

  DBI connection

- tbl_name:

  Table name

- geom_tables:

  Character vector of table names with geometry columns

## Value

logical TRUE if geometry conversion was performed
