# calcofi4db

CalCOFI Database Tools package for ingesting and managing datasets using a modern cloud-native data architecture with DuckDB and Parquet files.

## Overview

This package provides functions for the CalCOFI data workflow, which uses:

- **Google Cloud Storage (GCS)**: Versioned data lake with immutable archive snapshots
- **Apache Parquet**: Efficient columnar storage format for large datasets
- **DuckDB**: Fast, serverless analytical database
- **DuckLake**: Lakehouse catalog with time travel and versioning

### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Google Drive  │────▶│      GCS        │────▶│  Working        │────▶│    Frozen       │
│   (raw CSVs)    │     │  (versioned)    │     │  DuckLake       │     │    Releases     │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                              rclone                 ingest                  freeze
```

- **Working DuckLake**: Internal database with full provenance tracking (`_source_file`, `_source_row`, `_source_uuid`, `_ingested_at`)
- **Frozen DuckLake**: Immutable versioned releases (`v2026.02`, `v2026.03`, ...) for public access

### Key Features

- Reading CSV files from GCS with immutable archive paths
- Cloud storage operations (upload, download, version listing)
- Parquet conversion with schema enforcement and metadata
- DuckDB database creation and management
- Provenance tracking for data lineage
- Frozen release creation for reproducible data access
- Data validation before releases
- Backward-compatible PostgreSQL support (deprecated)

## Installation

```r
# install from GitHub
remotes::install_github("CalCOFI/calcofi4db")
```

## Usage

### Working with the Working DuckLake

The Working DuckLake is used internally for data ingestion with automatic provenance tracking:

```r
library(calcofi4db)

# connect to the Working DuckLake
con <- get_working_ducklake()

# read CSV from immutable GCS archive
csv_path <- get_calcofi_file(
  "swfsc.noaa.gov/calcofi-db/larva.csv",
  date = "2026-02-02_121557")  # specific archive snapshot

# read and transform data
larvae_data <- readr::read_csv(csv_path)

# ingest with automatic provenance columns
stats <- ingest_to_working(
 con         = con,
  data        = larvae_data,
  table       = "larva",
  source_file = "archive/2026-02-02_121557/swfsc.noaa.gov/calcofi-db/larva.csv",
  source_uuid_col = "larva_uuid",
  mode        = "replace")

# save changes to GCS
save_working_ducklake(con)

# close connection
close_duckdb(con)
```

### Creating Frozen Releases

Frozen releases are immutable snapshots for public access:

```r
# connect to Working DuckLake
con <- get_working_ducklake()

# validate data quality before freezing
validation <- validate_for_release(con)
if (!validation$passed) {
  print(validation$errors)
  stop("Validation failed")
}

# create frozen release
result <- freeze_release(
  con           = con,
  version       = "v2026.02",
  release_notes = "First release with bottle and larvae data.")

# result contains: version, gcs_path, tables, parquet_files
print(result$tables)

close_duckdb(con)
```

### Listing and Comparing Releases

```r
# list all available frozen releases
releases <- list_frozen_releases()
print(releases)

# get metadata for a specific release
meta <- get_release_metadata("v2026.02")
print(meta$tables)

# compare two releases
diff <- compare_releases("v2026.02", "v2026.03")
print(diff$summary)
```

### Cloud Storage Operations

```r
# download file from GCS
local_file <- get_gcs_file("gs://calcofi-files-public/_sync/bottle.csv")

# upload file to GCS
put_gcs_file("local/data.parquet", "gs://calcofi-db/parquet/data.parquet")

# list files in bucket
files <- list_gcs_files("calcofi-files-public", prefix = "_sync/")

# get file from immutable archive snapshot
file <- get_calcofi_file(
  "swfsc.noaa.gov/calcofi-db/cruise.csv",
  date = "latest")  # or specific timestamp
```

### Parquet Operations

```r
# convert CSV to Parquet
csv_to_parquet("data.csv", output = "data.parquet")

# read Parquet file
data <- read_parquet_table("data.parquet")

# write with partitioning
write_parquet_table(
  data,
  "output/data.parquet",
  partitions = c("year", "month"))

# add metadata to Parquet file
add_parquet_metadata(
  "data.parquet",
  list(source = "CalCOFI", version = "2026.02"))
```

### Basic DuckDB Operations

```r
# create in-memory DuckDB
con <- get_duckdb_con()

# create from Parquet files
con <- create_duckdb_from_parquet(
  c("parquet/bottle.parquet", "parquet/cast.parquet"),
  db_path = "calcofi.duckdb")

# add documentation comments
set_duckdb_comments(
  con,
  table = "bottle",
  table_comment = "Bottle sample data",
  column_comments = list(
    depth_m = "Sample depth in meters",
    temp_c  = "Water temperature in Celsius"))

# export to Parquet
duckdb_to_parquet(con, "bottle", "export/bottle.parquet")

# close connection
close_duckdb(con)
```

## For End Users

End users should use the `calcofi4r` package to access CalCOFI data:

```r
library(calcofi4r)

# connect to latest frozen release
con <- cc_get_db()

# or specific version
con <- cc_get_db(version = "v2026.02")

# convenience functions
larvae <- cc_read_larvae()
bottles <- cc_read_bottle()

# list available versions
cc_list_versions()
```

## Deprecated Functions

The following PostgreSQL-based functions are deprecated and will emit warnings:

| Deprecated | Replacement |
|------------|-------------|
| `get_db_con()` | `get_working_ducklake()` or `get_duckdb_con()` |
| `ingest_csv_to_db()` | `ingest_to_working()` |
| `ingest_dataset()` | Use targets pipeline with `ingest_to_working()` |

## Documentation

- [CalCOFI Data Workflow Plan](https://calcofi.io/workflows/README_PLAN.html) - Full architecture documentation
- [CalCOFI Database Documentation](https://calcofi.io/docs/db.html) - Database naming conventions and schema
- [Package Reference](https://calcofi.io/calcofi4db/reference/) - Function documentation

## GCS Bucket Structure

```
gs://calcofi-files-public/          # versioned source files
├── _sync/                          # working directory (rclone target)
├── archive/                        # immutable snapshots
│   └── 2026-02-02_121557/         # timestamp directories
└── manifests/                      # JSON manifests

gs://calcofi-db/                    # integrated database
├── ducklake/
│   ├── working/                    # Working DuckLake
│   │   └── calcofi.duckdb
│   └── releases/                   # Frozen releases
│       ├── v2026.02/
│       │   ├── catalog.json
│       │   ├── RELEASE_NOTES.md
│       │   └── parquet/*.parquet
│       └── latest.txt              # points to current release
└── parquet/                        # intermediate parquet files
```

## License

MIT
