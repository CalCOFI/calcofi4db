# calcofi4db

CalCOFI Database Tools package for ingesting and managing datasets in the CalCOFI database using an integrated schema-based workflow.

## Overview

This package provides functions for the CalCOFI integrated database ingestion strategy, which uses a two-schema approach:

- **`dev` schema**: Fresh development schema recreated with each ingestion run for QA/QC
- **`prod` schema**: Versioned production schema for stable public access

### Key features:

- Reading CSV files from Google Drive with metadata extraction
- Creating and managing redefinition files for tables and fields
- Transforming data according to redefinition rules
- Ingesting multiple datasets into a fresh database schema
- Managing database relationships and indexes
- Recording schema versions with full provenance

## Installation

```r
# Install from GitHub
remotes::install_github("CalCOFI/calcofi4db")
```

## Usage

### Master ingestion workflow

The primary workflow is the master ingestion script `inst/create_db.qmd` that recreates the `dev` schema with all datasets:

1. Drops and recreates `dev` schema
2. Ingests multiple datasets from Google Drive
3. Applies transformations via redefinition files
4. Creates relationships (primary/foreign keys, indexes)
5. Records schema version with metadata

### Individual dataset ingestion

For programmatic control, use the core functions:

```r
library(calcofi4db)
library(DBI)
library(RPostgres)

# Connect to database
con <- dbConnect(
  Postgres(),
  dbname = "gis",
  host = "localhost",
  port = 5432,
  user = "admin",
  password = "postgres"
)

# Read CSV files and metadata from Google Drive
d <- read_csv_files(
  provider = "swfsc.noaa.gov",
  dataset = "calcofi-db"
)

# Transform data according to redefinitions
transformed_data <- transform_data(d)

# Ingest into dev schema
ingest_csv_to_db(
  con = con,
  schema = "dev",
  transformed_data = transformed_data,
  d_flds_rd = d$d_flds_rd,
  d_gdata = d$d_gdata,
  workflow_info = d$workflow_info
)

# Record schema version
record_schema_version(
  con = con,
  schema = "dev",
  version = "1.0.0",
  description = "Initial ingestion of NOAA CalCOFI Database",
  script_permalink = "https://github.com/CalCOFI/calcofi4db/blob/main/inst/create_db.qmd"
)

# Disconnect
dbDisconnect(con)
```

## Schema versioning

Each successful ingestion records a version in the `schema_version` table with:

- **version**: Semantic version (e.g., "1.0.0", "1.1.0")
- **description**: Changes in this version
- **date_created**: Ingestion timestamp
- **script_permalink**: GitHub permalink to ingestion script

Versions are archived as SQL dumps in Google Drive for reproducibility.

## Documentation

See the [CalCOFI Database Documentation](https://calcofi.io/docs/db.html) for complete details on:

- Database naming conventions
- Ingestion workflow architecture
- Schema versioning strategy
- Metadata management
- Publishing to data portals

## License

MIT
