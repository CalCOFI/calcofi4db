# calcofi4db

CalCOFI Database Tools package for ingesting and managing datasets in the CalCOFI database.

## Overview

This package provides functions for:

- Reading CSV files and extracting metadata about tables and fields
- Creating and managing redefinition files for tables and fields
- Transforming data according to redefinition rules
- Detecting changes between CSV files and database tables
- Ingesting datasets into the database with proper metadata
- Managing database relationships and indexes

## Installation

```r
# Install from GitHub
remotes::install_github("CalCOFI/calcofi4db")
```

## Usage

### Basic workflow

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

# Ingest a dataset
result <- ingest_dataset(
  con = con,
  provider = "swfsc.noaa.gov",
  dataset = "calcofi-db",
  dir_data = "/path/to/data",
  schema = "public",
  dir_googledata = "https://drive.google.com/drive/folders/your-folder-id",
  email = "your.email@example.com"
)

# Examine changes and statistics
result$changes
result$stats

# Disconnect
dbDisconnect(con)
```

### Step-by-step workflow

For more control over the process, you can use the individual functions:

```r
# Load CSV files and metadata
data_info <- read_csv_files(
  provider = "swfsc.noaa.gov",
  dataset = "calcofi-db",
  dir_data = "/path/to/data",
  dir_googledata = "https://drive.google.com/drive/folders/your-folder-id",
  use_gdrive = TRUE,
  email = "your.email@example.com"
)

# Transform data
transformed_data <- transform_data(data_info)

# Detect changes
changes <- detect_csv_changes(
  con = con,
  schema = "public",
  transformed_data = transformed_data,
  d_flds_rd = data_info$d_flds_rd
)

# Ingest data to database
stats <- ingest_csv_to_db(
  con = con,
  schema = "public",
  transformed_data = transformed_data,
  d_flds_rd = data_info$d_flds_rd,
  d_gdir_data = data_info$d_gdir_data,
  workflow_info = data_info$workflow_info
)
```

## License

MIT
