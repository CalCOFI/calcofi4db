# Metadata for `.zenodo.json` and `CITATION.cff` at the workflows repo root

Zenodo's GitHub integration fills a record from `.zenodo.json` at each
release tag; without it the alpha record came out as "CalCOFI/workflows:
initial Zenodo release", MIT, creators = the GitHub contributors
(measured 2026-09-03). `zenodo_metadata()` makes the record a
**dataset**: title "CalCOFI Integrated Database", creators = the three
partners as organisations, contributors = every dataset's PIs
(`DataCollector`, from `pi_names`) and the curators (`DataCurator`),
license `cc-by-4.0` for the record (the code stays MIT in `LICENSE`,
said in the description), the GCS release as `isSupplementTo` and
db-schema as `isDocumentedBy`. `version` is omitted unless given,
because Zenodo takes it from the tag. `citation_cff()` is the same
record for GitHub's "Cite this repository", carrying the concept DOI.
`write_citation_files()` writes both.

## Usage

``` r
zenodo_metadata(
  dataset_df,
  version = NULL,
  publication_date = NULL,
  curators = CC_RELEASE_CURATORS
)

citation_cff(version, date_released, doi = CC_ZENODO_CONCEPT_DOI)

write_citation_files(
  dir,
  dataset_df,
  version,
  date_released,
  zenodo_version = NULL,
  doi = CC_ZENODO_CONCEPT_DOI
)
```

## Arguments

- dataset_df:

  the dataset table from
  [`ingest_yaml_to_dataset_df()`](https://calcofi.io/calcofi4db/reference/ingest_yaml_to_dataset_df.md)
  (only `pi_names` is read)

- version:

  the release tag, when the record is for one release

- publication_date:

  `"YYYY-MM-DD"`, optional

- curators:

  the DataCurator contributors

- date_released:

  `"YYYY-MM-DD"`

- doi:

  the DOI `CITATION.cff` carries (default: the concept DOI)

- dir:

  the workflows repo root

- zenodo_version:

  the `version` written into `.zenodo.json` (default `NULL`: Zenodo
  takes it from the tag)

## Value

A list, ready for `jsonlite::write_json(auto_unbox = TRUE)`.

`write_citation_files()`: the two paths, named.
