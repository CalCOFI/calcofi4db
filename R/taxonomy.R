# taxonomy standardization using WoRMS and ITIS APIs

#' Standardize Species Identifiers Using WoRMS/ITIS/GBIF APIs
#'
#' For each species in the species table, queries WoRMS (via `worrms` package)
#' to get the current accepted AphiaID, then queries ITIS (via `taxize`) for
#' the ITIS TSN, and optionally GBIF for the GBIF backbone key. Updates the
#' species table with canonical identifiers.
#'
#' @param con DBI connection to DuckDB with species table
#' @param species_tbl character; name of species table (default "species")
#' @param id_col character; name of species ID column (default "species_id")
#' @param sci_name_col character; column with scientific names
#'   (default "scientific_name")
#' @param update_in_place logical; if TRUE, UPDATE the table directly
#'   (default TRUE)
#' @param include_gbif logical; if TRUE, also query GBIF backbone
#'   (default TRUE)
#' @param batch_size integer; number of species per API batch (default 50)
#' @return tibble with species_id, scientific_name, worms_id (AphiaID),
#'   itis_id (TSN), gbif_id, taxonomic_status, accepted_name
#' @export
#' @concept taxonomy
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con("calcofi.duckdb")
#' sp_results <- standardize_species(con)
#' }
#' @importFrom DBI dbGetQuery dbExecute
#' @importFrom dplyr mutate select left_join bind_rows
#' @importFrom purrr map_dfr map_chr map_int
#' @importFrom tibble tibble
#' @importFrom glue glue
#' @importFrom rlang %||%
standardize_species <- function(
    con,
    species_tbl     = "species",
    id_col          = "species_id",
    sci_name_col    = "scientific_name",
    update_in_place = TRUE,
    include_gbif    = TRUE,
    batch_size      = 50) {

  if (!requireNamespace("worrms", quietly = TRUE))
    stop("Package 'worrms' is required. Install with: install.packages('worrms')")
  if (!requireNamespace("taxize", quietly = TRUE))
    stop("Package 'taxize' is required. Install with: install.packages('taxize')")

  # read species from duckdb
  species <- DBI::dbGetQuery(con, glue::glue(
    "SELECT {id_col}, {sci_name_col} FROM {species_tbl}
     WHERE {sci_name_col} IS NOT NULL"))

  n_species <- nrow(species)
  message(glue::glue("Standardizing {n_species} species..."))

  # process in batches
  batches <- split(seq_len(n_species), ceiling(seq_len(n_species) / batch_size))

  results <- purrr::map_dfr(seq_along(batches), function(b) {
    idx   <- batches[[b]]
    batch <- species[idx, ]
    message(glue::glue(
      "  batch {b}/{length(batches)}: {length(idx)} species"))

    purrr::map_dfr(seq_len(nrow(batch)), function(i) {
      sp_id   <- batch[[id_col]][i]
      sp_name <- batch[[sci_name_col]][i]

      # query WoRMS
      worms_id          <- NA_integer_
      taxonomic_status  <- NA_character_
      accepted_name     <- sp_name

      tryCatch({
        records <- worrms::wm_records_name(sp_name, fuzzy = FALSE)
        if (!is.null(records) && nrow(records) > 0) {
          rec <- records[1, ]
          worms_id         <- as.integer(rec$AphiaID)
          taxonomic_status <- rec$status

          # resolve synonyms
          if (!is.na(rec$status) &&
              tolower(rec$status) != "accepted" &&
              !is.na(rec$valid_AphiaID)) {
            worms_id      <- as.integer(rec$valid_AphiaID)
            accepted_name <- rec$valid_name %||% sp_name
            taxonomic_status <- "synonym"
          } else {
            accepted_name <- rec$scientificname %||% sp_name
          }
        }
      }, error = function(e) {
        # try fuzzy match if exact fails
        tryCatch({
          records <- worrms::wm_records_name(sp_name, fuzzy = TRUE)
          if (!is.null(records) && nrow(records) > 0) {
            rec <- records[1, ]
            worms_id         <<- as.integer(
              if (!is.na(rec$valid_AphiaID)) rec$valid_AphiaID else rec$AphiaID)
            accepted_name    <<- rec$valid_name %||% rec$scientificname %||% sp_name
            taxonomic_status <<- "fuzzy_match"
          }
        }, error = function(e2) NULL)
      })

      # rate limit: 0.5s between WoRMS calls
      Sys.sleep(0.5)

      # query ITIS for TSN
      itis_id <- NA_integer_
      tryCatch({
        tsn <- taxize::get_tsn_(accepted_name, rows = 1)
        if (!is.null(tsn) && length(tsn) > 0 && nrow(tsn[[1]]) > 0) {
          itis_id <- as.integer(tsn[[1]]$tsn[1])
        }
      }, error = function(e) NULL)

      # query GBIF backbone (optional)
      gbif_id <- NA_integer_
      if (include_gbif) {
        tryCatch({
          gbif <- taxize::get_gbifid_(accepted_name, rows = 1)
          if (!is.null(gbif) && length(gbif) > 0 && nrow(gbif[[1]]) > 0) {
            gbif_id <- as.integer(gbif[[1]]$usagekey[1])
          }
        }, error = function(e) NULL)
      }

      tibble::tibble(
        species_id       = sp_id,
        scientific_name  = sp_name,
        worms_id         = worms_id,
        itis_id          = itis_id,
        gbif_id          = gbif_id,
        taxonomic_status = taxonomic_status,
        accepted_name    = accepted_name)
    })
  })

  # update in place if requested
  if (update_in_place && nrow(results) > 0) {
    # add columns if they don't exist
    existing_cols <- DBI::dbGetQuery(con, glue::glue(
      "SELECT column_name FROM information_schema.columns
       WHERE table_name = '{species_tbl}'"))$column_name

    for (col in c("worms_id", "itis_id", "gbif_id")) {
      if (!col %in% existing_cols) {
        DBI::dbExecute(con, glue::glue(
          "ALTER TABLE {species_tbl} ADD COLUMN {col} INTEGER"))
        message(glue::glue("Added column {col} to {species_tbl}"))
      }
    }

    # write results to temp table and update
    DBI::dbExecute(con, "DROP TABLE IF EXISTS _sp_update")
    DBI::dbWriteTable(con, "_sp_update", results |>
      dplyr::select(species_id, worms_id, itis_id, gbif_id))

    DBI::dbExecute(con, glue::glue("
      UPDATE {species_tbl} SET
        worms_id = u.worms_id,
        itis_id  = u.itis_id,
        gbif_id  = u.gbif_id
      FROM _sp_update u
      WHERE {species_tbl}.{id_col} = u.species_id"))

    DBI::dbExecute(con, "DROP TABLE IF EXISTS _sp_update")

    n_worms <- sum(!is.na(results$worms_id))
    n_itis  <- sum(!is.na(results$itis_id))
    n_gbif  <- sum(!is.na(results$gbif_id))
    message(glue::glue(
      "Updated {species_tbl}: {n_worms} worms_id, {n_itis} itis_id, ",
      "{n_gbif} gbif_id out of {nrow(results)} species"))
  }

  results
}

#' Build Taxonomic Hierarchy Table from WoRMS
#'
#' For each unique `worms_id` in the species table, retrieves the full
#' classification from WoRMS (kingdom, phylum, class, order, family,
#' genus, species) and stores it in a taxon table. Optionally also
#' fetches ITIS classification.
#'
#' Also creates a `taxa_rank` lookup table with rank ordering consistent
#' with the WoRMS taxonomy hierarchy.
#'
#' @param con DBI connection to DuckDB
#' @param species_tbl character; name of species table with worms_id column
#' @param taxon_tbl character; name for output taxon table (default "taxon")
#' @param include_itis logical; also fetch ITIS classification (default TRUE)
#' @param batch_size integer; number of species per API batch (default 50)
#' @return tibble of taxon hierarchy rows written to con
#' @export
#' @concept taxonomy
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con("calcofi.duckdb")
#' taxon_rows <- build_taxon_table(con)
#' }
#' @importFrom DBI dbGetQuery dbWriteTable dbExecute
#' @importFrom purrr map_dfr
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows distinct filter
#' @importFrom glue glue
build_taxon_table <- function(
    con,
    species_tbl  = "species",
    taxon_tbl    = "taxon",
    include_itis = TRUE,
    batch_size   = 50) {

  if (!requireNamespace("worrms", quietly = TRUE))
    stop("Package 'worrms' is required. Install with: install.packages('worrms')")

  # get unique worms_ids
  worms_ids <- DBI::dbGetQuery(con, glue::glue(
    "SELECT DISTINCT worms_id FROM {species_tbl}
     WHERE worms_id IS NOT NULL"))$worms_id

  if (length(worms_ids) == 0) {
    message("No worms_id values found in species table. Run standardize_species() first.")
    return(tibble::tibble())
  }

  message(glue::glue("Building taxon hierarchy for {length(worms_ids)} unique WoRMS IDs..."))

  # process in batches
  batches <- split(worms_ids, ceiling(seq_along(worms_ids) / batch_size))

  all_taxa <- purrr::map_dfr(seq_along(batches), function(b) {
    ids <- batches[[b]]
    message(glue::glue("  batch {b}/{length(batches)}: {length(ids)} IDs"))

    purrr::map_dfr(ids, function(aid) {
      # WoRMS classification
      worms_taxa <- tryCatch({
        cl <- worrms::wm_classification(aid)
        if (!is.null(cl) && nrow(cl) > 0) {
          tibble::tibble(
            authority                = "WoRMS",
            taxonID                  = as.integer(cl$AphiaID),
            acceptedNameUsageID      = as.integer(aid),
            parentNameUsageID        = as.integer(c(NA, cl$AphiaID[-nrow(cl)])),
            scientificName           = cl$scientificname,
            taxonRank                = cl$rank,
            taxonomicStatus          = "accepted",
            scientificNameAuthorship = NA_character_)
        }
      }, error = function(e) NULL)

      Sys.sleep(0.3)  # rate limit

      # ITIS classification (optional)
      itis_taxa <- NULL
      if (include_itis) {
        tryCatch({
          if (requireNamespace("taxize", quietly = TRUE)) {
            # get itis_id for this worms_id from species table
            itis_row <- DBI::dbGetQuery(con, glue::glue(
              "SELECT itis_id FROM {species_tbl}
               WHERE worms_id = {aid} AND itis_id IS NOT NULL LIMIT 1"))
            if (nrow(itis_row) > 0) {
              cl_itis <- taxize::classification(
                itis_row$itis_id[1], db = "itis")
              if (!is.null(cl_itis) && !is.na(cl_itis[[1]]) &&
                  is.data.frame(cl_itis[[1]])) {
                df <- cl_itis[[1]]
                itis_taxa <- tibble::tibble(
                  authority                = "ITIS",
                  taxonID                  = as.integer(df$id),
                  acceptedNameUsageID      = as.integer(itis_row$itis_id[1]),
                  parentNameUsageID        = as.integer(c(NA, df$id[-nrow(df)])),
                  scientificName           = df$name,
                  taxonRank                = df$rank,
                  taxonomicStatus          = "accepted",
                  scientificNameAuthorship = NA_character_)
              }
            }
          }
        }, error = function(e) NULL)
      }

      dplyr::bind_rows(worms_taxa, itis_taxa)
    })
  })

  if (nrow(all_taxa) == 0) {
    message("No taxon hierarchy data retrieved")
    return(tibble::tibble())
  }

  # deduplicate
  all_taxa <- all_taxa |>
    dplyr::distinct(authority, taxonID, .keep_all = TRUE)

  # write taxon table
  DBI::dbExecute(con, glue::glue("DROP TABLE IF EXISTS {taxon_tbl}"))
  DBI::dbWriteTable(con, taxon_tbl, all_taxa)
  message(glue::glue(
    "Created {taxon_tbl} table: {nrow(all_taxa)} rows"))

  # create taxa_rank lookup table
  # rank ordering from int-app/taxa_worms.qmd
  taxa_ranks_chr <- c(
    "Kingdom", "Subkingdom",
    "Phylum", "Subphylum", "Infraphylum",
    "Superclass", "Class", "Subclass", "Infraclass", "Megacohort",
    "Supercohort", "Cohort", "Subcohort", "Infracohort",
    "Superorder", "Order", "Suborder", "Infraorder", "Parvorder",
    "Superfamily", "Family", "Subfamily",
    "Supertribe", "Tribe", "Subtribe",
    "Genus", "Subgenus",
    "Series", "Subseries",
    "Species", "Subspecies",
    "Natio", "Mutatio",
    "Form", "Forma", "Subform", "Subforma",
    "Variety", "Subvariety",
    "Coll. sp.", "Aggr.")

  d_taxa_rank <- tibble::tibble(
    taxonRank  = taxa_ranks_chr,
    rank_order = seq_along(taxa_ranks_chr))

  DBI::dbExecute(con, "DROP TABLE IF EXISTS taxa_rank")
  DBI::dbWriteTable(con, "taxa_rank", d_taxa_rank)
  message(glue::glue(
    "Created taxa_rank table: {nrow(d_taxa_rank)} rank levels"))

  all_taxa
}
