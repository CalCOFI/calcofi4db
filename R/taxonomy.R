# taxonomy standardization using WoRMS and ITIS APIs
# includes both API-based and local spp.duckdb-based approaches

#' Standardize Species Using Local spp.duckdb Lookups
#'
#' Updates the species table with WoRMS AphiaID, ITIS TSN, and GBIF backbone
#' key using fast SQL joins against a local MarineSensitivity species database
#' (\code{spp.duckdb}). Falls back to the WoRMS API only for species not found
#' locally.
#'
#' This is much faster than \code{standardize_species()} which queries external
#' APIs for every species. Intended for use in ingest workflows where the local
#' spp.duckdb is available.
#'
#' @param con DBI connection to DuckDB with a species table
#' @param spp_db_path Path to the MarineSensitivity spp.duckdb file
#' @param species_tbl Character; name of species table (default "species")
#' @param overwrite Logical; if TRUE, re-standardize even if columns already
#'   populated (default FALSE)
#' @param api_fallback Logical; if TRUE, query WoRMS API for species not found
#'   locally (default TRUE)
#'
#' @return Tibble with species_id, scientific_name, worms_id, itis_id, gbif_id
#' @export
#' @concept taxonomy
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con("calcofi.duckdb")
#' sp <- standardize_species_local(
#'   con         = con,
#'   spp_db_path = "/path/to/spp.duckdb")
#' }
#' @importFrom DBI dbGetQuery dbExecute
#' @importFrom glue glue
standardize_species_local <- function(
    con,
    spp_db_path,
    species_tbl = "species",
    overwrite   = FALSE,
    api_fallback = TRUE) {

  stopifnot(file.exists(spp_db_path))

  # check if species already standardized
  sp_cols <- DBI::dbGetQuery(con, glue::glue(
    "SELECT column_name FROM information_schema.columns
     WHERE table_name = '{species_tbl}'"))$column_name

  sp_standardized <- all(c("worms_id", "itis_id", "gbif_id") %in% sp_cols) &&
    DBI::dbGetQuery(con, glue::glue(
      "SELECT COUNT(*) AS n FROM {species_tbl} WHERE gbif_id IS NOT NULL"))$n > 0

  if (sp_standardized && !overwrite) {
    message("Species already standardized, skipping")
    return(DBI::dbGetQuery(con, glue::glue(
      "SELECT species_id, scientific_name, worms_id, itis_id, gbif_id
       FROM {species_tbl}")))
  }

  # add missing columns
  for (col in c("worms_id", "itis_id", "gbif_id")) {
    if (!col %in% sp_cols) {
      DBI::dbExecute(con, glue::glue(
        "ALTER TABLE {species_tbl} ADD COLUMN IF NOT EXISTS {col} INTEGER"))
    }
  }

  # attach spp.duckdb
  tryCatch(
    DBI::dbExecute(con, glue::glue("ATTACH '{spp_db_path}' AS spp (READ_ONLY)")),
    error = function(e) NULL)

  # worms: validate existing worms_id and resolve synonyms
  n_worms_before <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {species_tbl} WHERE worms_id IS NOT NULL"))$n

  DBI::dbExecute(con, glue::glue("
    UPDATE {species_tbl} SET worms_id = (
      SELECT CASE
        WHEN w.taxonomicStatus = 'accepted' THEN w.taxonID
        ELSE COALESCE(w.acceptedNameUsageID, w.taxonID)
      END
      FROM spp.worms w
      WHERE w.taxonID = {species_tbl}.worms_id
      LIMIT 1)
    WHERE worms_id IS NOT NULL"))

  n_worms <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {species_tbl} WHERE worms_id IS NOT NULL"))$n
  message(glue::glue("WoRMS: {n_worms} species with validated worms_id (was {n_worms_before})"))

  # worms: name match for any still missing
  DBI::dbExecute(con, glue::glue("
    UPDATE {species_tbl} SET worms_id = (
      SELECT CASE
        WHEN w.taxonomicStatus = 'accepted' THEN w.taxonID
        ELSE COALESCE(w.acceptedNameUsageID, w.taxonID)
      END
      FROM spp.worms w
      WHERE w.scientificName = {species_tbl}.scientific_name
        AND w.taxonRank = 'Species'
      ORDER BY CASE WHEN w.taxonomicStatus = 'accepted' THEN 0 ELSE 1 END
      LIMIT 1)
    WHERE worms_id IS NULL"))

  n_worms2 <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {species_tbl} WHERE worms_id IS NOT NULL"))$n
  message(glue::glue("WoRMS name match: {n_worms2 - n_worms} additional species"))

  # itis: match by scientific_name
  DBI::dbExecute(con, glue::glue("
    UPDATE {species_tbl} SET itis_id = (
      SELECT COALESCE(i.acceptedNameUsageID, i.taxonID)
      FROM spp.itis i
      WHERE i.scientificName = {species_tbl}.scientific_name
      ORDER BY CASE WHEN i.taxonomicStatus = 'valid' THEN 0 ELSE 1 END
      LIMIT 1)
    WHERE itis_id IS NULL"))

  n_itis <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {species_tbl} WHERE itis_id IS NOT NULL"))$n
  message(glue::glue("ITIS: {n_itis} species matched"))

  # gbif: match by canonicalName
  DBI::dbExecute(con, glue::glue("
    UPDATE {species_tbl} SET gbif_id = (
      SELECT COALESCE(
        TRY_CAST(NULLIF(g.acceptedNameUsageID, '') AS INTEGER),
        g.taxonID)
      FROM spp.gbif g
      WHERE g.canonicalName = {species_tbl}.scientific_name
      ORDER BY CASE WHEN g.taxonomicStatus = 'accepted' THEN 0 ELSE 1 END
      LIMIT 1)
    WHERE gbif_id IS NULL"))

  n_gbif <- DBI::dbGetQuery(con, glue::glue(
    "SELECT COUNT(*) AS n FROM {species_tbl} WHERE gbif_id IS NOT NULL"))$n
  message(glue::glue("GBIF: {n_gbif} species matched"))

  # API fallback for species still missing worms_id
  if (api_fallback) {
    missing_worms <- DBI::dbGetQuery(con, glue::glue(
      "SELECT species_id, scientific_name
       FROM {species_tbl} WHERE worms_id IS NULL"))

    if (nrow(missing_worms) > 0) {
      if (!requireNamespace("worrms", quietly = TRUE))
        stop("Package 'worrms' is required for API fallback")

      message(glue::glue(
        "API fallback for {nrow(missing_worms)} species missing worms_id"))

      for (i in seq_len(nrow(missing_worms))) {
        sp_name <- missing_worms$scientific_name[i]
        sp_id   <- missing_worms$species_id[i]

        worms_id <- tryCatch({
          records <- worrms::wm_records_name(sp_name, fuzzy = TRUE)
          if (!is.null(records) && nrow(records) > 0) {
            rec <- records[1, ]
            if (!is.na(rec$valid_AphiaID)) as.integer(rec$valid_AphiaID)
            else as.integer(rec$AphiaID)
          } else {
            NA_integer_
          }
        }, error = function(e) NA_integer_)

        if (!is.na(worms_id)) {
          DBI::dbExecute(con, glue::glue(
            "UPDATE {species_tbl} SET worms_id = {worms_id}
             WHERE species_id = {sp_id}"))
        }
        Sys.sleep(0.5)
      }
      message("API fallback complete")
    } else {
      message("All species matched locally, no API fallback needed")
    }
  }

  # detach spp.duckdb
  tryCatch(DBI::dbExecute(con, "DETACH spp"), error = function(e) NULL)

  DBI::dbGetQuery(con, glue::glue(
    "SELECT species_id, scientific_name, worms_id, itis_id, gbif_id
     FROM {species_tbl}"))
}

#' Build Taxon Hierarchy from Local spp.duckdb via Recursive CTEs
#'
#' Builds the \code{taxon} and \code{taxa_rank} tables using recursive CTEs
#' against the local MarineSensitivity species database (\code{spp.duckdb}).
#' This is much faster than \code{build_taxon_table()} which queries WoRMS/ITIS
#' APIs for each species.
#'
#' Creates both WoRMS and ITIS hierarchies by walking up the
#' \code{parentNameUsageID} chain from each species' WoRMS/ITIS ID.
#'
#' @param con DBI connection to DuckDB with a species table containing
#'   worms_id and itis_id columns
#' @param spp_db_path Path to the MarineSensitivity spp.duckdb file
#' @param species_tbl Character; name of species table (default "species")
#' @param overwrite Logical; if TRUE, rebuild even if taxon table exists
#'   (default FALSE)
#'
#' @return Tibble of taxon hierarchy rows written to con
#' @export
#' @concept taxonomy
#'
#' @examples
#' \dontrun{
#' con <- get_duckdb_con("calcofi.duckdb")
#' taxon <- build_taxon_hierarchy(
#'   con         = con,
#'   spp_db_path = "/path/to/spp.duckdb")
#' }
#' @importFrom DBI dbGetQuery dbWriteTable dbExecute dbListTables
#' @importFrom glue glue
#' @importFrom dplyr distinct bind_rows
#' @importFrom tibble tibble
build_taxon_hierarchy <- function(
    con,
    spp_db_path,
    species_tbl = "species",
    overwrite   = FALSE) {

  # check if taxon table already exists
  existing <- DBI::dbListTables(con)
  taxon_exists <- "taxon" %in% existing &&
    DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM taxon")$n > 0

  if (taxon_exists && !overwrite) {
    message("Taxon table already exists, skipping")
    return(DBI::dbGetQuery(con, "SELECT * FROM taxon"))
  }

  stopifnot(file.exists(spp_db_path))

  # ensure spp.duckdb is attached
  spp_attached <- tryCatch({
    DBI::dbGetQuery(con, "SELECT 1 FROM spp.worms LIMIT 1")
    TRUE
  }, error = function(e) FALSE)

  if (!spp_attached) {
    DBI::dbExecute(con, glue::glue(
      "ATTACH '{spp_db_path}' AS spp (READ_ONLY)"))
  }

  # build WoRMS hierarchy via recursive CTE
  worms_taxon <- DBI::dbGetQuery(con, glue::glue("
    WITH RECURSIVE hierarchy AS (
      SELECT
        w.taxonID,
        w.parentNameUsageID,
        w.scientificName,
        w.taxonRank,
        w.taxonID AS leaf_taxonID
      FROM spp.worms w
      WHERE w.taxonID IN (
        SELECT DISTINCT worms_id FROM {species_tbl}
        WHERE worms_id IS NOT NULL)

      UNION ALL

      SELECT
        p.taxonID,
        p.parentNameUsageID,
        p.scientificName,
        p.taxonRank,
        h.leaf_taxonID
      FROM spp.worms p
      JOIN hierarchy h ON p.taxonID = h.parentNameUsageID
      WHERE h.parentNameUsageID IS NOT NULL
        AND h.parentNameUsageID != h.taxonID
    )
    SELECT DISTINCT
      'WoRMS'                        AS authority,
      taxonID                        AS taxonID,
      leaf_taxonID                   AS acceptedNameUsageID,
      parentNameUsageID              AS parentNameUsageID,
      scientificName                 AS scientificName,
      taxonRank                      AS taxonRank,
      'accepted'                     AS taxonomicStatus,
      CAST(NULL AS VARCHAR)          AS scientificNameAuthorship
    FROM hierarchy
    WHERE taxonRank IS NOT NULL"))

  worms_taxon <- worms_taxon |>
    dplyr::distinct(authority, taxonID, .keep_all = TRUE)
  message(glue::glue("WoRMS hierarchy: {nrow(worms_taxon)} rows"))

  # build ITIS hierarchy via recursive CTE
  itis_taxon <- DBI::dbGetQuery(con, glue::glue("
    WITH RECURSIVE hierarchy AS (
      SELECT
        i.taxonID,
        i.parentNameUsageID,
        i.scientificName,
        i.taxonRank,
        i.taxonID AS leaf_taxonID
      FROM spp.itis i
      WHERE i.taxonID IN (
        SELECT DISTINCT itis_id FROM {species_tbl}
        WHERE itis_id IS NOT NULL)

      UNION ALL

      SELECT
        p.taxonID,
        p.parentNameUsageID,
        p.scientificName,
        p.taxonRank,
        h.leaf_taxonID
      FROM spp.itis p
      JOIN hierarchy h ON p.taxonID = h.parentNameUsageID
      WHERE h.parentNameUsageID IS NOT NULL
        AND h.parentNameUsageID != h.taxonID
    )
    SELECT DISTINCT
      'ITIS'                         AS authority,
      taxonID                        AS taxonID,
      leaf_taxonID                   AS acceptedNameUsageID,
      parentNameUsageID              AS parentNameUsageID,
      scientificName                 AS scientificName,
      taxonRank                      AS taxonRank,
      'accepted'                     AS taxonomicStatus,
      CAST(NULL AS VARCHAR)          AS scientificNameAuthorship
    FROM hierarchy
    WHERE taxonRank IS NOT NULL"))

  itis_taxon <- itis_taxon |>
    dplyr::distinct(authority, taxonID, .keep_all = TRUE)
  message(glue::glue("ITIS hierarchy: {nrow(itis_taxon)} rows"))

  # combine and write
  taxon_rows <- dplyr::bind_rows(worms_taxon, itis_taxon)

  DBI::dbExecute(con, "DROP TABLE IF EXISTS taxon")
  DBI::dbWriteTable(con, "taxon", taxon_rows)
  message(glue::glue("Created taxon table: {nrow(taxon_rows)} rows"))

  # create taxa_rank lookup table
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
  message(glue::glue("Created taxa_rank table: {nrow(d_taxa_rank)} rank levels"))

  # detach spp.duckdb
  tryCatch(DBI::dbExecute(con, "DETACH spp"), error = function(e) NULL)

  taxon_rows
}

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
          "ALTER TABLE {species_tbl} ADD COLUMN IF NOT EXISTS {col} INTEGER"))
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
  # rank ordering from db-viz-hex/taxa_worms.qmd
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
