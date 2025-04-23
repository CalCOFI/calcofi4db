#' @keywords internal
"_PACKAGE"

# usethis::use_import_from(package = "uuid", fun = "as.UUID")
## usethis namespace: start
#' @importFrom DBI Id dbConnect dbDisconnect dbExecute dbExistsTable dbGetQuery dbListFields
#' @importFrom DBI dbListTables dbReadTable dbWriteTable
#' @importFrom dm dm_add_fk dm_draw dm_enum_pk_candidates dm_from_con dm_get_all_uks dm_set_colors
#' @importFrom fs path_real
#' @importFrom glue glue
#' @importFrom googledrive drive_auth drive_ls
#' @importFrom here here
#' @importFrom janitor make_clean_names
#' @importFrom jsonlite toJSON
#' @importFrom lubridate as_datetime year
#' @importFrom purrr map map2 map2_chr map2_lgl map_chr map_int map_lgl pluck
#' @importFrom readr read_csv write_csv
#' @importFrom rlang sym parse_expr
#' @importFrom stringr str_detect
#' @importFrom tibble deframe tibble
#' @importFrom tidyr unnest
#' @importFrom uuid as.UUID
## usethis namespace: end
NULL
