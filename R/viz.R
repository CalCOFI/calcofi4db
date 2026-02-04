#' @title Show source files
#' @description Show source CSV files with local and GCS paths for provenance
#' @param d data object output from `read_csv_files()`
#' @return DT datatable showing source files with paths and row counts
#' @export
#' @importFrom dplyr mutate select
#' @importFrom DT datatable
#' @importFrom glue glue
#' @concept viz
show_source_files <- function(d) {
  # use source_files dataframe for provenance display
  d$source_files |>
    dplyr::mutate(
      local_file   = basename(local_path),
      gcs_file     = ifelse(
        is.na(gcs_path),
        NA_character_,
        glue::glue("<a href='https://storage.googleapis.com/{gsub(\"gs://\", \"\", gcs_path)}' target='_blank'>{basename(gcs_path)}</a>")),
      file_size_kb = round(file_size / 1024, 1),
      last_mod     = format(last_modified, "%Y-%m-%d %H:%M")) |>
    dplyr::select(
      table,
      rows         = nrow,
      cols         = ncol,
      file_size_kb,
      last_mod,
      local_file,
      gcs_file,
      local_path,
      gcs_path) |>
    DT::datatable(
      escape   = FALSE,
      rownames = FALSE,
      caption  = "Source CSV files with local and GCS paths for provenance.",
      options  = list(
        pageLength = 20,
        columnDefs = list(
          list(targets = c("local_path", "gcs_path"), visible = FALSE))))
}

#' @title Show fields to redefine
#' @description Show tables to redefine
#' @param d data object output from `read_csv_files()`
#' @return Data frame with tables to redefine
#' @export
#' @importFrom dplyr mutate select
#' @importFrom DT datatable formatStyle
#' @concept viz
show_fields_redefine <- function(d) {
  d$d_flds_rd |>
    mutate(
      tbl_is_equal   = tbl_old   == tbl_new,
      fld_is_equal   = fld_old   == fld_new,
      type_is_equal  = type_old  == type_new,
      order_is_equal = order_old == order_new,
      tbl = ifelse(
        tbl_is_equal,
        tbl_old,
        glue("{tbl_old} → {tbl_new}")),
      fld = ifelse(
        fld_is_equal,
        fld_old,
        glue("{fld_old} → {fld_new}")),
      type = ifelse(
        type_is_equal,
        type_old,
        glue("{type_old} → {type_new}")),
      order = ifelse(
        order_is_equal,
        order_old,
        glue("{order_old} → {order_new}"))) |>
    select(
      -tbl_old,   -tbl_new,
      -fld_old,   -fld_new,
      -type_old,  -type_new,
      -order_old, -order_new) |>
    relocate(tbl, fld, type, order) |>
    datatable(
      caption = "Fields to redefine.",
      rownames = F,
      options = list(
        colReorder = T,
        rowGroup = list(dataSrc = 0),
        pageLength = 50,
        columnDefs = list(list(
          targets = c(
            "tbl",
            "tbl_is_equal", "fld_is_equal",
            "type_is_equal", "order_is_equal"),
          visible = F))),
      extensions = c("ColReorder", "RowGroup", "Responsive")) |>
    formatStyle(
      "fld",
      backgroundColor = styleEqual(
        c(T, F),
        c("lightgray","lightgreen")),
      valueColumns    = "fld_is_equal") |>
    formatStyle(
      "type",
      backgroundColor = styleEqual(
        c(T, F),
        c("lightgray","lightgreen")),
      valueColumns    = "type_is_equal") |>
    formatStyle(
      "order",
      backgroundColor = styleEqual(
        c(T, F),
        c("lightgray","lightgreen")),
      valueColumns    = "order_is_equal")

}

#' @title Show tables to redefine
#' @description Show tables to redefine
#' @param d data object output from `read_csv_files()`
#' @return Data frame with tables to redefine
#' @export
#' @importFrom dplyr mutate select
#' @importFrom DT datatable formatStyle
#' @concept viz
show_tables_redefine = function(d) {

  stopifnot("d_tbls_rd" %in% names(d))

  d$d_tbls_rd |>
    dplyr::mutate(
      is_equal = tbl_old == tbl_new) |>
    DT::datatable(
      caption = "Tables to redefine.",
      options = list(
        columnDefs = list(list(
          targets = "is_equal", visible = F)))) |>
    DT::formatStyle(
      "tbl_new",
      backgroundColor = styleEqual(
        c(T,F),
        c("lightgray","lightgreen")),
      valueColumns    = "is_equal")
}

