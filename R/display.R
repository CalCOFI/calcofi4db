# display helper functions for workflow outputs

#' Create GitHub File Link
#'
#' Converts a local file path to a GitHub repository link. Extracts the relative
#' path from the workflows directory and creates an HTML anchor tag.
#'
#' @param file_path Local file path (can be full path or relative)
#' @param repo GitHub repository in format "owner/repo" (default: "CalCOFI/workflows")
#' @param branch Git branch (default: "main")
#' @param base_dir Base directory to extract relative path from (default: "workflows")
#'
#' @return HTML anchor tag string linking to the GitHub file
#' @export
#' @concept display
#'
#' @examples
#' \dontrun{
#' github_file_link("/Users/bbest/Github/CalCOFI/workflows/data/flagged/orphan_species.csv")
#' # Returns: "<a href='https://github.com/CalCOFI/workflows/blob/main/data/flagged/orphan_species.csv'>
#' #           calcofi/workflows: data/flagged/orphan_species.csv</a>"
#' }
#' @importFrom glue glue
github_file_link <- function(
    file_path,
    repo     = "CalCOFI/workflows",
    branch   = "main",
    base_dir = "workflows") {

  if (is.na(file_path) || is.null(file_path) || file_path == "") {
    return("")
  }

 # extract relative path from base_dir
  if (grepl(base_dir, file_path)) {
    # find position after base_dir/
    rel_path <- sub(paste0(".*", base_dir, "/"), "", file_path)
  } else {
    # use basename if can't find base_dir
    rel_path <- basename(file_path)
  }

  # create link
  url <- glue::glue("https://github.com/{repo}/blob/{branch}/{rel_path}")
  display_text <- glue::glue("{tolower(repo)}: {rel_path}")

  glue::glue("<a href='{url}' target='_blank'>{display_text}</a>")
}

#' Show Validation Results with GitHub Links
#'
#' Displays validation results from `validate_dataset()` as a datatable with
#' output_file paths converted to clickable GitHub links.
#'
#' @param validation_results Results from `validate_dataset()` containing `$checks` tibble
#' @param caption Table caption (default: "Validation Results")
#' @param repo GitHub repository (default: "CalCOFI/workflows")
#' @param branch Git branch (default: "main")
#'
#' @return DT datatable object
#' @export
#' @concept display
#'
#' @examples
#' \dontrun{
#' validation_results <- validate_dataset(con, validations, output_dir)
#' show_validation_results(validation_results)
#' }
#' @importFrom dplyr mutate
#' @importFrom DT datatable
#' @importFrom purrr map_chr
show_validation_results <- function(
    validation_results,
    caption = "Validation Results",
    repo    = "CalCOFI/workflows",
    branch  = "main") {

  validation_results$checks |>
    dplyr::mutate(
      output_file = purrr::map_chr(
        output_file,
        ~github_file_link(.x, repo = repo, branch = branch))) |>
    DT::datatable(
      caption = caption,
      escape  = FALSE)
}

#' Show Flagged File Result
#'
#' Displays the result of `flag_invalid_rows()` with a GitHub link to the
#' flagged file and summary statistics.
#'
#' @param invalid_rows Tibble of invalid rows that were flagged
#' @param output_path Path to the flagged CSV file
#' @param description Description of what was flagged
#' @param repo GitHub repository (default: "CalCOFI/workflows")
#' @param branch Git branch (default: "main")
#'
#' @return HTML string with flagged file summary and link
#' @export
#' @concept display
#'
#' @examples
#' \dontrun{
#' invalid_stages <- validate_egg_stages(con, "egg_stage", "stage")
#' if (nrow(invalid_stages) > 0) {
#'   output_path <- flag_invalid_rows(invalid_stages, "data/flagged/invalid_egg_stages.csv", "Invalid egg stages")
#'   show_flagged_file(invalid_stages, output_path, "Invalid egg stages")
#' }
#' }
#' @importFrom glue glue
#' @importFrom htmltools HTML
show_flagged_file <- function(
    invalid_rows,
    output_path,
    description,
    repo   = "CalCOFI/workflows",
    branch = "main") {

  n_rows <- nrow(invalid_rows)

  if (n_rows == 0) {
    return(htmltools::HTML(glue::glue(
      "<p><strong>{description}</strong>: No invalid rows found ✓</p>")))
  }

  link <- github_file_link(output_path, repo = repo, branch = branch)

  htmltools::HTML(glue::glue(
    "<p><strong>{description}</strong>: {format(n_rows, big.mark = ',')} rows flagged → {link}</p>"))
}
