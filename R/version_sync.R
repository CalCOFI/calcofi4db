#' Synchronized Version Management for Package and Database
#'
#' These functions manage synchronized versioning between the R package,
#' database schema, NEWS.md file, and git commits with GitHub permalinks.

#' Update Package Version and NEWS
#'
#' Updates the package DESCRIPTION version and prepends a new entry to NEWS.md.
#' This should be called before committing and recording the schema version.
#'
#' @param version New semantic version number (e.g., "1.0.0")
#' @param description Description of changes for NEWS.md
#' @param news_items Character vector of bullet points for NEWS.md (optional)
#'
#' @return List with updated version info
#' @export
#' @concept version_sync
#'
#' @examples
#' \dontrun{
#' update_package_version(
#'   version = "1.0.0",
#'   description = "Initial production release",
#'   news_items = c(
#'     "Complete NOAA CalCOFI Database ingestion",
#'     "Add schema versioning system",
#'     "Create master ingestion workflow"
#'   )
#' )
#' }
#' @importFrom desc description
#' @importFrom glue glue
update_package_version <- function(
    version,
    description,
    news_items = NULL) {

  # validate version format
  if (!grepl("^\\d+\\.\\d+\\.\\d+$", version)) {
    stop("Version must be in semantic versioning format: MAJOR.MINOR.PATCH (e.g., '1.0.0')")
  }

  # paths
  desc_path <- here::here("DESCRIPTION")
  news_path <- here::here("NEWS.md")

  # update DESCRIPTION file
  if (!file.exists(desc_path)) {
    stop(glue::glue("DESCRIPTION file not found: {desc_path}"))
  }

  # read and update DESCRIPTION
  desc_lines <- readLines(desc_path)
  version_line <- grep("^Version:", desc_lines)

  if (length(version_line) == 0) {
    stop("Version field not found in DESCRIPTION")
  }

  desc_lines[version_line] <- glue::glue("Version: {version}")
  writeLines(desc_lines, desc_path)

  # update NEWS.md
  if (!file.exists(news_path)) {
    # create new NEWS.md
    news_content <- character()
  } else {
    news_content <- readLines(news_path)
  }

  # create new news entry
  new_entry <- c(
    glue::glue("# calcofi4db {version}"),
    "",
    glue::glue("*{description}*"),
    ""
  )

  # add news items if provided
  if (!is.null(news_items) && length(news_items) > 0) {
    new_entry <- c(
      new_entry,
      paste0("* ", news_items),
      ""
    )
  }

  # prepend to existing news
  updated_news <- c(new_entry, news_content)
  writeLines(updated_news, news_path)

  message(glue::glue("Updated package version to {version}"))
  message(glue::glue("Updated NEWS.md with new entry"))

  invisible(list(
    version = version,
    desc_path = desc_path,
    news_path = news_path
  ))
}

#' Commit Version Changes and Get Permalink
#'
#' Commits DESCRIPTION, NEWS.md, and specified files to git, then generates
#' a GitHub permalink to the ingestion script at the committed version.
#'
#' @param version Version number for commit message
#' @param files Additional files to commit (e.g., "inst/create_db.qmd")
#' @param repo_owner GitHub repository owner (default: "CalCOFI")
#' @param repo_name GitHub repository name (default: "calcofi4db")
#' @param script_path Path to script for permalink (default: "inst/create_db.qmd")
#' @param commit Logical, whether to actually commit (default: FALSE for safety)
#' @param push Logical, whether to push to remote (default: FALSE for safety)
#'
#' @return List with commit hash and permalink
#' @export
#' @concept version_sync
#'
#' @examples
#' \dontrun{
#' # Dry run (no commit)
#' result <- commit_version_and_permalink(
#'   version = "1.0.0",
#'   files = c("inst/create_db.qmd", "inst/schema_version.csv")
#' )
#'
#' # Actually commit and push
#' result <- commit_version_and_permalink(
#'   version = "1.0.0",
#'   files = c("inst/create_db.qmd", "inst/schema_version.csv"),
#'   commit = TRUE,
#'   push = TRUE
#' )
#' }
commit_version_and_permalink <- function(
    version,
    files = NULL,
    repo_owner = "CalCOFI",
    repo_name = "calcofi4db",
    script_path = "inst/create_db.qmd",
    commit = FALSE,
    push = FALSE) {

  # check if git is available
  git_available <- system2("git", "--version", stdout = FALSE, stderr = FALSE) == 0
  if (!git_available) {
    stop("Git is not available in the system PATH")
  }

  # check if in git repo
  is_repo <- system2("git", c("rev-parse", "--git-dir"), stdout = FALSE, stderr = FALSE) == 0
  if (!is_repo) {
    stop("Not in a git repository")
  }

  # files to commit
  commit_files <- c("DESCRIPTION", "NEWS.md")
  if (!is.null(files)) {
    commit_files <- c(commit_files, files)
  }

  # check if files exist
  missing_files <- commit_files[!file.exists(commit_files)]
  if (length(missing_files) > 0) {
    warning(glue::glue("Some files don't exist: {paste(missing_files, collapse=', ')}"))
    commit_files <- commit_files[file.exists(commit_files)]
  }

  if (!commit) {
    message("Dry run mode - would commit these files:")
    message(paste("  -", commit_files, collapse = "\n"))
    message(glue::glue("Commit message: 'Release version {version}'"))

    return(list(
      version = version,
      commit_hash = "dry-run-no-hash",
      permalink = glue::glue("https://github.com/{repo_owner}/{repo_name}/blob/main/{script_path}"),
      committed = FALSE
    ))
  }

  # stage files
  system2("git", c("add", commit_files))

  # commit
  commit_msg <- glue::glue("Release version {version}")
  system2("git", c("commit", "-m", shQuote(commit_msg)))

  # get commit hash
  commit_hash <- system2("git", c("rev-parse", "HEAD"), stdout = TRUE)

  # push if requested
  if (push) {
    message("Pushing to remote repository...")
    push_result <- system2("git", c("push"), stdout = TRUE, stderr = TRUE)
    message(paste(push_result, collapse = "\n"))
  }

  # generate permalink
  permalink <- glue::glue("https://github.com/{repo_owner}/{repo_name}/blob/{commit_hash}/{script_path}")

  message(glue::glue("Committed version {version}"))
  message(glue::glue("Commit hash: {commit_hash}"))
  message(glue::glue("Permalink: {permalink}"))

  list(
    version = version,
    commit_hash = commit_hash,
    permalink = permalink,
    committed = TRUE,
    pushed = push
  )
}

#' Complete Version Release Workflow
#'
#' Performs the complete version release workflow:
#' 1. Updates package DESCRIPTION and NEWS.md
#' 2. Commits changes to git
#' 3. Records schema version in database
#' 4. Updates schema_version.csv
#'
#' @param con Database connection
#' @param schema Database schema (default: "dev")
#' @param version New version number
#' @param description Version description
#' @param news_items News items for NEWS.md (optional)
#' @param additional_files Additional files to commit (optional)
#' @param commit Logical, whether to commit (default: FALSE)
#' @param push Logical, whether to push (default: FALSE)
#' @param repo_owner GitHub owner (default: "CalCOFI")
#' @param repo_name GitHub repo (default: "calcofi4db")
#'
#' @return List with version info, commit details, and database record
#' @export
#' @concept version_sync
#'
#' @examples
#' \dontrun{
#' # Complete release workflow
#' release_result <- complete_version_release(
#'   con = con_dev,
#'   schema = "dev",
#'   version = "1.0.0",
#'   description = "Initial production release with NOAA CalCOFI Database",
#'   news_items = c(
#'     "Complete NOAA CalCOFI Database ingestion",
#'     "Add synchronized versioning system",
#'     "Create master ingestion workflow"
#'   ),
#'   additional_files = c("inst/create_db.qmd", "inst/schema_version.csv"),
#'   commit = TRUE,
#'   push = TRUE
#' )
#' }
complete_version_release <- function(
    con,
    schema = "dev",
    version,
    description,
    news_items = NULL,
    additional_files = NULL,
    commit = FALSE,
    push = FALSE,
    repo_owner = "CalCOFI",
    repo_name = "calcofi4db") {

  message(glue::glue("Starting version release workflow for {version}..."))

  # step 1: update package version and NEWS
  message("\n1. Updating package version and NEWS.md...")
  pkg_update <- update_package_version(
    version = version,
    description = description,
    news_items = news_items
  )

  # step 2: commit and get permalink
  message("\n2. Committing changes to git...")
  git_result <- commit_version_and_permalink(
    version = version,
    files = additional_files,
    repo_owner = repo_owner,
    repo_name = repo_name,
    script_path = "inst/create_db.qmd",
    commit = commit,
    push = push
  )

  # step 3: record schema version in database
  message("\n3. Recording schema version in database...")
  record_schema_version(
    con = con,
    schema = schema,
    version = version,
    description = description,
    script_permalink = git_result$permalink
  )

  message(glue::glue("\n✅ Version release workflow completed for {version}!"))

  list(
    version = version,
    package_update = pkg_update,
    git_result = git_result,
    schema = schema,
    completed = TRUE
  )
}

#' Get Current Package Version
#'
#' Reads the current version from DESCRIPTION file.
#'
#' @return Character string with current version
#' @export
#' @concept version_sync
#'
#' @examples
#' \dontrun{
#' current_version <- get_package_version()
#' print(current_version)
#' }
get_package_version <- function() {
  desc_path <- here::here("DESCRIPTION")

  if (!file.exists(desc_path)) {
    stop(glue::glue("DESCRIPTION file not found: {desc_path}"))
  }

  desc_lines <- readLines(desc_path)
  version_line <- grep("^Version:", desc_lines, value = TRUE)

  if (length(version_line) == 0) {
    stop("Version field not found in DESCRIPTION")
  }

  version <- sub("^Version:\\s*", "", version_line)
  return(version)
}

#' Suggest Next Version
#'
#' Suggests the next version based on current version and type of change.
#'
#' @param change_type Type of change: "major", "minor", or "patch" (default: "patch")
#'
#' @return Character string with suggested next version
#' @export
#' @concept version_sync
#'
#' @examples
#' \dontrun{
#' suggest_next_version("minor")  # If current is 1.0.0, suggests 1.1.0
#' suggest_next_version("major")  # If current is 1.0.0, suggests 2.0.0
#' }
suggest_next_version <- function(change_type = "patch") {
  current <- get_package_version()
  parts <- as.integer(strsplit(current, "\\.")[[1]])

  if (length(parts) != 3) {
    stop("Current version is not in semantic versioning format")
  }

  major <- parts[1]
  minor <- parts[2]
  patch <- parts[3]

  next_version <- switch(
    change_type,
    major = glue::glue("{major + 1}.0.0"),
    minor = glue::glue("{major}.{minor + 1}.0"),
    patch = glue::glue("{major}.{minor}.{patch + 1}"),
    stop("change_type must be 'major', 'minor', or 'patch'")
  )

  return(as.character(next_version))
}
