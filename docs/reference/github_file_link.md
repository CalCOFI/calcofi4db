# Create GitHub File Link

Converts a local file path to a GitHub repository link. Extracts the
relative path from the workflows directory and creates an HTML anchor
tag.

## Usage

``` r
github_file_link(
  file_path,
  repo = "CalCOFI/workflows",
  branch = "main",
  base_dir = "workflows"
)
```

## Arguments

- file_path:

  Local file path (can be full path or relative)

- repo:

  GitHub repository in format "owner/repo" (default:
  "CalCOFI/workflows")

- branch:

  Git branch (default: "main")

- base_dir:

  Base directory to extract relative path from (default: "workflows")

## Value

HTML anchor tag string linking to the GitHub file

## Examples

``` r
if (FALSE) { # \dontrun{
github_file_link("/Users/bbest/Github/CalCOFI/workflows/data/flagged/orphan_species.csv")
# Returns: "<a href='https://github.com/CalCOFI/workflows/blob/main/data/flagged/orphan_species.csv'>
#           calcofi/workflows: data/flagged/orphan_species.csv</a>"
} # }
```
