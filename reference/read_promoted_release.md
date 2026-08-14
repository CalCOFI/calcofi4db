# Read the currently promoted release, authoritatively

`latest.txt` is the pointer every consumer resolves through, so reading
it over the public `https://storage.googleapis.com/...` URL is not safe
for a control-flow decision: that URL is CDN-cached, and the object
carried no `Cache-Control` until 2026-08-14, so it inherited the 1-hour
public default.

## Usage

``` r
read_promoted_release(bucket = "calcofi-db")
```

## Arguments

- bucket:

  GCS bucket holding `ducklake/releases/latest.txt`

## Value

the promoted version string (e.g. `"v2026.08.14"`), or `NA_character_`
if the object cannot be read

## Details

On 2026-08-14 that bit twice in one hour, and the second direction is
the dangerous one. A rollback to `v2026.08.11` took an hour to reach
consumers; then `release_database.qmd`'s republish guard — reading the
same cached URL — false-fired because it still saw the pre-rollback
value. The mirror image is worse: immediately after a promotion the
cache still shows the *previous* version, so a guard comparing against
it concludes `latest.txt` points somewhere else and permits a run to
overwrite the release consumers are actively reading, which is the exact
thing it exists to prevent. A one-hour blind window after every
promotion, failing open.

This reads the object through the authenticated API instead, which is
never cached.
