# Common failures of probe-scraper runs and how to solve them

`probe-scraper` runs every week day in pull mode for some repositories, such as `mozilla-central`.
It looks at all commits changing metric and ping definition files (`metrics.yaml`, `pings.yaml`).
This can fail for a variety of reasons.

## Backouts

Commits adding new metric or ping files get backed out, thus removing the file again.

### Solution

Add the offending commits to the the `SKIP_COMMITS` list of the product in [`probe_scraper/scrapers/git_scraper.py`][skipcommits].


[skipcommits]: https://github.com/mozilla/probe-scraper/blob/1d23fcf4d041ea7fdf3e2c0c79252151f472ad0b/probe_scraper/scrapers/git_scraper.py


## Invalid metric definitions files

A new commit changes a `metrics.yaml` file in a way that fails to parse.
That is fixed in a subsequent commit.

### Solution

Add the offending commit(s) to `SKIP_COMMITS` as above for [Backouts](#backouts).

## Invalid metric definitions files in the past

A `metrics.yaml` is already available in old commits in a project, but invalid.
At some point later the file is fixed and correct.

### Solution

Add a minimal date from which to start parsing the file in `MIN_DATES` in [`probe-scraper/probe_scraper/scrapers/git_scraper.py`][mindates].

[mindates]: https://github.com/mozilla/probe-scraper/blob/1d23fcf4d041ea7fdf3e2c0c79252151f472ad0b/probe_scraper/scrapers/git_scraper.py#L29
