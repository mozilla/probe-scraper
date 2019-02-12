# probe-scraper
Scrape Telemetry probe data from Firefox repositories.

This extracts per-version Telemetry probe data for Firefox and mobile products from registry files like Histograms.json and Scalars.yaml.
The data allows answering questions like "which Firefox versions is this Telemetry probe in anyway?".
Also, probes outside of Histograms.json - like the CSS use counters - are included in the output data.

The data is pulled from two different sources:
- From [`hg.mozilla.org`](https://hg.mozilla.org) for Firefox data.
- From a [configurable set of Github repositories](repositories.yaml).

A web tool to explore the data is available [here](https://telemetry.mozilla.org/probe-dictionary/).

## Adding a New Git Repository

To scrape a git repository for probe definitions, an entry needs to be added in `repositories.yaml`.
Currently all repositories are assumed to be for mobile-metrics. The `app_name` and `os` should
match the corresponding fields in the [mobile metrics ping](https://github.com/mozilla-services/mozilla-pipeline-schemas/blob/dev/schemas/telemetry/mobile-metrics/mobile-metrics.1.schema.json).

- `notification_emails`: Where emails about probe-scraper failures and improper files will be forwarded to. These
will be just about your specific repository.
- `url`: The URL of the repository to scrape. It should be able to be cloned directly from that URL.
- `histogram_file_paths`: A list of relative paths to `Histograms.json` files
- `scalar_file_paths`: A list of relative paths to `Scalars.yaml` files

Future work:
- `Events.yaml` support
- `Histograms.yaml` support
- Support for repos containing addon Scalar and Event definitions

## Developing the probe-scraper
Install the requirements:
```
pip install -r requirements.txt
pip install -r test_requirements.txt
python setup.py develop
```

Run tests:
```
pytest
```

To test whether the code conforms to the style rules, you can run:
```
flake8
```

### Performing a Dry-Run

Before opening a PR, it's good to test the code you wrote on the production data.

1. Change `MIN_FIREFOX_VERSION` in `scrapers/moz_central_scraper.py` to something larger, e.g. `60`.
   This will facilitate a faster scraping (so it's not searching through all of the historical commits).
2. Run `python probe_scraper/runner.py`
3. Check the output of the various files listed, and that the changes you expected to happen, did

## Module overview

The module is built around the following data flow:

- scrape registry files from mozilla-central, clone files from repositories directory
- extract probe data from the files
- transform probe data into output formats
- save to disk

The code layout consists mainly of:

- `probe_scraper`
  - `runner.py` - the central script, ties the other pieces together
  - `scrapers`
     - `moz_central_scraper.py` - loads probe registry files for multiple versions from mozilla-central
     - `git_scraper.py` - loads probe registry files from a git repository (no version or channel support yet, just per-commit)
  - `parsers/` - extract probe data from the registry files
     - `third_party` - these are imported parser scripts from [mozilla-central](https://dxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/)
   - `transform_*.py` - transform the extracted raw data into output formats
- `tests/` - the unit tests

## File formats
This scraper generates three different JSON file types.

### `revisions`
This file contains the revision hashes of the changesets the probe files were scraped. These hashes are mapped to an human-readable version string.

```
{
  "<channel>": {
    "<revision hash>": {
      "version": "<human-readable version string>"
    },
    ...
  },
  ...
  "aurora": {
    "1196bf3032e1bce1fb07a01fd9082a767426c5fb": {
      "version": "51"
    },
  },
  ...
}
```

### `general`
This file contains general properties related to the scraping process. As of today, it only contains the `lastUpdate` property, which is the day and time the scraping was performed, in ISO 8601 format.

```
{
  "lastUpdate": "2018-01-15T17:57:08.944690+01:00"
}
```

### Probe data file
This file contains the data for the probes. The data might be spread across multiple files. It has the following format:

```
{
  "<probe type>/<probe name>": {
    "history": {
      "<channel>": [
        {
          "cpp_guard": <string or null>,
          "description": "<string>",
          "details": {
            "<type specific detail>": "<detail data>",
            ...
            "record_in_processes": [
              "<string>",
              ...
            ]
          },
          "expiry_version": "<string>",
          "optout": <bool>,
          "revisions": {
            "first": "<string>",
            "last": "<string>"
          },
          "versions": {
            "first": "<string>",
            "last": "<string>"
          }
        },
        ...
      ]
    },
    "name": "<probe name>",
    "type": "<probe type>"
  },
  ...
  "histogram/A11Y_CONSUMERS": {
    "history": {
      "nightly": [
        {
          "cpp_guard": null,
          "description": "A list of known accessibility clients that inject into Firefox process space (see https://dxr.mozilla.org/mozilla-central/source/accessible/windows/msaa/Compatibility.h).",
          "details": {
            "high": 11,
            "keyed": false,
            "kind": "enumerated",
            "low": 1,
            "n_buckets": 12
          },
          "expiry_version": "never",
          "optout": true,
          "revisions": {
            "first": "320642944e42a889db13c6c55b404e32319d4de6",
            "last": "6f5fac320fcb6625603fa8a744ffa8523f8b3d71"
          },
          "versions": {
            "first": "56",
            "last": "59"
          }
        }
      ]
    },
    "name": "A11Y_CONSUMERS",
    "type": "histogram"
  },
}
```

Please refer to the Telemetry data collection [documentation](https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/collection/index.html) for a detailed explaination of the field information reported for each probe (e.g. `cpp_guard`).

## Git Repository Probe Data Files
The format is similar for probe data files, but without the `revisions` and `versions` keys. Instead it has a `git-commits` key, which contains the
first and last commits that definition has been seen in.

```
{
  "<probe type>/<probe name>": {
    "history": {
      "<repository-name>": [
        {
          "cpp_guard": <string or null>,
          "description": "<string>",
          "details": {
            "<type specific detail>": "<detail data>",
            ...
            "record_in_processes": [
              "<string>",
              ...
            ]
          },
          "expiry_version": "<string>",
          "optout": <bool>,
          "git-commits": {
            "first": "<commit-hash>",
            "last": "<commit-hash>"
          },
        },
        ...
      ]
    },
    "name": "<probe name>",
    "type": "<probe type>"
  },
  ...
  "histogram/EXAMPLE_EXPONENTIAL_HISTOGRAM": {
    "history": {
      "mobile_metrics_example": [
        {
          "git-commits": {
            "first": "71f9c017fb75c46e4f5167a92d549f12dc088f1c", 
            "last": "ebb0e4637ebb1c665d384c094ee71c79656b1acd"
          }, 
          "dates": {
            "first": "20180504", 
            "last": "20180510"
          }, 
          "description": "An example exponential histogram, sent on prerelease and release, recorded in the engine process", 
          "details": {
            "high": 1000, 
            "keyed": false, 
            "kind": "exponential", 
            "low": 1, 
            "n_buckets": 50
          }, 
          "expiry_version": "2", 
          "optout": false
        }
      ]
    }, 
    "name": "EXAMPLE_EXPONENTIAL_HISTOGRAM", 
    "type": "histogram"
  },
  ...
}
```

## Accessing the data files
The processed probe data is serialized to the disk in a directory hierarchy starting from the provided output directory. The directory layout resembles a REST-friendly structure.

    |-- product
        |-- general
        |-- revisions
        |-- channel (or "all")
            |-- ping type
                |-- probe type (or "all_probes")

For example, all the JSON probe data in the [main ping]() for the *Firefox Nightly* channel can be accessed with the followign path: `firefox/nightly/main/all_probes`. The probe data for all the channels (same product and ping) can be accessed instead using `firefox/all/main/all_probes`.

The root directory for the output generated from the scheduled job can be found at: https://probeinfo.telemetry.mozilla.org/ . All the probe data for Firefox coming from the main ping can be found [here](https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes).
