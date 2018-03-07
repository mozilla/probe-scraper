# probe-scraper
Scrape Telemetry probe data from Firefox repositories.

This extracts per-version Telemetry probe data for Firefox from registry files like Histograms.json and Scalars.yaml.
The data allows answering questions like "which Firefox versions is this Telemetry probe in anyway?".
Also, probes outside of Histograms.json - like the CSS use counters - are included in the output data.

A web tool to explore the data is available [here](https://telemetry.mozilla.org/probe-dictionary/).

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

## Module overview

The module is built around the following data flow:

- scrape registry files from mozilla-central
- extract probe data from the files
- transform probe data into output formats
- save to disk

The code layout consists mainly of:

- `probe_scraper`
  - `runner.py` - the central script, ties the other pieces together
  - `scraper.py` - loads probe registry files for multiple versions from mozilla-central
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

## Accessing the data files
The processed probe data is serialized to the disk in a directory hierarchy starting from the provided output directory. The directory layout resembles a REST-friendly structure.

    |-- product
        |-- general
        |-- revisions
        |-- channel (or "all")
            |-- ping type
                |-- probe type (or "all_probes")

For example, all the JSON probe data in the [main ping]() for the *Firefox Nightly* channel can be accessed with the followign path: `firefox/nightly/main/all_probes`. The probe data for all the channels (same product and ping) can be accessed instead using `firefox/all/main/all_probes`.

The root directory for the output generated from the scheduled job can be found at: https://analysis-output.telemetry.mozilla.org/probe-scraper/data-rest/ . All the probe data for Firefox coming from the main ping can be found [here](https://analysis-output.telemetry.mozilla.org/probe-scraper/data-rest/firefox/all/main/all_probes).
