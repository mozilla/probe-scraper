# probe-scraper
Scrape Telemetry probe data from Firefox repositories.

This extracts per-version Telemetry probe data for Firefox and mobile products from registry files like Histograms.json and Scalars.yaml.
The data allows answering questions like "which Firefox versions is this Telemetry probe in anyway?".
Also, probes outside of Histograms.json - like the CSS use counters - are included in the output data.

The data is pulled from two different sources:
- From [`hg.mozilla.org`](https://hg.mozilla.org) for Firefox data.
- From a [configurable set of Github repositories](repositories.yaml) that use [Glean](https://github.com/mozilla-mobile/android-components/tree/master/components/service/glean).

A web tool to explore the data is available [here](https://telemetry.mozilla.org/probe-dictionary/).

## Adding a New Glean Repository

To scrape a git repository for probe definitions, an entry needs to be added in `repositories.yaml`.
Each application repository should be listed under `application_families`.
The following fields are supported:

- `app_name` is a lowercase_with_underscores name that is short and unambiguous. It needs to be something that can be used both in a generated table name in BigQuery or as a value in an `app_name` string field in a derived table. BigQuery users might be using this literal name in a `WHERE` clause, so it should feel reasonable for that use case. Examples: `fenix`, `firefox_ios`
- `canonical_app_name` is how we would formally name this application in marketing copy or in a dashboard. This should not be materialized in ETL, but rather only made available in user-facing views since it may be verbose and it may change over time. Examples: Firefox for Android, Firefox for Android (Legacy Fennec), Firefox for Desktop, Firefox Focus for iOS
- `description`: A brief description of the repository.
- `deprecated` (optional) `true` if repository corresponds to a deprecated product or library.
- `prototype`: (optional) `true` if the application is still under developement.
- `notification_emails`: Where emails about probe-scraper failures and improper files will be forwarded to. These
will be just about your specific repository.
- `url`: The URL of the repository to scrape. It should be able to be cloned directly from that URL.
- `branch` (optional): The branch in the repository to use. (Defaults to `master`).
- `metrics_files`: A list of relative paths to `metrics.yaml` files
- `ping_files`: A list of relative paths to `pings.yaml` files

There is also an `apps` list to define the concrete applications in the family.
In most cases, there will be only one entry under `apps`, but more complex situations
like Fenix have separate apps for different logical release channels.
Each application can override specific fields like `description` from the family, but
additionally must provide the following:

- `app_id` is the app’s identifier exactly as it appears in the relevant app store listing (for relevant platforms) or in the app’s Glean initialization call (for other platforms). In the case of the Google Play store, it may contain both dots and dashes. For applicable platforms, you should be able to construct an app store URL from this value.
- `v1_name` is a unique identifier used to look up the app in the V1 probe scraper API;
  it appears as `name` in the `/glean/repositories` endpoint.
  The v2 API will use `document_namespace` for this purpose instead, so `v1_name` may
  eventually be removed.
- `app_channel`: (optional) The release channel. Must be one of "release", "beta", "nightly" or "esr".
  If this is not specified, then we assume the application provides a reasonable channel value in
  `client_info.app_channel` in its pings.

Note that the output in the `/v2/glean/applications` endpoint includes some
additional derived fields that aren't defined directly in the yaml:

- `document_namespace` is derived from `app_id`, normalized to contain only dashes as punctuation, and it is what Glean inserts into URLs when submitting telemetry.
- `bq_dataset_family` is derived from app_id, normalized to contain only underscores as punctuation, and it is the value that appears in the names of BigQuery datasets associated with the given application.

### Adding an application

For a given application, Glean metrics are emitted by the application itself, any libraries it uses
that also use Glean, as well as the Glean library proper. Therefore, probe scraper needs a way to
find all of the dependencies to determine all of the metrics emitted by
that application.

Therefore, each application should specify a `dependencies` parameter, which is a list of Glean-using libraries used by the application.  Each entry should be a library name as specified by the library's `library_names` parameter.

For Android applications, if you're not sure what the dependencies of the application are, you can run the following command at the root of the project folder:

```bash
$ ./gradlew :app:dependencies
```

### Adding a library

All **libraries** are defined under the `libraries` top-level key.
They support a subset of the same fields as applications, and additionally
must define `library_names`, which allows the probe scraper to map dependencies
back to an entry in the `repositories.yaml` file.
We expect `library_names` to be build-system-specific and thus a single
repository could present multiple library names for different platforms.

## Developing the probe-scraper

You can choose to develop using the container, or locally. Using the container will be slower, since changes will trigger a rebuild of the container.
But using the container method will ensure that your PR passes CircleCI build/test phases.

### Local development

Install the requirements:
```
pip install -r requirements.txt
pip install -r test_requirements.txt
python setup.py develop
```

Run tests. This by default does not run tests that require a web connection:
```
pytest tests/
```

To run all tests, including those that require a web connection:
```
pytest tests/ --run-web-tests
```

To test whether the code conforms to the style rules, you can run:
```
flake8 --max-line-length 100 .
```

### Developing using the container

Run tests in container. This does not run tests that require a web connection:
```
export COMMAND='pytest tests/'
make run
```

To run all tests, including those that require a web connection:
```
make test
```

To test whether the code conforms to the style rules, you can run:
```
make lint
```

### Tests with Web Dependencies

Any tests that require a web connection to run should be marked with `@pytest.mark.web_dependency`.

These will not run by default, but will run on CI.

### Performing a Dry-Run

Before opening a PR, it's good to test the code you wrote on the production data. You can specify a specific Firefox
version to run on by using `first-version`:
```
export COMMAND='python -m probe_scraper.runner --firefox-version 65 --dry-run'
make run
```
or locally via:
```
python -m probe_scraper.runner --firefox-version 65 --dry-run
```

Additionally, you can test just on Glean repositories:
```
export COMMAND='python -m probe_scraper.runner --glean --dry-run'
make run
```
or locally via:
```
python -m probe_scraper.runner --glean --dry-run
```

Including `--dry-run` means emails will not be sent.

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
     - `buildhub.py` - pull build info from the [BuildHub](https://buildhub.moz.tools) service
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

Please refer to the Telemetry data collection [documentation](https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/telemetry/collection/index.html) for a detailed explanation of the field information reported for each probe (e.g. `cpp_guard`).

## Glean Metrics Data Files
The format is similar for probe data files, but without the `revisions` and `versions` keys. Instead it has `git-commits` and `dates` keys, which contains the
first and last commits that definition has been seen in, and when those commits were committed.

```
{
  "<metric name>": {
    "history": [
      {
        "type": "timespan",
        "description": "  The duration of the last foreground session.",
        "time_unit": "second",
        "send_in_pings": ["baseline"],
        "bugs": [1497894, 1519120],
        "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
        "notification_emails": ["telemetry-client-dev@mozilla.com"],
        "git-commits": {
          "first": "<commit-hash>",
          "last": "<commit-hash>"
        },
        "dates": {
          "first": "2019-01-01 12:12:12",
          "last": "2019-02-01 14:14:14"
        },
      },
      ...
    ]
    "name": "<metric name>",
    "type": "<metric type>"
  },
  ...
}
```

### Glean dependencies files

The Glean dependency file contains information about the dependencies of an
application in `repositories.yaml`.

The format is similar for Glean metrics data files. Each entry in the top-level
object represents a dependency of the application. The only data point tracked
in the `history` log is `version`, which is the version number of the
dependency. At the top-level of each entry is:

- `name`: the name of the dependency (for Android, this is a Maven
  package name).
- `type`: Always `"dependency"`.

```
{
  "<library name>": {
    "history": [
      {
        "dates": {
          "first": "2019-05-25 00:39:19",
          "last": "2019-05-28 10:24:06"
        },
        "git-commits": {
          "first": "9aa4f48e77001058c05f3d3182228706720bf87a",
          "last": "69c485078950fb09ee2cef609b75ea9dd30d249b"
        },
        "type": "dependency",
        "version": "1.1.0-alpha05"
      }
    ],
    "name": "<library-name>",
    "type": "dependency"
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

For example, all the JSON probe data in the [main ping]() for the *Firefox Nightly* channel can be accessed with the following path: `firefox/nightly/main/all_probes`. The probe data for all the channels (same product and ping) can be accessed instead using `firefox/all/main/all_probes`.

The root directory for the output generated from the scheduled job can be found at: https://probeinfo.telemetry.mozilla.org/ . All the probe data for Firefox coming from the main ping can be found [here](https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes).

## Accessing `Glean` metrics data
Glean data is generally laid out as follows:

```
| -- glean
    | -- repositories
    | -- general
    | -- repository-name
        | -- general
        | -- metrics
```

For example, the data for a repository called `fenix` would be found at [`/glean/fenix/metrics`](https://probeinfo.telemetry.mozilla.org/glean/fenix/metrics). The time the data was last updated for that project can be found at [`glean/fenix/general`](https://probeinfo.telemetry.mozilla.org/glean/fenix/general).

A list of available repositories is at [`/glean/repositories`](https://probeinfo.telemetry.mozilla.org/glean/repositories).

## Using the v2 Glean API

We are incrementally building a v2 API for Glean applications that will
provide views of underlying probe scraper data that are more directly
usable by consuming applications. The general direction is discussed in
the [Nested structure for Glean repositories.yaml proposal](https://docs.google.com/document/d/1_zx0cxBnnFOhct1cCb_QnXntV-HN9sjqfGfiBwEBL-8/edit?ts=5fca0606#heading=h.uk893w7s4i3o).

The first available endpoint is `/v2/glean/applications` which is similar to the
v1 `/glean/repositories` endpoint, but filters out libraries and provides
additional fields like `canonical_app_name` and derived fields like `bq_dataset_family`
that are intended to provide better usability for future downstream ETL
and documentation use cases.
