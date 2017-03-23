# probe-scraper
Scrape Telemetry probe data from Firefox repositories.

This extracts per-version Telemetry probe data for Firefox from registry files like Histograms.json and Scalars.yaml.
The data allows answering questions like "which Firefox versions is this Telemetry probe in anyway?".
Also, probes outside of Histograms.json - like the CSS use counters - are included in the output data.

A prototype web viewer is available [here](http://georgf.github.io/fx-data-explorer/index.html).

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

## Output location
The output from the scheduled job can be found at:

The output from the scheduled job can be found at:

* https://analysis-output.telemetry.mozilla.org/probe-scraper/data/general.json
* https://analysis-output.telemetry.mozilla.org/probe-scraper/data/revisions.json
* https://analysis-output.telemetry.mozilla.org/probe-scraper/data/probes.json

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

