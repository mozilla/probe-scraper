# probe-scraper
Scrape various probe data from Firefox repositories.

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

https://analysis-output.telemetry.mozilla.org/probe-scraper/data/*.json
