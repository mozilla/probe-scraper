#!/usr/bin/env python

from setuptools import setup

setup(
    name="probe-scraper",
    version="0.1",
    description="Scrape metric data from Mozilla products repositories.",
    author="Mozilla",
    # While this is not owned by the Glean team, I could not find a better
    # email address for this.
    author_email="glean-team@mozilla.com",
    classifiers=[
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    url="https://github.com/mozilla/probe-scraper/",
    packages=["probe_scraper","awscli","beautifulsoup4","GitPython","boto3","Flask","glean_parser","google-cloud-storage","gsutil","Jinja2","jsonschema","python-dateutil","PyYAML","requests","requests_cache","requests_file","schema","urllib3","Werkzeug","yamllint"],
    license="MPL 2.0",
)
