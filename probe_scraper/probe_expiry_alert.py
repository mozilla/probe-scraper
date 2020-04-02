# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import argparse
import datetime
import os
import tempfile

import requests

from probe_scraper.parsers.events import EventsParser
from probe_scraper.parsers.histograms import HistogramsParser
from probe_scraper.parsers.scalars import ScalarsParser
from probe_scraper.parsers.utils import get_major_version

BUGZILLA_BUG_URL = "https://bugzilla.mozilla.org/rest/bug"
BUGZILLA_USER_URL = "https://bugzilla.mozilla.org/rest/user"

BASE_URI = "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/"
HISTOGRAMS_FILE = "Histograms.json"
SCALARS_FILE = "Scalars.yaml"
EVENTS_FILE = "Events.yaml"

BUG_WHITEBOARD_TAG = "probe-expiry-alert"
BUG_SUMMARY_TEMPLATE = "Remove or update probe expiring in Firefox {version}: {probe}"
BUG_RESOLVED_COMMENT_TEMPLATE = "This probe is no longer expiring in Firefox {version}."

BUG_DESCRIPTION_TEMPLATE = """
The following Firefox probe will expire in the next major Firefox nightly release: version {version} [1].

`{probe_name}`

What to do about this:
1. If one, some, or all of the metrics are no longer needed, please remove them from their definitions files (Histograms.json, Scalars.yaml, Events.yaml).
2. If one, some, or all of the metrics are still required, please submit a Data Collection Review [2] and patch to extend their expiry.

If you have any problems, please ask for help on the #fx-metrics Slack channel. We'll give you a hand.

Your Friendly, Neighborhood Telemetry Team

[1] https://wiki.mozilla.org/Release_Management/Calendar
[2] https://wiki.mozilla.org/Firefox/Data_Collection

This is an automated bug created by probe-scraper.  See https://github.com/mozilla/probe-scraper for details.

{notes}
"""  # noqa

PROBE_LIST_FORMAT_STRING = """
The following probes {verb} in Firefox version {version}:
{probes}
"""


def get_latest_nightly_version():
    versions = requests.get("https://product-details.mozilla.org/1.0/firefox_versions.json").json()
    return get_major_version(versions["FIREFOX_NIGHTLY"])


def find_expiring_probes(probes, target_version):
    """
    Find probes expiring in the target version

    Returns dict of form:
    {
        histogram_name: [notification_emails...]
    }
    """
    expiring_probes = defaultdict(dict)
    for name, details in probes.items():
        expiry_version = details["expiry_version"]

        if expiry_version == target_version:
            expiring_probes[name] = (
                    details.get("notification_emails", []))

    return expiring_probes


def check_bugzilla_user_exists(email, request_headers):
    user_response = requests.get(BUGZILLA_USER_URL + "?match=" + email, headers=request_headers)
    user_response.raise_for_status()
    return len(user_response.json()["users"]) > 0


def search_bugs(version, request_header):
    search_query_params = {
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=""),
        "whiteboard": BUG_WHITEBOARD_TAG,
    }
    return requests.get(BUGZILLA_BUG_URL, params=search_query_params,
                        headers=request_header).json()["bugs"]


def create_bug(probe_name, version, emails, request_header):
    description_notes = ("No emails associated with the probe have a "
                         "corresponding Bugzilla account." if len(emails) == 0 else "")

    create_params = {
        "product": "Toolkit",
        "component": "Telemetry",
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=probe_name),
        "description": BUG_DESCRIPTION_TEMPLATE.format(
            version=version, probe_name=probe_name, notes=description_notes),
        "version": "unspecified",
        "type": "task",
        "whiteboard": BUG_WHITEBOARD_TAG,
        "flags": [
            {
                "name": "needinfo",
                "type_id": 800,
                "status": "?",
                "requestee": email,
            }
            for email in emails
        ],
    }
    create_response = requests.post(BUGZILLA_BUG_URL, json=create_params,
                                    headers=request_header)
    create_response.raise_for_status()
    print(f"Created bug {str(create_response.json())} for probe {probe_name}")


def file_bugs(probes, version, bugzilla_api_key, create_bugs=False):
    """
    Search for bugs that have already been created by probe_expiry_alerts
    for probes in the current version.
    For each probe/bug:
        - if a bug exists for a probe in the given list, do nothing
        - if no bug exists for a given probe, create a bug
    """
    request_header = {
        "X-BUGZILLA-API-KEY": bugzilla_api_key
    }

    found_bugs = search_bugs(version, request_header)
    print(f"Found {len(found_bugs)} previously created bugs for version {version}")

    for bug in found_bugs:
        probe_name = bug["summary"].split()[-1]
        if probe_name in probes.keys():
            probes.pop(probe_name)

    for probe_name, emails in probes.items():
        valid_emails = list(filter(
            lambda email: check_bugzilla_user_exists(email, request_header), emails))

        if create_bugs:
            create_bug(probe_name, version, valid_emails, request_header)


def download_file(url, output_filepath):
    content = requests.get(url).text
    with open(output_filepath, "w") as output_file:
        output_file.write(content)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        help="Whether emails should be sent",
        action="store_true",
    )
    parser.add_argument(
        "--bugzilla-api-key",
        type=str,
        required=True,
    )
    return parser.parse_args()


def main(current_date, dryrun, bugzilla_api_key):
    next_version = str(int(get_latest_nightly_version()) + 1)

    with tempfile.TemporaryDirectory() as tempdir:
        events_file_path = os.path.join(tempdir, EVENTS_FILE)
        download_file(BASE_URI + EVENTS_FILE, events_file_path)
        events = EventsParser().parse([events_file_path])

        histograms_file_path = os.path.join(tempdir, HISTOGRAMS_FILE)
        download_file(BASE_URI + HISTOGRAMS_FILE, histograms_file_path)
        histograms = HistogramsParser().parse([histograms_file_path], version=next_version)

        scalars_file_path = os.path.join(tempdir, SCALARS_FILE)
        download_file(BASE_URI + SCALARS_FILE, scalars_file_path)
        scalars = ScalarsParser().parse([scalars_file_path])

    all_probes = events.copy()
    all_probes.update(histograms)
    all_probes.update(scalars)

    expiring_probes = find_expiring_probes(all_probes, next_version)

    print(f"Found {len(expiring_probes)} probes expiring in nightly {next_version}")
    print(list(expiring_probes.keys()))

    # Only send create bugs on Wednesdays, run the rest for debugging/error detection
    create_bugs = not dryrun and current_date.weekday() == 2
    file_bugs(expiring_probes, next_version, bugzilla_api_key, create_bugs=create_bugs)


if __name__ == "__main__":
    args = parse_args()
    main(datetime.date.today(), args.dry_run, args.bugzilla_api_key)
