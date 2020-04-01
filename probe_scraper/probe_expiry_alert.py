# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import argparse
import datetime
import os
import tempfile

import requests

from probe_scraper import emailer
from probe_scraper.parsers.events import EventsParser
from probe_scraper.parsers.histograms import HistogramsParser
from probe_scraper.parsers.scalars import ScalarsParser
from probe_scraper.parsers.utils import get_major_version

FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "dev-telemetry-alerts@lists.mozilla.org"

BUGZILLA_BUG_URL = "https://bugzilla-dev.allizom.org/rest/bug"
BUGZILLA_USER_URL = "https://bugzilla-dev.allizom.org/rest/user"

BASE_URI = "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/"
HISTOGRAMS_FILE = "Histograms.json"
SCALARS_FILE = "Scalars.yaml"
EVENTS_FILE = "Events.yaml"

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

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
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


def send_emails_for_expiring_probes(expired_probes, expiring_probes,
                                    current_version, dryrun=True):
    probes_by_email_by_state = defaultdict(lambda: defaultdict(list))

    # Get expired probes for each email
    for name, emails in expired_probes.items():
        for email in emails:
            probes_by_email_by_state[email]["expired"].append(name)

    # Get expiring probes for each email
    for name, emails in expiring_probes.items():
        for email in emails:
            probes_by_email_by_state[email]["expiring"].append(name)

    for email in probes_by_email_by_state.keys():
        probe_list_format_strings = []
        if len(probes_by_email_by_state[email]["expiring"]) > 0:
            probe_list_format_strings.append(
                PROBE_LIST_FORMAT_STRING.format(
                    verb="are expiring", version=int(current_version) + 1,
                    probes="\n".join(probes_by_email_by_state[email]["expiring"])))
        if len(probes_by_email_by_state[email]["expired"]) > 0:
            probe_list_format_strings.append(
                PROBE_LIST_FORMAT_STRING.format(
                    verb="have expired", version=current_version,
                    probes="\n".join(probes_by_email_by_state[email]["expired"])))

        email_body = BUG_DESCRIPTION_TEMPLATE.format(
            next_version=int(current_version) + 1,
            probes="\n".join(probe_list_format_strings))

        emailer.send_ses(FROM_EMAIL, "Telemetry Probe Expiry",
                         body=email_body, recipients=email, dryrun=dryrun)

    print(f"Sent emails to {len(probes_by_email_by_state)} recipients")


def check_bugzilla_user_exists(email, request_headers):
    user_response = requests.get(BUGZILLA_USER_URL + "?match=" + email, headers=request_headers)
    user_response.raise_for_status()
    return len(user_response.json()["users"]) > 0


def file_bugs(probes, version, bugzilla_api_key, create_bugs=False, dryrun=True):
    """
    Search for bugs that have already been created by probe_expiry_alerts
    for probes in the current version.
    For each probe/bug:
        - if a bug exists for a probe in the given list, do nothing
        - if a bug exists for a probe that is not in the given list resolve the bug
        - if no bug exists for a  given probe, create a bug
    """
    request_header = {
        "X-BUGZILLA-API-KEY": bugzilla_api_key
    }

    whiteboard_tag = 'probe-expiry-alert'

    # search for previously created bugs
    search_query_params = {
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=''),
        "whiteboard": whiteboard_tag,
    }
    found_bugs = requests.get(BUGZILLA_BUG_URL, params=search_query_params, headers=request_header).json()

    # Close bugs for removed/updated probes and prevent duplicates from being created
    for bug in found_bugs["bugs"]:
        probe_name = bug["summary"].split(" ")[-1]
        if probe_name not in probes.keys() and bug["status"] != "RESOLVED":  # probe not expiring
            update_params = {
                "status": "RESOLVED",
                "resolution": "FIXED",
                "comment": {
                    "body": BUG_RESOLVED_COMMENT_TEMPLATE.format(version=version)
                }
            }
            print(f"PUT: {update_params}")  # TODO
            print("update: " + str(requests.put(BUGZILLA_BUG_URL + "/" + str(bug["id"]), json=update_params, headers=request_header).json()))
        elif probe_name in probes.keys():  # bug already exists
            probes.pop(probe_name)

    # Create bugs new expiring probes
    if create_bugs:
        for probe_name, emails in probes.items():
            valid_emails = list(filter(
                lambda email: check_bugzilla_user_exists(email, request_header), emails))

            create_params = {
                "product": "Toolkit",
                "component": "Telemetry",
                "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=probe_name),
                "description": BUG_DESCRIPTION_TEMPLATE.format(version=version, probe_name=probe_name),
                "version": "unspecified",
                "type": "task",
                "whiteboard": whiteboard_tag,
                "cc": valid_emails,
            }
            print(f"POST: {create_params}")  # TODO
            create_response = requests.post(BUGZILLA_BUG_URL, json=create_params, headers=request_header)
            print("create: " + str(create_response.json()))


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
    )
    return parser.parse_args()


def main(current_date, dryrun, bugzilla_api_key):
    current_version = get_latest_nightly_version()
    next_version = str(int(current_version) + 1)

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

    expired_probes = find_expiring_probes(all_probes, current_version)
    expiring_probes = find_expiring_probes(all_probes, next_version)

    print(f"Found {len(expired_probes)} expired probes in nightly {current_version}")
    print(f"Found {len(expiring_probes)} expiring probes in nightly {next_version}")

    # Only send emails on Wednesdays, run the rest for debugging/error detection
    if not dryrun and current_date.weekday() != 2:
        print("Skipping emails because it is not Wednesday")
        #return

    file_bugs(expiring_probes, next_version, bugzilla_api_key,
              create_bugs=True, dryrun=dryrun)

    #send_emails_for_expiring_probes(expired_probes, expiring_probes, current_version, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(datetime.date.today(), args.dry_run, args.bugzilla_api_key)
