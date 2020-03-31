# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import argparse
import datetime
import hashlib
import logging
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

BUGZILLA_URL = "https://bugzilla-dev.allizom.org/rest/bug"

BASE_URI = "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/"
HISTOGRAMS_FILE = "Histograms.json"
SCALARS_FILE = "Scalars.yaml"
EVENTS_FILE = "Events.yaml"

BUG_SUMMARY_TEMPLATE = "Remove or update probe expiring in Firefox {version}: {probe}"
BUB_RESOLVED_COMMENT_TEMPLATE = "This probe is no longer expiring in Firefox {version}."

EMAIL_BODY_FORMAT_STRING = """
The following Firefox probes have either expired or will expire in the next major Firefox nightly release: version {next_version} [1].

{probes}

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

        email_body = EMAIL_BODY_FORMAT_STRING.format(
            next_version=int(current_version) + 1,
            probes="\n".join(probe_list_format_strings))

        emailer.send_ses(FROM_EMAIL, "Telemetry Probe Expiry",
                         body=email_body, recipients=email, dryrun=dryrun)

    logging.info(f"Sent emails to {len(probes_by_email_by_state)} recipients")


def file_bugs(probes, version, create_bugs=False, dryrun=True):
    """
    Search for bugs that have already been created by probe_expiry_alerts
    for probes in the current version.
    For each probe/bug:
        - if a bug exists for a probe in the given list, do nothing
        - if a bug exists for a probe that is not in the given list resolve the bug
        - if no bug exists for a  given probe, create a bug
    """
    whiteboard_tag = 'probe-expiry-alert'

    # search for previously created bugs
    search_query_params = {
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=''),
        "whiteboard": whiteboard_tag,
        "status": ["NEW", "ASSIGNED"],
    }
    found_bugs = requests.get(BUGZILLA_URL, params=search_query_params).json()

    # Close bugs for removed/updated probes
    for bug in found_bugs["bugs"]:
        probe_name = bug["summary"].substring(" ")[-1]
        if probe_name not in probes.keys():
            update_params = {
                "status": "RESOLVED",
                "resolution": "FIXED",
                "comment": BUG_SUMMARY_TEMPLATE.format(version=version)
            }
            print(f"PUT: {update_params}")  # TODO
            requests.put(BUGZILLA_URL + "/" + bug["id"], data=update_params)
        probes.pop(probe_name)

    # Create bugs new expiring probes
    if create_bugs:
        for probe_name, emails in probes.items():
            def fudge_email(email):
                e = email.split("@")
                e[0] = hashlib.sha1(e[0].encode()).hexdigest()[:8]
                return "@".join(e)

            create_params = {
                "product": "Toolkit",
                "component": "Telemetry",
                "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=probe_name),
                "version": "unspecified",
                "type": "task",
                "whiteboard": whiteboard_tag,
                "flags": [
                    {
                        "name": "needinfo",
                        "type_id": 800,
                        "status": "?",
                        "requestee": fudge_email(email),
                    }
                    for email in emails
                ],
            }
            print(f"POST: {create_params}")  # TODO
            requests.post(BUGZILLA_URL, data=create_params)


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
    return parser.parse_args()


def main(current_date, dryrun):
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

    logging.info(f"Found {len(expired_probes)} expired probes in nightly {current_version}")
    logging.info(f"Found {len(expiring_probes)} expiring probes in nightly {next_version}")

    # Only send emails on Wednesdays, run the rest for debugging/error detection
    if not dryrun and current_date.weekday() != 2:
        logging.info("Skipping emails because it is not Wednesday")
        #return

    file_bugs(expiring_probes, next_version, create_bugs=True, dryrun=dryrun)

    #send_emails_for_expiring_probes(expired_probes, expiring_probes, current_version, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(datetime.date.today(), args.dry_run)
