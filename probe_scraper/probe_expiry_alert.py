# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import argparse
import datetime
import logging

import requests

from probe_scraper import emailer
from probe_scraper.parsers.utils import get_major_version

FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "dev-telemetry-alerts@mozilla.com"
PROBE_INFO_BASE_URL = "https://probeinfo.telemetry.mozilla.org/"

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
    Find probes expiring in the target version using output of the probe info service

    Returns dict of form:
    {
        histogram_name: [notification_emails...]
    }
    """
    expiring_probes = defaultdict(dict)
    for probe in probes.values():
        details = probe["history"].get("nightly")
        if details is None or len(details) == 0:
            continue
        details = details[0]
        expiry_version = details["expiry_version"]
        if expiry_version == "never":
            continue

        if expiry_version == target_version:
            expiring_probes[probe["name"]] = (
                    details.get("notification_emails", []) + [DEFAULT_TO_EMAIL])

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
                    verb='are expiring', version=int(current_version) + 1,
                    probes="\n".join(probes_by_email_by_state[email]["expiring"])))
        if len(probes_by_email_by_state[email]["expired"]) > 0:
            probe_list_format_strings.append(
                PROBE_LIST_FORMAT_STRING.format(
                    verb='have expired', version=current_version,
                    probes="\n".join(probes_by_email_by_state[email]["expired"])))

        email_body = EMAIL_BODY_FORMAT_STRING.format(
            next_version=int(current_version) + 1,
            probes="\n".join(probe_list_format_strings))

        emailer.send_ses(FROM_EMAIL, "Telemetry Probe Expiry",
                         body=email_body, recipients=email, dryrun=dryrun)

    logging.info(f"Sent emails to {len(probes_by_email_by_state)} recipients")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        help="Whether emails should be sent",
        action="store_true",
    )
    return parser.parse_args()


def main(current_date, dryrun):
    probe_info = requests.get(PROBE_INFO_BASE_URL + "firefox/all/main/all_probes").json()

    current_version = get_latest_nightly_version()
    next_version = str(int(current_version) + 1)

    expired_probes = find_expiring_probes(probe_info, current_version)
    expiring_probes = find_expiring_probes(probe_info, next_version)

    logging.info(f"Found {len(expired_probes)} expired probes in nightly {current_version}")
    logging.info(f"Found {len(expiring_probes)} expiring probes in nightly {next_version}")

    # Only send emails on Tuesdays, run the rest for debugging/error detection
    if current_date.weekday() != 1:
        logging.info("Skipping emails because it is not Tuesday")
        return

    send_emails_for_expiring_probes(expired_probes, expiring_probes, current_version, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(datetime.date.today(), args.dry_run)
