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
EMAIL_DAYS_BEFORE = 14
PROBE_INFO_BASE_URL = "https://probeinfo.telemetry.mozilla.org/"

EMAIL_BODY_FORMAT_STRING = """
The following Firefox probes have either expired or will expire in the next major Firefox release: version {release} on the release channel, {beta} on the beta channel, and {nightly} on the nightly channel.

{per_channel_probes}

What to do about this:
1. If one, some, or all of the metrics are no longer needed, please remove them from their definitions files (Histograms.json, Scalars.yaml, Events.yaml).
2. If one, some, or all of the metrics are still required, please submit a Data Collection Review and patch to extend their expiry.

If you have any problems, please ask for help on the #fx-metrics Slack channel. We'll give you a hand.

Your Friendly, Neighborhood Telemetry Team

[1] https://wiki.mozilla.org/Firefox/Data_Collection

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa

PER_CHANNEL_FORMAT_STRING = """
The following probes {verb} on the {channel} channel in Firefox version {version}:
{probes}
"""


def get_latest_firefox_versions():
    versions = requests.get("https://product-details.mozilla.org/1.0/firefox_versions.json").json()

    return {
        "release": get_major_version(versions['LATEST_FIREFOX_VERSION']),
        "beta": get_major_version(versions['LATEST_FIREFOX_RELEASED_DEVEL_VERSION']),
        "nightly": get_major_version(versions["FIREFOX_NIGHTLY"]),
    }


def find_expiring_probes(probes, target_versions):
    """
    Find probes expiring in the next release or expired in the last release
    using output of the probe info service

    Returns dict of form:
    {
      channel: {
        histogram_name: [notification_emails...]
      }
    }
    """
    expiring_histograms_by_channel = defaultdict(dict)
    for probe in probes.values():
        for channel, details in probe["history"].items():
            details = details[0]
            expiry_version = details["expiry_version"]
            if expiry_version == "never":
                continue

            if expiry_version == target_versions[channel]:
                expiring_histograms_by_channel[channel][probe["name"]] = (
                        details.get("notification_emails", []) + [DEFAULT_TO_EMAIL])

    return expiring_histograms_by_channel


def send_emails_for_expiring_probes(expired_probes_by_channel, expiring_probes_by_channel,
                                    versions, dryrun=True):
    probes_by_email_by_state_by_channel = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))

    # Get expired probes for each email
    for channel, probes in expired_probes_by_channel.items():
        for name, emails in probes.items():
            for email in emails:
                probes_by_email_by_state_by_channel[email]["expired"][channel].append(name)

    # Get expiring probes for each email
    for channel, probes in expiring_probes_by_channel.items():
        for name, emails in probes.items():
            for email in emails:
                probes_by_email_by_state_by_channel[email]["expiring"][channel].append(name)

    for email in probes_by_email_by_state_by_channel.keys():
        per_channel_format_strings = [
            PER_CHANNEL_FORMAT_STRING.format(
                channel=channel, verb='are expiring',
                version=int(versions[channel]) + 1, probes="\n".join(probe_names))
            for channel, probe_names
            in probes_by_email_by_state_by_channel[email]["expiring"].items()
        ] + [
            PER_CHANNEL_FORMAT_STRING.format(
                channel=channel, verb='have expired',
                version=versions[channel], probes="\n".join(probe_names))
            for channel, probe_names
            in probes_by_email_by_state_by_channel[email]["expired"].items()
        ]

        email_body = EMAIL_BODY_FORMAT_STRING.format(
            release=versions['release'],
            beta=versions['beta'],
            nightly=versions['nightly'],
            per_channel_probes="\n".join(per_channel_format_strings)
        )

        emailer.send_ses(FROM_EMAIL, "Telemetry Probe Expiry",
                         body=email_body, recipients=email, dryrun=dryrun)

    logging.info(f"Sent emails to {len(probes_by_email_by_state_by_channel)} recipients")


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

    current_versions = get_latest_firefox_versions()
    next_versions = {
        channel: str(int(version) + 1) for channel, version in current_versions.items()}

    logging.info(f"Retrieved current versions: {current_versions}")

    expired_probes = find_expiring_probes(probe_info, current_versions)
    expiring_probes = find_expiring_probes(probe_info, next_versions)

    for channel, probes in expired_probes.items():
        print(f"Found {len(probes)} expired probes in {channel}")
    for channel, probes in expiring_probes.items():
        print(f"Found {len(probes)} expiring probes in {channel}")

    # Only send emails on Tuesdays, run the rest for debugging/error detection
    if current_date.weekday() != 1:
        logging.info("Skipping emails because it is not Tuesday")
        return

    send_emails_for_expiring_probes(expired_probes, expiring_probes, next_versions, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(datetime.date.today(), args.dry_run)
