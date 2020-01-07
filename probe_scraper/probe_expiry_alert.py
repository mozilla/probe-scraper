# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import argparse
import datetime

import requests

from probe_scraper import emailer
from probe_scraper.scrapers import release_calendar

FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "dev-telemetry-alerts@mozilla.com"
EMAIL_TIME_BEFORE = datetime.timedelta(weeks=2)
PROBE_INFO_BASE_URL = "https://probeinfo.telemetry.mozilla.org/"


def find_expiring_probes(target_date, probes, release_dates):
    """
    Find probes expiring on the target date using output of the probe info service

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

            expiry_date = release_dates[channel].get(expiry_version)
            if expiry_date == target_date:
                expiring_histograms_by_channel[channel][probe["name"]] = (
                    details.get("notification_emails", []) + [DEFAULT_TO_EMAIL])

    return expiring_histograms_by_channel


def send_emails_for_expiring_probes(target_date, expiring_histograms_by_channel, dryrun=True):
    histograms_by_email = defaultdict(lambda: defaultdict(list))

    for channel, histograms in expiring_histograms_by_channel.items():
        for name, emails in histograms.items():
            for email in emails:
                histograms_by_email[email][channel].append(name)

    email_body_format_string = """
The following probes will be expiring on {} and should be removed from the codebase
or have their expiry versions updated:
{}

This is an automated message from probe-scraper (https://github.com/mozilla/probe-scraper).
    """
    per_channel_format_string = """
On {}:
{}
    """

    for email, histograms_by_channel in histograms_by_email.items():
        per_channel_format_strings = [
            per_channel_format_string.format(channel, "\n".join(histogram_names))
            for channel, histogram_names
            in histograms_by_channel.items()
        ]

        email_body = email_body_format_string.format(
            target_date,
            "\n".join(per_channel_format_strings)
        )

        emailer.send_ses(FROM_EMAIL, "Telemetry Probe Expiry",
                         body=email_body, recipients=email, dryrun=dryrun)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        help="Whether emails should be sent",
        action="store_true",
    )
    return parser.parse_args()


def main(dryrun):
    probe_info = requests.get(PROBE_INFO_BASE_URL + "firefox/all/main/all_probes").json()
    target_date = datetime.date.today() + EMAIL_TIME_BEFORE

    release_dates = release_calendar.get_release_dates()

    expiring_probes = find_expiring_probes(target_date, probe_info, release_dates)

    send_emails_for_expiring_probes(target_date, expiring_probes, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(args.dry_run)
