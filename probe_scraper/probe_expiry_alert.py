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
EMAIL_DAYS_BEFORE = 14
PROBE_INFO_BASE_URL = "https://probeinfo.telemetry.mozilla.org/"

EMAIL_BODY_FORMAT_STRING = """
The following Firefox probes will expire in the next {expire_days} days or have already expired.

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
These probes are expiring on the {channel} channel:
{probes}
"""


def find_expiring_probes(probes, release_dates, base_date=datetime.date.today(), expire_days=14):
    """
    Find probes expiring in the next {expire_days} days using output of the probe info service

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

            expiry = release_dates[channel].get(expiry_version)
            if expiry is None:
                continue
            if base_date <= expiry <= base_date + datetime.timedelta(days=expire_days):
                expiring_histograms_by_channel[channel][probe["name"]] = (
                    details.get("notification_emails", []) + [DEFAULT_TO_EMAIL])

    return expiring_histograms_by_channel


def send_emails_for_expiring_probes(expiring_histograms_by_channel, expire_days=14, dryrun=True):
    histograms_by_email = defaultdict(lambda: defaultdict(list))

    for channel, histograms in expiring_histograms_by_channel.items():
        for name, emails in histograms.items():
            for email in emails:
                histograms_by_email[email][channel].append(name)

    for email, histograms_by_channel in histograms_by_email.items():
        per_channel_format_strings = [
            PER_CHANNEL_FORMAT_STRING.format(channel=channel, probes="\n".join(histogram_names))
            for channel, histogram_names
            in histograms_by_channel.items()
        ]

        email_body = EMAIL_BODY_FORMAT_STRING.format(
            expire_days=expire_days,
            per_channel_probes="\n".join(per_channel_format_strings)
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
    # Only run on Mondays
    if datetime.date.today().weekday() != 0:
        return

    probe_info = requests.get(PROBE_INFO_BASE_URL + "firefox/all/main/all_probes").json()

    release_dates = release_calendar.get_release_dates()

    expiring_probes = find_expiring_probes(probe_info, release_dates, expire_days=EMAIL_DAYS_BEFORE)

    send_emails_for_expiring_probes(expiring_probes, EMAIL_DAYS_BEFORE, dryrun)


if __name__ == "__main__":
    args = parse_args()
    main(args.dry_run)
