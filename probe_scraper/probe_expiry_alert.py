# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from dataclasses import dataclass
from typing import List
import argparse
import datetime
import os
import re
import tempfile

import requests

from probe_scraper.parsers.events import EventsParser
from probe_scraper.parsers.histograms import HistogramsParser
from probe_scraper.parsers.scalars import ScalarsParser
from probe_scraper.parsers.utils import get_major_version

BUGZILLA_BUG_URL = "https://bugzilla-dev.allizom.org/rest/bug"
BUGZILLA_BUG_URL_2 = "https://bugzilla.mozilla.org/rest/bug"  # TODO: delete
BUGZILLA_USER_URL = "https://bugzilla-dev.allizom.org/rest/user"

BASE_URI = "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/"
HISTOGRAMS_FILE = "Histograms.json"
SCALARS_FILE = "Scalars.yaml"
EVENTS_FILE = "Events.yaml"

BUG_WHITEBOARD_TAG = "probe-expiry-alert"
BUG_SUMMARY_TEMPLATE = "Remove or update probes expiring in Firefox {version}: {probe}"

BUG_DESCRIPTION_TEMPLATE = """
The following Firefox probes will expire in the next major Firefox nightly release: version {version} [1].

```
{probes}
```

What to do about this:
1. If one, some, or all of the metrics are no longer needed, please remove them from their definitions files (Histograms.json, Scalars.yaml, Events.yaml).
2. If one, some, or all of the metrics are still required, please submit a Data Collection Review [2] and patch to extend their expiry.

If you have any problems, please ask for help on the #fx-metrics Slack channel or the #telemetry Matrix room at https://chat.mozilla.org/#/room/#telemetry:mozilla.org. We'll give you a hand.

Your Friendly, Neighborhood Telemetry Team

[1] https://wiki.mozilla.org/Release_Management/Calendar
[2] https://wiki.mozilla.org/Firefox/Data_Collection

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa


@dataclass
class ProbeDetails:
    name: str
    product: str
    component: str
    emails: List[str]
    previous_bug: int

    def __eq__(self, other):
        if type(other) == str:
            return self.name == other
        elif type(other) == type(self):
            return self.name == other.name
        raise ValueError(f'Incompatible comparison types: {type(self)} == {type(other)}')


def bugzilla_request_header(api_key: str):
    return {"X-BUGZILLA-API-KEY": api_key}


def get_bug_component(bug_id: int, api_key: str):
    response = requests.get(BUGZILLA_BUG_URL_2 + "/" + str(bug_id))
                            #headers=bugzilla_request_header(api_key))  # TODO: change url
    response.raise_for_status()
    bug = response.json()["bugs"][0]
    return bug["product"], bug["component"]


def search_bugs(api_key: str):
    search_query_params = {
        "whiteboard": BUG_WHITEBOARD_TAG,
        "include_fields": "description,summary",
    }
    response = requests.get(BUGZILLA_BUG_URL, params=search_query_params,
                            headers=bugzilla_request_header(api_key))
    response.raise_for_status()
    return response.json()["bugs"]


def get_longest_prefix(values, tolerance=0):
    """
    Return the longest matching prefix among the list of strings.
    If a prefix is less than 2 characters, return the first string.
    Tolerance allows some characters to not match.
    """
    if len(values) == 1:
        return values[0]
    if len(values) == 0:
        return ''

    longest_match = []
    for c in zip(*values):
        if len(set(c)) > min([1 + tolerance, len(values) - 1]):
            break
        longest_match.append(c[0])

    if len(longest_match) < 2:
        return values[0]

    return ''.join(longest_match) + '*'


def create_bug(probes: List[ProbeDetails], version: str,  api_key: str):
    probe_names = [probe.name for probe in probes]
    probe_prefix = get_longest_prefix(probe_names, tolerance=1)

    create_params = {
        "product": probes[0].product,
        "component": probes[0].component,
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=probe_prefix),
        "description": BUG_DESCRIPTION_TEMPLATE.format(
            version=version, probes="\n".join(probe_names)),
        "version": "unspecified",
        "type": "task",
        "whiteboard": BUG_WHITEBOARD_TAG,
        "see_also": 1396144,  # TODO: probes[0].previous_bug,
        "flags": [
            {
                "name": "needinfo",
                "type_id": 800,
                "status": "?",
                "requestee": email,
            }
            for email in probes[0].emails
        ],
    }
    create_response = requests.post(BUGZILLA_BUG_URL, json=create_params,
                                    headers=bugzilla_request_header(api_key))
    try:  # TODO: remove catch
        create_response.raise_for_status()
    except requests.HTTPError:
        print(create_response.json())
    print(f"Created bug {str(create_response.json())} for {probe_prefix}")


def check_bugzilla_user_exists(email: str, api_key: str):
    user_response = requests.get(BUGZILLA_USER_URL + "?match=" + email,
                                 headers=bugzilla_request_header(api_key))
    user_response.raise_for_status()
    return len(user_response.json()["users"]) > 0


def get_latest_nightly_version():
    versions = requests.get("https://product-details.mozilla.org/1.0/firefox_versions.json").json()
    return get_major_version(versions["FIREFOX_NIGHTLY"])


def download_file(url: str, output_filepath: str):
    content = requests.get(url).text
    with open(output_filepath, "w") as output_file:
        output_file.write(content)


def find_expiring_probes(probes: dict, target_version: str,
                         bugzilla_api_key: str) -> List[ProbeDetails]:
    """
    Find probes expiring in the target version

    Returns list of probes where each probe is of form:
    {
        name: str,
        emails: [str],
        product: str,
        component: str,
    }
    """
    expiring_probes = []
    for name, details in probes.items():
        expiry_version = details["expiry_version"]

        if expiry_version == target_version:
            if len(details["bug_numbers"]) == 0:
                last_bug_number = None
                product = "Firefox"
                component = "General"
            else:
                last_bug_number = max(details["bug_numbers"])
                product, component = (get_bug_component(last_bug_number, bugzilla_api_key))
            expiring_probes.append(
                ProbeDetails(name, product, component,
                             details.get("notification_emails", []), last_bug_number))

    return expiring_probes


def file_bugs(probes: List[ProbeDetails], version: str,
              bugzilla_api_key: str, create_bugs: bool = False):
    """
    Search for bugs that have already been created by probe_expiry_alerts
    for probes in the current version.
    For each probe/bug:
        - if a bug exists for a probe in the given list, do nothing
        - if no bug exists for a given probe, create a bug
    """
    found_bugs = search_bugs(bugzilla_api_key)

    new_expiring_probes = probes.copy()
    for bug in found_bugs:
        # Regex for version and probe names requires bug description to be BUG_DESCRIPTION_TEMPLATE
        if re.search("release: version (\d+)", bug["description"]).group(1) != version:
            continue
        probes_in_bug = re.search("```(.*)```", bug["description"], re.DOTALL).group(1).split()
        for probe_name in probes_in_bug:
            if probe_name in [probe.name for probe in probes]:
                new_expiring_probes.remove(probe_name)

    print(f"Found previously created bugs for {len(probes) - len(new_expiring_probes)} probes "
          f"for version {version}")

    # group by component and email
    probes_by_component_by_email_set = defaultdict(list)
    for probe in new_expiring_probes:
        probes_by_component_by_email_set[
            (probe.product, probe.component, ','.join(sorted(probe.emails)))].append(probe)

    print(f"creating {len(probes_by_component_by_email_set)} new bugs")

    for grouping, probe_group in probes_by_component_by_email_set.items():
        if create_bugs:
            create_bug(probe_group, version, bugzilla_api_key)


def main(current_date: datetime.datetime, dryrun: bool, bugzilla_api_key: str):
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

    expiring_probes = find_expiring_probes(all_probes, next_version, bugzilla_api_key)

    print(f"Found {len(expiring_probes)} probes expiring in nightly {next_version}")
    print([probe.name for probe in expiring_probes])

    # Only send create bugs on Wednesdays, run the rest for debugging/error detection
    create_bugs = not dryrun and current_date.weekday() == 2

    # find emails with no bugzilla account
    emails_wo_accounts = defaultdict(list)
    for probe_name, email_list in [(probe.name, probe.emails) for probe in expiring_probes]:
        for i, email in enumerate(email_list.copy()):
            if (email in emails_wo_accounts.keys() or
                    not check_bugzilla_user_exists(email, bugzilla_api_key)):
                emails_wo_accounts[email].append(probe_name)
                email_list.remove(email)

    file_bugs(expiring_probes, next_version, bugzilla_api_key, create_bugs=create_bugs)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=datetime.date.fromisoformat,
        required=True,
    )
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


if __name__ == "__main__":
    args = parse_args()
    main(args.date, args.dry_run, args.bugzilla_api_key)
