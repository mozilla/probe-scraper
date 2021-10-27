# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import datetime
import os
import re
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple, Union

import requests

from probe_scraper import emailer
from probe_scraper.parsers.events import EventsParser
from probe_scraper.parsers.histograms import HistogramsParser
from probe_scraper.parsers.scalars import ScalarsParser
from probe_scraper.parsers.utils import get_major_version

FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "glean-team@mozilla.com"

BUGZILLA_BUG_URL = "https://bugzilla.mozilla.org/rest/bug"
BUGZILLA_USER_URL = "https://bugzilla.mozilla.org/rest/user"
BUGZILLA_BUG_LINK_TEMPLATE = "https://bugzilla.mozilla.org/show_bug.cgi?id={bug_id}"

BASE_URI = (
    "https://hg.mozilla.org/mozilla-central/raw-file/tip/toolkit/components/telemetry/"
)
HISTOGRAMS_FILE = "Histograms.json"
SCALARS_FILE = "Scalars.yaml"
EVENTS_FILE = "Events.yaml"

BUG_DEFAULT_PRODUCT = "Firefox"
BUG_DEFAULT_COMPONENT = "General"
BUG_WHITEBOARD_TAG = "[probe-expiry-alert]"
BUG_SUMMARY_TEMPLATE = "Remove or update probes expiring in Firefox {version}: {probe}"

# Regex for version and probe names requires bug description to have a certain structure
# This template should be modified with care
BUG_DESCRIPTION_TEMPLATE = """
The following Firefox probes will expire in the next major Firefox nightly release: version {version} [1].

```
{probes}
```

{notes}

What to do about this:
1. If one, some, or all of the metrics are no longer needed, please remove them from their definitions files (Histograms.json, Scalars.yaml, Events.yaml).
2. If one, some, or all of the metrics are still required, please submit a Data Collection Review [2] and patch to extend their expiry.  There is a shorter form for data collection renewal [3].

If you have any problems, please ask for help on the #data-help Slack channel or the #telemetry Matrix room at https://chat.mozilla.org/#/room/#telemetry:mozilla.org. We'll give you a hand.

Your Friendly, Neighborhood Telemetry Team

[1] https://wiki.mozilla.org/Release_Management/Calendar
[2] https://wiki.mozilla.org/Firefox/Data_Collection
[3] https://github.com/mozilla/data-review/blob/master/renewal_request.md

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa

BUG_LINK_LIST_TEMPLATE = """The following bugs were filed for the above probes:
{bug_links}
"""

# This text is compared to a json blob, where quotes are escaped
NEEDINFO_BLOCKED_TEXT = 'is not currently accepting \\"needinfo\\" requests.'


@dataclass
class ProbeDetails:
    name: str
    product: str
    component: str
    emails: List[str]
    # int will be put in the "see also" field, string will be in description due to permissions
    previous_bug: Union[int, str, None]


def bugzilla_request_header(api_key: str) -> Dict[str, str]:
    return {"X-BUGZILLA-API-KEY": api_key}


def get_bug_component(
    bug_id: int, api_key: str
) -> Tuple[Union[str, None], Union[str, None]]:
    response = requests.get(
        BUGZILLA_BUG_URL + "/" + str(bug_id), headers=bugzilla_request_header(api_key)
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"Error getting component for bug {bug_id}: {e}")
        if (
            e.response.status_code == 401
        ):  # Some confidential security bugs are not accessible
            return None, None
        else:
            raise

    bug = response.json()["bugs"][0]
    return bug["product"], bug["component"]


def find_existing_bugs(version: str, api_key: str) -> Dict[str, int]:
    """Find bugs filed for the version and return mappings of probe name to bug id."""
    search_query_params = {
        "whiteboard": BUG_WHITEBOARD_TAG,
        "include_fields": "description,summary,id",
    }
    response = requests.get(
        BUGZILLA_BUG_URL,
        params=search_query_params,
        headers=bugzilla_request_header(api_key),
    )
    response.raise_for_status()

    found_bugs = response.json()["bugs"]

    probes_with_bugs = {}
    for bug in found_bugs:
        if re.search(r"release: version (\d+)", bug["description"]).group(1) != version:
            continue
        probes_in_bug = (
            re.search(r"```(.*)```", bug["description"], re.DOTALL).group(1).split()
        )
        for probe_name in probes_in_bug:
            probes_with_bugs[probe_name] = bug["id"]

    return probes_with_bugs


def get_longest_prefix(values: List[str], tolerance: int = 0) -> str:
    """
    Return the longest matching prefix among the list of strings.
    If a prefix is less than 4 characters, return the first string.
    Tolerance allows some characters to not match and returns the highest occurring prefix.
    """
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")
    if len(values) == 1:
        return values[0]
    if len(values) == 0:
        return ""

    if tolerance > 0:
        longest_value_length = max(len(v) for v in values)
        values = [v.ljust(longest_value_length) for v in values]

    prefix_length = 0
    for c in zip(*values):
        if len(set(c)) > min([1 + tolerance, len(values) - 1]):
            break
        prefix_length += 1

    if prefix_length < 4:
        return values[0]

    if tolerance == 0:
        return values[0][:prefix_length]

    prefix_count = defaultdict(int)
    for value in values:
        prefix_count[value[:prefix_length]] += 1

    return (
        sorted(prefix_count.items(), key=lambda item: item[1], reverse=True)[0][0] + "*"
    )


def create_bug(
    probes: List[ProbeDetails], version: str, api_key: str, needinfo: bool = True
) -> int:
    probe_names = [probe.name for probe in probes]
    probe_prefix = get_longest_prefix(probe_names, tolerance=1)

    see_also_bugs = list(
        set(
            [
                probe.previous_bug
                for probe in probes
                if isinstance(probe.previous_bug, int)
            ]
        )
    )
    see_also_bugs_str = list(
        set(
            [
                probe.previous_bug
                for probe in probes
                if isinstance(probe.previous_bug, str)
            ]
        )
    )

    if len(see_also_bugs_str) == 0:
        notes = ""
    else:
        notes = (
            "The following bugs are associated with the above "
            f"probe{'s' if len(probes) > 0 else ''}: "
            f"{', '.join([f'bug {bug_num}' for bug_num in see_also_bugs_str])}"
        )

    create_params = {
        "product": probes[0].product,
        "component": probes[0].component,
        "summary": BUG_SUMMARY_TEMPLATE.format(version=version, probe=probe_prefix),
        "description": BUG_DESCRIPTION_TEMPLATE.format(
            version=version, probes="\n".join(probe_names), notes=notes
        ),
        "version": "unspecified",
        "type": "task",
        "whiteboard": BUG_WHITEBOARD_TAG,
        "see_also": see_also_bugs,
        "flags": [
            {"name": "needinfo", "type_id": 800, "status": "?", "requestee": email}
            for email in probes[0].emails
            if needinfo
        ],
        "cc": [email for email in probes[0].emails if not needinfo],
    }
    create_response = requests.post(
        BUGZILLA_BUG_URL, json=create_params, headers=bugzilla_request_header(api_key)
    )
    try:
        create_response.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to create bugs with arguments: {create_params}", file=sys.stderr)
        print(f"Error response: {create_response.text}", file=sys.stderr)
        if needinfo and NEEDINFO_BLOCKED_TEXT in create_response.text:
            print(
                "Needinfo request blocked, retrying request without needinfo",
                file=sys.stderr,
            )
            return create_bug(probes, version, api_key, needinfo=False)
        else:
            raise
    print(f"Created bug {str(create_response.json())} for {probe_prefix}")
    return create_response.json()["id"]


def check_bugzilla_user_exists(email: str, api_key: str):
    user_response = requests.get(
        BUGZILLA_USER_URL + "?names=" + email, headers=bugzilla_request_header(api_key)
    )
    try:
        user_response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # 400 is raised if user does not exist
        if e.response.status_code == 400 and e.response.json()["code"] == 51:
            return False
        raise
    # As of Sept 2020, api seems to be returning 200 response with an unknown
    # error code when user isn't found
    if user_response.json().get("error"):
        return False
    return user_response.json()["users"][0]["can_login"]


def get_latest_nightly_version():
    versions = requests.get(
        "https://product-details.mozilla.org/1.0/firefox_versions.json"
    ).json()
    return get_major_version(versions["FIREFOX_NIGHTLY"])


def download_file(url: str, output_filepath: str):
    content = requests.get(url).text
    with open(output_filepath, "w") as output_file:
        output_file.write(content)


def find_expiring_probes(
    probes: dict, target_version: str, bugzilla_api_key: str
) -> List[ProbeDetails]:
    """
    Find probes expiring in the target version
    """
    expiring_probes = []
    for name, details in probes.items():
        expiry_version = details["expiry_version"]

        if expiry_version == target_version:
            if len(details["bug_numbers"]) == 0:
                last_bug_number = None
                product = BUG_DEFAULT_PRODUCT
                component = BUG_DEFAULT_COMPONENT
            else:
                last_bug_number = max(details["bug_numbers"])
                product, component = get_bug_component(
                    last_bug_number, bugzilla_api_key
                )
                if product is None and component is None:
                    last_bug_number = str(last_bug_number)
                    product = BUG_DEFAULT_PRODUCT
                    component = BUG_DEFAULT_COMPONENT
            expiring_probes.append(
                ProbeDetails(
                    name,
                    product,
                    component,
                    details.get("notification_emails", []),
                    last_bug_number,
                )
            )

    return expiring_probes


def send_emails(
    probes_by_email: Dict[str, List[str]],
    probe_to_bug_id: Dict[str, int],
    version: str,
    dryrun: bool = True,
):
    # send all probes to glean-team for debugging
    probes_by_email[DEFAULT_TO_EMAIL] = list(set(sum(probes_by_email.values(), [])))

    email_count = 0
    for email, probe_names in probes_by_email.items():
        # No probes found -> nothing to do
        if not probe_names:
            continue

        bug_links = {
            BUGZILLA_BUG_LINK_TEMPLATE.format(bug_id=probe_to_bug_id[probe])
            for probe in probe_names
            if probe in probe_to_bug_id.keys()
        }
        if len(bug_links) == 0 and email != DEFAULT_TO_EMAIL:
            continue  # no bug links means bugs were already created and emails sent

        email_body = BUG_DESCRIPTION_TEMPLATE.format(
            version=version,
            probes="\n".join(probe_names),
            notes=BUG_LINK_LIST_TEMPLATE.format(bug_links="\n".join(bug_links)),
        )

        emailer.send_ses(
            FROM_EMAIL,
            "Telemetry Probe Expiry",
            body=email_body,
            recipients=email,
            dryrun=dryrun,
        )
        email_count += 1

    print(f"Sent emails to {email_count} recipients")


def file_bugs(
    probes: List[ProbeDetails], version: str, bugzilla_api_key: str, dryrun: bool = True
) -> Dict[str, int]:
    """
    Search for bugs that have already been created by probe_expiry_alerts
    for probes in the current version.
    For each probe/bug:
        - if a bug exists for a probe in the given list, do nothing
        - if no bug exists for a given probe, create a bug

    Return mapping of probe names to bug id for any newly created bugs
    """
    existing_bugs = find_existing_bugs(version, bugzilla_api_key)

    new_expiring_probes = [
        probe for probe in probes if probe.name not in existing_bugs.keys()
    ]

    print(
        f"Found previously created bugs for {len(probes) - len(new_expiring_probes)}"
        f" probes for version {version}"
    )

    # group by component and email
    probes_by_component_by_email_set = defaultdict(list)
    for probe in new_expiring_probes:
        probes_by_component_by_email_set[
            (probe.product, probe.component, ",".join(sorted(probe.emails)))
        ].append(probe)

    print(f"creating {len(probes_by_component_by_email_set)} new bugs")

    probe_to_bug_id_map = existing_bugs

    for grouping, probe_group in probes_by_component_by_email_set.items():
        if not dryrun:
            bug_id = create_bug(probe_group, version, bugzilla_api_key)
            for probe in probe_group:
                probe_to_bug_id_map[probe.name] = bug_id

    return probe_to_bug_id_map


def main(current_date: datetime.date, dryrun: bool, bugzilla_api_key: str):
    # Only send create bugs on Wednesdays, run the rest for debugging/error detection
    dryrun = dryrun or current_date.weekday() != 2

    next_version = str(int(get_latest_nightly_version()) + 1)

    with tempfile.TemporaryDirectory() as tempdir:
        events_file_path = os.path.join(tempdir, EVENTS_FILE)
        download_file(BASE_URI + EVENTS_FILE, events_file_path)
        events = EventsParser().parse([events_file_path])

        histograms_file_path = os.path.join(tempdir, HISTOGRAMS_FILE)
        download_file(BASE_URI + HISTOGRAMS_FILE, histograms_file_path)
        histograms = HistogramsParser().parse(
            [histograms_file_path], version=next_version
        )

        scalars_file_path = os.path.join(tempdir, SCALARS_FILE)
        download_file(BASE_URI + SCALARS_FILE, scalars_file_path)
        scalars = ScalarsParser().parse([scalars_file_path])

    all_probes = events.copy()
    all_probes.update(histograms)
    all_probes.update(scalars)

    expiring_probes = find_expiring_probes(all_probes, next_version, bugzilla_api_key)

    print(f"Found {len(expiring_probes)} probes expiring in nightly {next_version}")
    print([probe.name for probe in expiring_probes])

    # find emails with no bugzilla account
    emails_wo_accounts = defaultdict(list)
    for probe_name, email_list in [
        (probe.name, probe.emails) for probe in expiring_probes
    ]:
        for i, email in enumerate(email_list.copy()):
            if email in emails_wo_accounts.keys() or not check_bugzilla_user_exists(
                email, bugzilla_api_key
            ):
                emails_wo_accounts[email].append(probe_name)
                email_list.remove(email)

    probe_to_bug_id = file_bugs(
        expiring_probes, next_version, bugzilla_api_key, dryrun=dryrun
    )

    send_emails(emails_wo_accounts, probe_to_bug_id, next_version, dryrun=dryrun)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument(
        "--dry-run", help="Whether emails should be sent", action="store_true"
    )
    parser.add_argument("--bugzilla-api-key", type=str, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args.date, args.dry_run, args.bugzilla_api_key)
