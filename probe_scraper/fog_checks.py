# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This file contains various checks for Firefox on Glean (FOG).

FOG is Glean, yes, but is sufficiently different that it benefits from doing
its own expiry checks. Sending its own emails. Filing its own bugs.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, TypedDict

from probe_scraper import probe_expiry_alert

from .glean_checks import get_current_metrics_by_repo
from .parsers.repositories import Repository

EXPIRED_METRICS_EMAIL_TEMPLATE = """
Each metric in the following list will soon expire in Firefox {version}.
For your convenience, we've filed bugs to track the work of removing or renewing them:

{expiring_bugs_list}

What to do about this:

1. If the metric is no longer needed, remove it from its `metrics.yaml` file.
2. If the metric is still required, extend its expiration.

If you have any problems, please ask for help on the #glean Matrix channel[1]. We'll give you a hand.

What happens if you don't fix this:

The expiring metric will expire, causing a test failure which
* makes sheriffs unhappy,
* prevents developers from landing code, and
* generally makes for a bad time.

You will continue to get this e-mail as a reminder to clean up.

Your Friendly Neighbourhood Glean Team

[1] https://chat.mozilla.org/#/room/#glean:mozilla.org

This is an automated message sent from probe-scraper. See https://github.com/mozilla/probe-scraper for details.
"""  # noqa


###
# Types for Annotations:
###
class Email(TypedDict):
    subject: str
    message: str


class EmailInfo(TypedDict):
    addresses: List[str]
    emails: List[Email]


# The full list of all repos that are FOG style. Must:
#  * Expire based on Firefox Desktop Nightly Version, and
#  * Use Bugzilla for its bug urls
FOG_REPOS: Set[str] = {"firefox-desktop", "gecko"}


# The BMO whiteboard tag to use for auto-filed bugs
BUG_WHITEBOARD_TAG = "[metric-expiry-alert]"
# The BMO Title, templated by version and metric family
BUG_SUMMARY_TEMPLATE = "Remove or update metrics expiring in Firefox {version}: {probe}"
# BE ALERT: We regex on this template to find existing bugs.
# SEE probe_expiry_alert.find_existing_bugs FOR DETAILS.
# IF YOU MODIFY THIS WITHOUT CARE WE WILL FILE DUPLICATE BUGS.
# Please be kind to your Sheriffs and only modify with care.
BUG_DESCRIPTION_TEMPLATE = """
The following metrics will expire in the next Firefox Nightly release: [version {version}][1].

```
{probes}
```

{notes}

What to do about this:
1. If one, some, or all of the metrics are no longer needed, please remove them from their `metrics.yaml` definition file.
2. If one, some, or all of the metrics are still required, please submit a patch to extend their expiry.

If you have any problems, please ask for help on the [#glean Matrix room](https://chat.mozilla.org/#/room/#glean:mozilla.org) or the #data-help Slack channel.
We'll give you a hand.

Your Friendly Neighbourhood Glean Team

[1]: https://wiki.mozilla.org/Release_Management/Calendar

---
This bug was auto-filed by [probe-scraper](https://github.com/mozilla/probe-scraper).
"""  # noqa


def get_expiring_metrics(
    metrics: Dict[str, Dict], latest_nightly_version: str
) -> Dict[str, Dict]:
    """
    Filter the provided dict of metric name to metric info to just the expiring ones.
    """

    # We start warning one version ahead.
    target_version = int(latest_nightly_version) + 1

    expiring_metrics = {}
    for metric_name, metric in metrics.items():
        if metric["expires"] == "never":
            continue

        if metric["expires"] == "expired":
            # Also include manually-expired ones.
            # This is not only technically correct, but makes testing easier.
            expiring_metrics[metric_name] = metric
            continue

        try:
            expiry_version = int(metric["expires"])
        except ValueError:
            # Expires cannot be parsed as a version. Treat as unexpired.
            # TODO: Should we send emails for unparseable expiry versions?
            continue

        if expiry_version == target_version:
            expiring_metrics[metric_name] = metric

    return expiring_metrics


def bug_number_from_url(url: str) -> Optional[int]:
    """
    Given a bug url, get its bug number.
    If we can't figure out a reasonable bug number, return None.
    """
    # ASSUMPTION: bug urls end in the pattern `=<bug number>`
    # TODO: Write a test in Firefox Desktop that asserts this is true.
    try:
        return int(url.rsplit("=")[-1])
    except ValueError:
        print(f"Can't figure out bug number for url: {url}")
        return None


def file_bugs(
    expiring_metrics: Dict[str, Dict],
    latest_nightly_version: str,
    bugzilla_api_key: str,
    dry_run: bool = True,
) -> Dict[str, List[str]]:
    """
    Find existing and file new Bugzilla bugs for expiring metrics.
    Needs a network connection.
    If `dry_run`, doesn't file any new bugs, returning a fake bug url for all expiring metrics.
    """

    next_version = str(int(latest_nightly_version) + 1)

    # We try our best to reuse pieces of probe_expiry_alert.
    # Swizzle and filter expiring_metrics into a list of ProbeDetails structs.
    expiring_probes: List[probe_expiry_alert.ProbeDetails] = []
    for metric_name, metric in expiring_metrics.items():
        bug_numbers: List[Optional[int]] = [
            bug_number_from_url(url) for url in metric["bugs"]
        ]
        biggest_bug_number: Optional[int] = max(
            [bug for bug in bug_numbers if bug is not None], default=None
        )
        if biggest_bug_number is not None:
            product, component = probe_expiry_alert.get_bug_component(
                biggest_bug_number, bugzilla_api_key
            )
        else:
            product, component = None, None
        if product is None and component is None:
            product = probe_expiry_alert.BUG_DEFAULT_PRODUCT
            component = probe_expiry_alert.BUG_DEFAULT_COMPONENT

        expiring_probes.append(
            probe_expiry_alert.ProbeDetails(
                metric_name,
                product,
                component,
                metric.get("notification_emails", []),
                biggest_bug_number,
            )
        )

    # Debug print time
    print(f"Found {len(expiring_probes)} 'probes' expiring in nightly {next_version}:")
    print([probe.name for probe in expiring_probes])

    metrics_to_bug_numbers = probe_expiry_alert.file_bugs(
        expiring_probes,
        next_version,
        bugzilla_api_key,
        dry_run,
        BUG_WHITEBOARD_TAG,
        BUG_SUMMARY_TEMPLATE,
        BUG_DESCRIPTION_TEMPLATE,
    )

    # Swizzle out to a metric_name -> List[bug urls] dict
    bug_urls_to_metrics = defaultdict(list)
    for metric_name, bug_number in metrics_to_bug_numbers.items():
        bug_urls_to_metrics[
            probe_expiry_alert.BUGZILLA_BUG_LINK_TEMPLATE.format(bug_id=bug_number)
        ].append(metric_name)

    if dry_run:
        return {"https://example.com/fake_bug_url/": expiring_metrics.keys()}

    return bug_urls_to_metrics


def file_bugs_and_get_emails_for_expiring_metrics(
    repositories: List[Repository],
    metrics_by_repo: Dict[str, Dict[str, Dict[str, Any]]],
    bugzilla_api_key: Optional[str],
    dry_run: bool = True,
) -> Optional[Dict[str, EmailInfo]]:
    """
    If the provided repositories and metrics contain FOG-using repos:
     * Determine which metrics are expiring in the next version.
     * File bugs in Bugzilla for them, in the product and component of the most recent bug.
       At most one bug per metric category. (Doesn't happen if you don't provide an API key.)
     * Return a list of emails to send. At most one per FOG repo.
    """

    if len(FOG_REPOS & metrics_by_repo.keys()) == 0:
        print("No FOG-using repositories. Nothing to do.")
        return None

    # Glean repositories have a default list of notification emails we should include as well.
    repo_addresses = {
        repo.name: repo.notification_emails
        for repo in repositories
        if repo.name in FOG_REPOS
    }

    current_metrics_by_repo = get_current_metrics_by_repo(metrics_by_repo)

    emails = {}
    for fog_repo in FOG_REPOS:
        if fog_repo not in metrics_by_repo:
            continue
        current_metrics: Dict[str, Dict] = current_metrics_by_repo[fog_repo]
        latest_nightly_version: str = probe_expiry_alert.get_latest_nightly_version()
        expiring_metrics: Dict[str, Dict] = get_expiring_metrics(
            current_metrics, latest_nightly_version
        )

        print(f"Found {len(expiring_metrics)} expiring metrics in {fog_repo}.")
        if len(expiring_metrics) == 0:
            continue

        metrics_addresses = set(repo_addresses[fog_repo])
        for metric in expiring_metrics.values():
            metrics_addresses.update(metric["notification_emails"])
        addresses = list(metrics_addresses)

        filed_bugs: Dict[str, List[str]] = file_bugs(
            expiring_metrics, latest_nightly_version, bugzilla_api_key, dry_run
        )

        expiring_bugs_list = []
        for bug_url, bug_metrics in filed_bugs.items():
            # Sort the metric names for easier reading
            bug_metrics = list(bug_metrics)
            bug_metrics.sort()

            expiring_metrics_list_str = "\n".join(bug_metrics)
            expiring_bugs_list.append(f"{bug_url}:\n{expiring_metrics_list_str}")

        # Nothing expiring? No emails needed.
        if len(expiring_bugs_list) == 0:
            continue

        emails[f"expired_metrics_{fog_repo}"] = EmailInfo(
            emails=[
                {
                    "subject": f"Expired metrics in {fog_repo}",
                    "message": EXPIRED_METRICS_EMAIL_TEMPLATE.format(
                        expiring_bugs_list="\n".join(expiring_bugs_list),
                        version=int(latest_nightly_version) + 1,
                    ),
                }
            ],
            addresses=addresses,
        )

    return emails
