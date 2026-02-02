# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This file contains various sanity checks for Glean.
"""

import datetime
import logging
from pathlib import Path
from typing import Any, Dict

from schema import And, Optional, Schema

from probe_scraper import probe_expiry_alert

from .scrapers.git_scraper import Commit

# Ugly hack to skip certain metrics which we know aren't duplicated,
# but show up duplicated due to them being moved from one
# to the other application/library.
SKIP_METRICS = {
    "gecko.version": ["gecko", "pine", "firefox-desktop"],
    "gecko.build_id": ["gecko", "pine", "firefox-desktop"],
    "broken_site_report.breakage_category": ["gecko", "fenix", "firefox-desktop"],
    "broken_site_report.description": ["gecko", "fenix", "firefox-desktop"],
    "broken_site_report.url": ["gecko", "fenix", "firefox-desktop"],
    "broken_site_report.browser_info.app.default_locales": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.app.default_useragent_string": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.app.fission_enabled": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.device_pixel_ratio": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.devices_json": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.drivers_json": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.features_json": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.has_touch_screen": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.graphics.monitors_json": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.cookie_behavior": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.global_privacy_control_enabled": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.installtrigger_enabled": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.opaque_response_blocking": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.resist_fingerprinting_enabled": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.prefs.software_webrender": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.browser_info.system.memory": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.languages": ["gecko", "fenix", "firefox-desktop"],
    "broken_site_report.tab_info.useragent_string": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.block_list": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.btp_has_purged_site": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.has_mixed_active_content_blocked": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.has_mixed_display_content_blocked": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.has_tracking_content_blocked": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.antitracking.is_private_browsing": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.frameworks.fastclick": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.frameworks.marfeel": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    "broken_site_report.tab_info.frameworks.mobify": [
        "gecko",
        "fenix",
        "firefox-desktop",
    ],
    # All moved into the crashping.
    # Due to the metrics file being _removed_ from firefox-crashreporter
    # these are not correctly flagged as not-in-source
    # and are thus marked as duplicate.
    "crash.app_build": ["crashping", "firefox-crashreporter"],
    "crash.app_channel": ["crashping", "firefox-crashreporter"],
    "crash.app_display_version": ["crashping", "firefox-crashreporter"],
    "crash.async_shutdown_timeout": ["crashping", "firefox-crashreporter"],
    "crash.background_task_name": ["crashping", "firefox-crashreporter"],
    "crash.event_loop_nesting_level": ["crashping", "firefox-crashreporter"],
    "crash.font_name": ["crashping", "firefox-crashreporter"],
    "crash.gpu_process_launch": ["crashping", "firefox-crashreporter"],
    "crash.ipc_channel_error": ["crashping", "firefox-crashreporter"],
    "crash.is_garbage_collecting": ["crashping", "firefox-crashreporter"],
    "crash.main_thread_runnable_name": ["crashping", "firefox-crashreporter"],
    "crash.moz_crash_reason": ["crashping", "firefox-crashreporter"],
    "crash.process_type": ["crashping", "firefox-crashreporter"],
    "crash.profiler_child_shutdown_phase": ["crashping", "firefox-crashreporter"],
    "crash.quota_manager_shutdown_timeout": ["crashping", "firefox-crashreporter"],
    "crash.remote_type": ["crashping", "firefox-crashreporter"],
    "crash.shutdown_progress": ["crashping", "firefox-crashreporter"],
    "crash.stack_traces": ["crashping", "firefox-crashreporter"],
    "crash.startup": ["crashping", "firefox-crashreporter"],
    "crash.time": ["crashping", "firefox-crashreporter"],
    "crash.utility_actors_name": ["crashping", "firefox-crashreporter"],
    "crash.windows.error_reporting": ["crashping", "firefox-crashreporter"],
    "crash.windows.file_dialog_error_code": ["crashping", "firefox-crashreporter"],
    "dll_blocklist.init_failed": ["crashping", "firefox-crashreporter"],
    "dll_blocklist.list": ["crashping", "firefox-crashreporter"],
    "dll_blocklist.user32_loaded_before": ["crashping", "firefox-crashreporter"],
    "environment.headless_mode": ["crashping", "firefox-crashreporter"],
    "environment.nimbus_enrollments": ["crashping", "firefox-crashreporter"],
    "environment.uptime": ["crashping", "firefox-crashreporter"],
    "memory.available_commit": ["crashping", "firefox-crashreporter"],
    "memory.available_physical": ["crashping", "firefox-crashreporter"],
    "memory.available_swap": ["crashping", "firefox-crashreporter"],
    "memory.available_virtual": ["crashping", "firefox-crashreporter"],
    "memory.js_large_allocation_failure": ["crashping", "firefox-crashreporter"],
    "memory.js_out_of_memory": ["crashping", "firefox-crashreporter"],
    "memory.low_physical": ["crashping", "firefox-crashreporter"],
    "memory.oom_allocation_size": ["crashping", "firefox-crashreporter"],
    "memory.purgeable_physical": ["crashping", "firefox-crashreporter"],
    "memory.system_use_percentage": ["crashping", "firefox-crashreporter"],
    "memory.texture": ["crashping", "firefox-crashreporter"],
    "memory.total_page_file": ["crashping", "firefox-crashreporter"],
    "memory.total_physical": ["crashping", "firefox-crashreporter"],
    "memory.total_virtual": ["crashping", "firefox-crashreporter"],
    "windows.package_family_name": ["crashping", "firefox-crashreporter"],
    "crash.minidump_sha256_hash": ["crashping", "firefox-crashreporter"],
}


def _metric_sort_key(metric: Dict[str, Any]):
    return (
        datetime.datetime.fromisoformat(metric["dates"]["last"]),
        -metric["reflog-index"]["last"],
    )


def get_current_metrics_by_repo(
    metrics_by_repo: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    We have a whole history of these metrics, but expiry only cares about the current state.
    Return the current state of metrics.
    """
    return {
        repo_name: {
            metric_name: sorted(metric["history"], key=_metric_sort_key)[-1]
            for metric_name, metric in metrics.items()
            if metric["in-source"]
        }
        for repo_name, metrics in metrics_by_repo.items()
    }


def check_glean_metric_structure(data):
    schema = Schema(
        {
            str: {
                Optional(And(Commit, lambda x: len(x.hash) == 40)): [
                    And(Path, lambda x: x.exists())
                ]
            }
        }
    )

    schema.validate(data)


DUPLICATE_METRICS_EMAIL_TEMPLATE = """
Glean has detected duplicated metric identifiers coming from the product '{repo.name}'.

{duplicates}

What to do about this:

1. File a bug to track your investigation. You can just copy this email into the bug Description to get you started.
2. Reply-All to this email to let the list know that you are investigating. Include the bug number so we can help out.
3. Rename the most recently added metric to be more specific. See [1]
4. Make sure a Glean team member reviews any patches. Care needs to be taken that the resolution of this problem is schema-compatible.

If you have any problems, please ask for help on the #glean Slack channel. We'll give you a hand.

What this is:

We have a system called probe-scraper [2] that scrapes the metric information from all Mozilla products using the Glean SDK. All the scraped data is available on the probeinfo service [3]. The scraped definition is used to build things such as the probe-dictionary [4] and other data tools. It detected that one metric that was recently added has an identifier collision with some metric that already existed in the application namespace. So it sent this email out, encouraging you to fix the problem.

What happens if you don't fix this:

The metrics will compete to send their data in pings, making the data unreliable at best.

You can do this!

Your Friendly, Neighborhood Glean Team

[1] - https://mozilla.github.io/glean/book/user/adding-new-metrics.html#naming-things
[2] - https://github.com/mozilla/probe-scraper
[3] - https://probeinfo.telemetry.mozilla.org/
[4] - https://telemetry.mozilla.org/probe-dictionary/
"""  # noqa


class MissingDependencyError(ValueError):
    pass


def check_for_duplicate_metrics(repositories, metrics_by_repo, emails):
    """
    Checks for duplicate metric names across all libraries used by a particular application.
    It only checks for metrics that exist in the latest (HEAD) commit in each repo, so that
    it's possible to remove (or disable) the metric in the latest commit and not have this
    check repeatedly fail.
    If duplicates are found, e-mails are queued and this returns True.
    """
    found_duplicates = False

    repo_by_library_name = {}
    repo_by_name = {}
    for repo in repositories:
        for library_name in repo.library_names or []:
            repo_by_library_name[library_name] = repo.name
        repo_by_name[repo.name] = repo

    for repo in repositories:
        for library_name in repo.dependencies:
            if library_name not in repo_by_library_name:
                raise MissingDependencyError(
                    f"{repo.name} missing dependency {library_name}"
                )
        dependencies = [repo.name] + [
            repo_by_library_name[library_name] for library_name in repo.dependencies
        ]

        metric_sources = {}
        for dependency in dependencies:
            # skip if no metrics
            if not metrics_by_repo[dependency]:
                continue
            # otherwise look for the latest timestamp for all metrics --
            # metrics which don't appear in the latest can be assumed to
            # no longer be present
            last_timestamp = max(
                [
                    metric["history"][-1]["dates"]["last"]
                    for metric in metrics_by_repo[dependency].values()
                ]
            )
            for metric_name, metric in metrics_by_repo[dependency].items():
                if metric["history"][-1]["dates"]["last"] == last_timestamp:
                    metric_sources.setdefault(metric_name, []).append(dependency)

        duplicate_sources = {}
        for k, v in metric_sources.items():
            # Exempt cases when one of the sources is Geckoview Streaming to
            # avoid false positive duplication accross app channels.
            # Temporarily exempt cases when one of the sources is server compat library
            # to avoid raising alarm for metrics defined in fxa's custom ping.
            v = [
                dep
                for dep in v
                if (
                    "engine-gecko" not in dep
                    and "glean-server-metrics-compat" not in dep
                )
            ]

            if k in SKIP_METRICS.keys():
                potential_deps = SKIP_METRICS[k]
                if any([dep for dep in potential_deps if dep in v]):
                    continue

            if len(v) > 1:
                duplicate_sources[k] = v

        if not len(duplicate_sources):
            continue

        found_duplicates = True

        addresses = set()
        duplicates = []
        for name, sources in duplicate_sources.items():
            duplicate_error_msg = "- {!r} defined more than once in {}".format(
                name, ", ".join(sorted(sources))
            )

            duplicates.append(duplicate_error_msg)
            logging.error(duplicate_error_msg)

            for source in sources:
                # Send to the repository contacts
                addresses.update(repo_by_name[source].notification_emails)

                # Also send to the metric's contacts
                for history_entry in metrics_by_repo[source][name]["history"]:
                    addresses.update(history_entry["notification_emails"])

        duplicates = "\n".join(duplicates)

        emails[f"duplicate_metrics_{repo.name}"] = {
            "emails": [
                {
                    "subject": "Glean: Duplicate metric identifiers detected",
                    "message": DUPLICATE_METRICS_EMAIL_TEMPLATE.format(
                        duplicates=duplicates, repo=repo
                    ),
                }
            ],
            "addresses": list(addresses),
        }

    return found_duplicates


EXPIRED_METRICS_EMAIL_TEMPLATE = """
Each metric in the following list from {repo_name} will expire in the next {expire_days} days or has already expired.

{expired_metrics}

What to do about this:

1. If the metric is no longer needed, remove it from its `metrics.yaml` [1] file.
2. If the metric is still required, resubmit a data review [2] and extend its expiration date.

If you have any problems, please ask for help on the #glean Matrix channel[3]. We'll give you a hand.

What happens if you don't fix this:

The metrics listed above will stop collecting data from builds built after this expiration date,
and you will continue to get this e-mail as a reminder.

Your Friendly, Neighborhood Glean Team

[1] The correct metrics.yaml is in this list:
{metrics_yaml_url}
[2] https://wiki.mozilla.org/Firefox/Data_Collection
[3] https://chat.mozilla.org/#/room/#glean:mozilla.org

This is an automated message sent from probe-scraper.  See https://github.com/mozilla/probe-scraper for details.
"""  # noqa


def check_for_expired_metrics(
    repositories,
    metrics_by_repo,
    emails,
    expire_days=14,
):
    """
    Checks for all expired metrics and generates e-mails, one per repository.
    """

    expiration_cutoff = datetime.datetime.utcnow().date() + datetime.timedelta(
        days=expire_days
    )

    current_metrics_by_repo = get_current_metrics_by_repo(metrics_by_repo)

    for repo in repositories:
        metrics = current_metrics_by_repo[repo.name]

        addresses = set(repo.notification_emails)

        # Do not send expiry for deprecated repositories
        if repo.deprecated:
            continue

        target_version = None
        if repo.name == "fenix":
            try:
                target_version = (
                    int(probe_expiry_alert.get_latest_nightly_version()) + 1
                )
            except ValueError:
                # Can't parse the version as an int. Welp.
                pass

        expired_metrics = []
        for metric_name, metric in metrics.items():
            if metric["expires"] == "never":
                continue

            # `expires` field supports manual expiry, too.
            if metric["expires"] == "expired":
                expired_metrics.append(f"- {metric_name} manually expired")
                addresses.update(metric["notification_emails"])
                continue

            if isinstance(metric["expires"], int):
                # Uses expire-by-version.
                if target_version is not None:
                    if metric["expires"] == target_version:
                        expired_metrics.append(f" - {metric_name} in {target_version}")
                        addresses.update(metric["notification_emails"])
                continue

            try:
                expires = datetime.datetime.strptime(
                    metric["expires"], "%Y-%m-%d"
                ).date()
            except ValueError:
                # String does not contain a date, so we don't currently handle expiration.
                pass
            else:
                if expiration_cutoff >= expires:
                    expired_metrics.append(f"- {metric_name} on {expires}")
                    addresses.update(metric["notification_emails"])
        expired_metrics.sort()

        if len(expired_metrics) == 0:
            continue

        metrics_yaml_url = "\n".join(
            f"{repo.url}/tree/HEAD/{file}" for file in repo.metrics_file_paths
        )

        emails[f"expired_metrics_{repo.name}"] = {
            "emails": [
                {
                    "subject": f"Glean: Expired metrics in {repo.name}",
                    "message": EXPIRED_METRICS_EMAIL_TEMPLATE.format(
                        repo_name=repo.name,
                        expire_days=expire_days,
                        expired_metrics="\n".join(expired_metrics),
                        metrics_yaml_url=metrics_yaml_url,
                    ),
                }
            ],
            "addresses": list(addresses),
        }
