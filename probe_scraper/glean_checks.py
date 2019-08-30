# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This file contains various sanity checks for Glean.
"""

import os

from schema import And, Optional, Schema


def check_glean_metric_structure(data):
    schema = Schema(
        {
            str: {
                Optional(And(str, lambda x: len(x) == 40)): [
                    And(str, lambda x: os.path.exists(x))
                ]
            }
        }
    )

    schema.validate(data)


def check_for_duplicate_metrics(repositories, metrics_by_repo, emails):
    """
    Checks for duplicate metric names across all libraries used by a particular application.
    Queues a warning e-mail if any are found, and removes all metrics for the app with
    duplicate metrics.
    """
    repo_by_library_name = {}
    repo_by_name = {}
    for repo in repositories:
        for library_name in repo.library_names or []:
            repo_by_library_name[library_name] = repo.name
        repo_by_name[repo.name] = repo

    for repo in repositories:
        dependencies = [repo.name] + [
            repo_by_library_name[library_name] for library_name in repo.dependencies
        ]

        metric_sources = {}
        for dependency in dependencies:
            for metric in metrics_by_repo[dependency].keys():
                metric_sources.setdefault(metric, []).append(dependency)

        duplicate_sources = dict(
            (k, v) for (k, v) in metric_sources.items() if len(v) > 1
        )

        if len(duplicate_sources):
            for name, sources in duplicate_sources.items():
                msg = "Duplicate metric: {!r}: exists in {}".format(
                    name, ", ".join(sources)
                )

                addresses = set()
                for source in sources:
                    # Send to the repository contacts
                    addresses.update(repo_by_name[source].notification_emails)

                    # Also send to the metric's contacts
                    for history_entry in metrics_by_repo[source][name]["history"]:
                        addresses.update(history_entry["notification_emails"])

                emails[name] = {
                    "emails": [
                        {
                            "subject": "Probe scraper: Duplicate metric identifiers",
                            "message": msg,
                        }
                    ],
                    "addresses": list(addresses),
                }

            # Delete metrics for the given repo
            metrics_by_repo[repo.name] = {}
