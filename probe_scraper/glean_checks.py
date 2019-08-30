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
    for repo in repositories:
        for library_name in repo.library_names or []:
            repo_by_library_name[library_name] = repo.name

    for repo in repositories:
        dependencies = [repo.name] + [
            repo_by_library_name[library_name] for library_name in repo.dependencies
        ]

        metric_sources = {}
        for dependency in dependencies:
            for metric in metrics_by_repo[dependency].keys():
                metric_sources.setdefault(metric, []).append(dependency)

        if any(len(x) > 1 for x in metric_sources.values()):
            msg = ["Duplicate metrics:"]
            for name, sources in metric_sources.items():
                if len(sources) > 1:
                    msg.append("{!r}: from {}".format(name, ", ".join(sources)))
            msg = "\n".join(msg)
            emails[repo.name]["emails"].append(
                {"subject": "Probe scraper: Duplicate metric names", "message": msg}
            )

            # Delete metrics for the given repo
            metrics_by_repo[repo.name] = {}
