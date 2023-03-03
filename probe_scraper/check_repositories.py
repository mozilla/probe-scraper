import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Set, Tuple

import git
import requests as reqs
from glean_parser.lint import lint_yaml_files

from .parsers.repositories import RepositoriesParser

GIT = git.Git()
GIT_BRANCH_PATTERN = re.compile("ref: refs/heads/([^\t]+)\tHEAD")
GITHUB_RAW_URL = "https://raw.githubusercontent.com"
REPOSITORIES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "repositories.yaml"
)
EXPECTED_MISSING_FILES: Set[Tuple[str, str]] = {
    ("support-migration", "components/support/migration/metrics.yaml")
}
validation_errors = []
repos = RepositoriesParser().parse(REPOSITORIES)

app_id_channels = defaultdict(lambda: defaultdict(lambda: 0))

for repo in repos:
    metrics_files = repo.get_metrics_file_paths()
    temp_errors = []

    if repo.app_id and repo.channel and not repo.deprecated:
        app_id_channels[repo.app_id][repo.channel] += 1

    for metric_file in metrics_files:
        if (repo.name, metric_file) in EXPECTED_MISSING_FILES:
            continue  # ignore missing files

        branch = repo.branch
        if branch is None:
            match = GIT_BRANCH_PATTERN.match(
                GIT.ls_remote("--symref", repo.url, "HEAD")
            )
            if match is None:
                temp_errors += ["Failed to get default branch from git for " + repo.url]
                continue
            branch = match.groups()[0]

        temp_url = (
            repo.url.replace("https://github.com", GITHUB_RAW_URL)
            + "/"
            + branch
            + "/"
            + metric_file
        )
        response = reqs.get(temp_url)
        if response.status_code != 200:
            temp_errors += ["Metrics file was not found at " + temp_url]
        else:
            with open("temp-metrics.yaml", "w") as filehandle:
                filehandle.write(response.text)
            yaml_lint_errors = open("yaml-lint-errors.txt", "w")
            temp_errors += lint_yaml_files(
                [Path("./temp-metrics.yaml")], yaml_lint_errors, {}
            )
            os.remove("yaml-lint-errors.txt")
            os.remove("temp-metrics.yaml")
    if temp_errors and not repo.prototype:
        validation_errors.append({"repo": repo.name, "errors": temp_errors})

# Ensure non-deprecated channels are uniquely named
duplication_errors = []
for app_id, channels in app_id_channels.items():
    temp_errors = []
    for channel_name, num in channels.items():
        if num > 1:
            duplication_errors.append(
                f"Non-deprecated channel names must be unique, found {channel_name} {num} "
                f"times for {app_id}"
            )

if validation_errors:
    print("\nSummary of validation errors:\n")
    print(f"{len(validation_errors)} repositories had problems\n")
    for error in validation_errors:
        print(f"\nErrors found in {error['repo']}:\n")
        for line_errors in error["errors"]:
            print(line_errors)

if duplication_errors:
    print("\nDuplicate channel names found:\n")
    for duplication_error in duplication_errors:
        print(duplication_error)

if validation_errors or duplication_errors:
    exit(1)
