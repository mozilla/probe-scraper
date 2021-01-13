# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime
import json
import os
import shutil
import time
import unittest.mock
from pathlib import Path

import pytest
import yaml
from git import Head, Repo

from probe_scraper import runner
from probe_scraper.emailer import EMAIL_FILE
from probe_scraper.transform_probes import COMMITS_KEY, HISTORY_KEY

# Where the test files are located
base_dir = "tests/resources/test_repo_files"

# Where we will build the test git repo
test_dir = ".test_git_repositories"

# Where we will write the repositories file
repositories_file = ".repositories.yaml"

# Number of commits in the test repository
num_commits = 1000

cache_dir = ".cache"
out_dir = ".out"

# names of the test repos
normal_repo_name = "normal"
improper_repo_name = "improper"
duplicate_repo_name = "duplicate"
expired_repo_name = "expired"


def rm_if_exists(*paths):
    for path in paths:
        if os.path.exists(path):
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)


@pytest.yield_fixture(autouse=True)
def run_before_tests():
    rm_if_exists(EMAIL_FILE, cache_dir, out_dir)
    os.mkdir(cache_dir)
    os.mkdir(out_dir)
    yield
    rm_if_exists(cache_dir, out_dir, test_dir)


def get_repo(repo_name, branch="master"):
    directory = os.path.join(test_dir, repo_name)
    repo = Repo.init(directory)
    # Ensure the default branch is using a fixed name.
    # User config could change that,
    # breaking tests with implicit assumptions further down the line.
    repo.head.reference = Head(repo, f"refs/heads/{branch}")

    # We need to synthesize the time stamps of commits to each be a second
    # apart, otherwise the commits may be at exactly the same second, which
    # means they won't always sort in order, and thus the merging of identical
    # metrics in adjacent commits may not happen correctly.
    base_time = time.time()

    base_path = os.path.join(base_dir, repo_name)
    for i in range(num_commits):
        files_dir = os.path.join(base_path, str(i))
        if not os.path.exists(files_dir):
            break

        files = os.listdir(files_dir)
        for filename in files:
            print("Copying file " + filename)
            path = os.path.join(base_path, str(i), filename)
            destination = os.path.join(directory, filename)
            shutil.copyfile(path, destination)

        repo.index.add("*")
        commit_date = datetime.datetime.fromtimestamp(base_time + i).isoformat()
        commit_date = commit_date[: commit_date.find(".")]
        repo.index.commit("Commit {index}".format(index=i), commit_date=commit_date)

    return directory


def proper_repo(branch="master"):
    location = get_repo(normal_repo_name, branch)
    repositories_info = {
        "version": "2",
        "libraries": [
            {
                "v1_name": "glean",
                "description": "foo",
                "notification_emails": ["frank@mozilla.com"],
                "url": location,
                "library_names": ["org.mozilla.components:service-glean"],
            },
            {
                "v1_name": "boollib",
                "description": "foo",
                "notification_emails": ["frank@mozilla.com"],
                "url": location,
                "library_names": ["org.mozilla.components:lib-crash"],
            },
        ],
        "applications": [
            {
                "app_name": "proper_repo_example",
                "canonical_app_name": "Proper Repo Example",
                "description": "foo",
                "url": location,
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "dependencies": [
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                ],
                "channels": [
                    {
                        "v1_name": normal_repo_name,
                        "app_id": "normal-app-name",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


@pytest.fixture
def normal_repo():
    return proper_repo()


@pytest.fixture
def main_repo():
    return proper_repo("main")


@pytest.fixture
def improper_metrics_repo():
    location = get_repo(improper_repo_name)
    repositories_info = {
        "version": "2",
        "libraries": [],
        "applications": [
            {
                "app_name": "mobile_metrics_example",
                "canonical_app_name": "Mobile Metrics Example",
                "description": "foo",
                "url": location,
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": improper_repo_name,
                        "app_id": "improper-app-name",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


def test_normal_repo(normal_repo):
    runner.main(
        cache_dir,
        out_dir,
        None,
        None,
        False,
        True,
        repositories_file,
        True,
        None,
        None,
        None,
        None,
        "dev",
    )

    path = os.path.join(out_dir, "glean", normal_repo_name, "metrics")

    with open(path, "r") as data:
        metrics = json.load(data)

    # there are 2 metrics
    assert len(metrics) == 2

    duration = "example.duration"
    os_metric = "example.os"

    # duration has 2 definitions
    assert len(metrics[duration][HISTORY_KEY]) == 2

    # os has 3 definitions
    assert len(metrics[os_metric][HISTORY_KEY]) == 3

    # duration same begin/end commits for first history entry
    assert len(set(metrics[duration][HISTORY_KEY][0][COMMITS_KEY].values())) == 1

    # duration same begin/end commits for first history entry
    assert len(set(metrics[duration][HISTORY_KEY][1][COMMITS_KEY].values())) == 2

    # os was in 1 commit
    assert len(set(metrics[os_metric][HISTORY_KEY][0][COMMITS_KEY].values())) == 1

    # There should have been no errors
    assert not Path(EMAIL_FILE).exists()

    path = os.path.join(out_dir, "glean", normal_repo_name, "dependencies")

    with open(path, "r") as data:
        dependencies = json.load(data)

    assert len(dependencies) == 2

    path = os.path.join(out_dir, "v2", "glean", "app-listings")

    with open(path, "r") as data:
        applications = json.load(data)

    # /v2/glean/app-listings excludes libraries
    assert len(applications) == 1

    # /v2/glean/app-listings includes derived fields
    assert applications[0]["document_namespace"] == "normal-app-name"
    assert applications[0]["bq_dataset_family"] == "normal_app_name"


def test_improper_metrics_repo(improper_metrics_repo):
    runner.main(
        cache_dir,
        out_dir,
        None,
        None,
        False,
        True,
        repositories_file,
        True,
        None,
        None,
        None,
        None,
        "dev",
    )

    path = os.path.join(out_dir, "glean", improper_repo_name, "metrics")
    with open(path, "r") as data:
        metrics = json.load(data)

    # should be empty output, since it was an improper file
    assert not metrics

    with open(EMAIL_FILE, "r") as email_file:
        emails = yaml.load(email_file, Loader=yaml.FullLoader)

    # should send 1 email
    assert len(emails) == 1


@pytest.fixture
def normal_duplicate_repo():
    return get_repo(normal_repo_name)


@pytest.fixture
def duplicate_repo():
    return get_repo(duplicate_repo_name)


def test_check_for_duplicate_metrics(normal_duplicate_repo, duplicate_repo):
    repositories_info = {
        "version": "2",
        "libraries": [
            {
                "v1_name": "mylib",
                "description": "foo",
                "notification_emails": ["repo_alice@example.com"],
                "url": normal_duplicate_repo,
                "metrics_files": ["metrics.yaml"],
                "library_names": ["duplicate_library"],
            },
        ],
        "applications": [
            {
                "app_name": "duplicate_metrics_example",
                "canonical_app_name": "Duplicate Metrics Example",
                "description": "foo",
                "url": duplicate_repo,
                "notification_emails": ["repo_bob@example.com"],
                "metrics_files": ["metrics.yaml"],
                "dependencies": [
                    "duplicate_library",
                ],
                "channels": [
                    {
                        "v1_name": normal_repo_name,
                        "app_id": "normal-app-name",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    try:
        runner.main(
            cache_dir,
            out_dir,
            None,
            None,
            False,
            True,
            repositories_file,
            True,
            None,
            None,
            None,
            None,
            "dev",
        )
    except ValueError:
        pass
    else:
        assert False, "Expected exception"

    with open(EMAIL_FILE, "r") as email_file:
        emails = yaml.load(email_file, Loader=yaml.FullLoader)

    # should send 1 email
    assert len(emails) == 1

    assert "'example.duration' defined more than once" in emails[0]["body"]
    assert "example.os" not in emails[0]["body"]

    assert set(emails[0]["recipients"].split(",")) == set(
        [
            # Metrics owners
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            # Repo owners
            "repo_alice@example.com",
            "repo_bob@example.com",
            # Everything goes here
            "glean-team@mozilla.com",
        ]
    )


@pytest.fixture
def expired_repo():
    return get_repo(expired_repo_name)


def test_check_for_expired_metrics(expired_repo):
    repositories_info = {
        "version": "2",
        "libraries": [],
        "applications": [
            {
                "app_name": "expired_metrics_example",
                "canonical_app_name": "Expired Metrics Example",
                "description": "foo",
                "url": expired_repo,
                "notification_emails": ["repo_alice@example.com"],
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": expired_repo_name,
                        "app_id": "expired-app-name",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    # Mock `datetime.date.today` so it's a Monday, the only day that
    # expirations are checked.
    class MockDate(datetime.date):
        @classmethod
        def today(cls):
            return datetime.date(2019, 10, 14)

    with unittest.mock.patch("probe_scraper.glean_checks.datetime.date", new=MockDate):
        runner.main(
            cache_dir,
            out_dir,
            None,
            None,
            False,
            True,
            repositories_file,
            True,
            None,
            None,
            None,
            None,
            "dev",
        )

    with open(EMAIL_FILE, "r") as email_file:
        emails = yaml.load(email_file, Loader=yaml.FullLoader)

    # should send 1 email
    assert len(emails) == 1

    assert "example.duration on 2019-01-01" in emails[0]["body"]

    assert set(emails[0]["recipients"].split(",")) == set(
        [
            # Metrics owners
            "bob@example.com",
            # Repo owners
            "repo_alice@example.com",
            # Everything goes here
            "glean-team@mozilla.com",
        ]
    )


def test_repo_default_main_branch(main_repo):
    runner.main(
        cache_dir,
        out_dir,
        None,
        None,
        False,
        True,
        repositories_file,
        True,
        None,
        None,
        None,
        None,
        "dev",
    )

    path = os.path.join(out_dir, "glean", normal_repo_name, "metrics")

    with open(path, "r") as data:
        metrics = json.load(data)

    # there are 2 metrics
    assert len(metrics) == 2

    duration = "example.duration"
    os_metric = "example.os"

    # duration has 2 definitions
    assert len(metrics[duration][HISTORY_KEY]) == 2

    # os has 3 definitions
    assert len(metrics[os_metric][HISTORY_KEY]) == 3

    # duration same begin/end commits for first history entry
    assert len(set(metrics[duration][HISTORY_KEY][0][COMMITS_KEY].values())) == 1

    # duration same begin/end commits for first history entry
    assert len(set(metrics[duration][HISTORY_KEY][1][COMMITS_KEY].values())) == 2

    # os was in 1 commit
    assert len(set(metrics[os_metric][HISTORY_KEY][0][COMMITS_KEY].values())) == 1

    # There should have been no errors
    assert not Path(EMAIL_FILE).exists()

    path = os.path.join(out_dir, "glean", normal_repo_name, "dependencies")

    with open(path, "r") as data:
        dependencies = json.load(data)

    assert len(dependencies) == 2
