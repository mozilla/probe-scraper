# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime
import json
import time
from pathlib import Path

import pytest
import yaml
from git import Head, Repo

from probe_scraper import runner
from probe_scraper.emailer import EMAIL_FILE
from probe_scraper.transform_probes import COMMITS_KEY, HISTORY_KEY

# Where the test files are located
base_dir = Path("tests/resources/test_repo_files")

# Number of commits in the test repository
num_commits = 1000

# names of the test repos
normal_repo_name = "normal"
improper_repo_name = "improper"
duplicate_repo_name = "duplicate"
expired_repo_name = "expired"


@pytest.fixture(autouse=True)
def run_before_tests():
    path = Path(EMAIL_FILE)
    if path.exists():
        path.unlink()


@pytest.fixture
def cache_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("cache")


@pytest.fixture
def out_dir(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("out")


@pytest.fixture
def test_dir(tmp_path_factory) -> Path:
    # Where we will build the test git repo
    return tmp_path_factory.mktemp("test_git_repositories")


@pytest.fixture
def repositories_file(test_dir: Path) -> Path:
    # Where we will write the repositories file
    return test_dir / "repositories.yaml"


def get_repo(test_dir: Path, repo_name: str, branch: str = "master") -> Path:
    directory = test_dir / repo_name
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

    base_path = base_dir / repo_name
    for i in range(num_commits):
        files_dir = base_path / str(i)
        if not files_dir.exists():
            break

        for path in files_dir.iterdir():
            print(f"Copying file {path.name}")
            destination = directory / path.name
            destination.write_bytes(path.read_bytes())

        repo.index.add("*")
        commit_date = datetime.datetime.fromtimestamp(base_time + i).isoformat()
        commit_date = commit_date[: commit_date.find(".")]
        repo.index.commit("Commit {index}".format(index=i), commit_date=commit_date)

    return directory


def proper_repo(
    test_dir: Path, repositories_file: Path, branch: str = "master"
) -> Path:
    location = get_repo(test_dir, normal_repo_name, branch)
    repositories_info = {
        "version": "2",
        "libraries": [
            {
                "library_name": "glean-core",
                "description": "foo",
                "notification_emails": ["frank@mozilla.com"],
                "url": str(location),
                "variants": [
                    {
                        "v1_name": "glean",
                        "dependency_name": "org.mozilla.components:service-glean",
                    }
                ],
            },
            {
                "library_name": "boollib",
                "description": "foo",
                "notification_emails": ["frank@mozilla.com"],
                "url": str(location),
                "variants": [
                    {
                        "v1_name": "boollib",
                        "dependency_name": "org.mozilla.components:lib-crash",
                    }
                ],
            },
        ],
        "applications": [
            {
                "app_name": "proper_repo_example",
                "canonical_app_name": "Proper Repo Example",
                "app_description": "foo",
                "url": str(location),
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "tag_files": ["tags.yaml"],
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
def normal_repo(test_dir: Path, repositories_file: Path):
    return proper_repo(test_dir, repositories_file)


@pytest.fixture
def main_repo(test_dir: Path, repositories_file: Path):
    return proper_repo(test_dir, repositories_file, "main")


@pytest.fixture
def improper_metrics_repo(test_dir: Path, repositories_file: Path):
    location = get_repo(test_dir, improper_repo_name)
    repositories_info = {
        "version": "2",
        "libraries": [],
        "applications": [
            {
                "app_name": "mobile_metrics_example",
                "canonical_app_name": "Mobile Metrics Example",
                "app_description": "foo",
                "url": str(location),
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


def test_normal_repo(
    cache_dir: Path, out_dir: Path, repositories_file: Path, normal_repo: Path
):
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
        None,
    )

    path = out_dir / "glean" / normal_repo_name / "metrics"

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

    # duration *different* begin/end commits for last history entry
    assert len(set(metrics[duration][HISTORY_KEY][1][COMMITS_KEY].values())) == 2

    # os in last history entry has tags
    assert metrics[os_metric][HISTORY_KEY][-1].get("metadata") == {"tags": ["foo"]}

    # os was in 1 commit
    assert len(set(metrics[os_metric][HISTORY_KEY][0][COMMITS_KEY].values())) == 1

    # There should have been no errors
    assert not Path(EMAIL_FILE).exists()

    path = out_dir / "glean" / normal_repo_name / "dependencies"

    with open(path, "r") as data:
        dependencies = json.load(data)

    assert len(dependencies) == 2

    path = out_dir / "v2" / "glean" / "app-listings"

    with open(path, "r") as data:
        applications = json.load(data)

    # /v2/glean/app-listings excludes libraries
    assert len(applications) == 1

    # /v2/glean/app-listings includes derived fields
    assert applications[0]["document_namespace"] == "normal-app-name"
    assert applications[0]["bq_dataset_family"] == "normal_app_name"


def test_improper_metrics_repo(
    cache_dir: Path, out_dir: Path, repositories_file: Path, improper_metrics_repo: Path
):
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
        None,
    )

    path = out_dir / "glean" / improper_repo_name / "metrics"
    with open(path, "r") as data:
        metrics = json.load(data)

    # should be empty output, since it was an improper file
    assert not metrics

    with open(EMAIL_FILE, "r") as email_file:
        emails = yaml.load(email_file, Loader=yaml.FullLoader)

    # should send 1 email
    assert len(emails) == 1


@pytest.fixture
def normal_duplicate_repo(test_dir: Path):
    return get_repo(test_dir, normal_repo_name)


@pytest.fixture
def duplicate_repo(test_dir: Path):
    return get_repo(test_dir, duplicate_repo_name)


def test_check_for_duplicate_metrics(
    normal_duplicate_repo: Path,
    duplicate_repo: Path,
    cache_dir: Path,
    out_dir: Path,
    repositories_file: Path,
):
    repositories_info = {
        "version": "2",
        "libraries": [
            {
                "library_name": "mylib",
                "description": "foo",
                "notification_emails": ["repo_alice@example.com"],
                "url": str(normal_duplicate_repo),
                "metrics_files": ["metrics.yaml"],
                "variants": [
                    {
                        "v1_name": "mylib",
                        "dependency_name": "duplicate_library",
                    }
                ],
            },
        ],
        "applications": [
            {
                "app_name": "duplicate_metrics_example",
                "canonical_app_name": "Duplicate Metrics Example",
                "app_description": "foo",
                "url": str(duplicate_repo),
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
        f.write(yaml.safe_dump(repositories_info))

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
            None,
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

    assert set(emails[0]["to"].split(",")) == {
        # Metrics owners
        "alice@example.com",
        "bob@example.com",
        "charlie@example.com",
        # Repo owners
        "repo_alice@example.com",
        "repo_bob@example.com",
        # Everything goes here
        "glean-team@mozilla.com",
    }


@pytest.fixture
def expired_repo(test_dir: Path):
    return get_repo(test_dir, expired_repo_name)


def test_check_for_expired_metrics(
    expired_repo: Path, out_dir: Path, cache_dir: Path, repositories_file: str
):
    repositories_info = {
        "version": "2",
        "libraries": [],
        "applications": [
            {
                "app_name": "expired_metrics_example",
                "canonical_app_name": "Expired Metrics Example",
                "app_description": "foo",
                "url": str(expired_repo),
                "notification_emails": ["repo_alice@example.com"],
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": expired_repo_name,
                        "app_id": "expired-app-name",
                        "app_channel": "release",
                    }
                ],
            },
            {
                "app_name": "expired_metrics_example_deprecated",
                "canonical_app_name": "Expired Metrics Example Deprecated",
                "deprecated": True,
                "app_description": "foo",
                "deprecated": True,
                "url": str(expired_repo),
                "notification_emails": ["repo_alice@example.com"],
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": expired_repo_name + "-deprecated",
                        "app_id": "expired-app-name-deprecated",
                        "app_channel": "release",
                    }
                ],
            },
        ],
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.safe_dump(repositories_info))

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
        None,
        check_expiry=True,
    )

    with open(EMAIL_FILE, "r") as email_file:
        emails = yaml.load(email_file, Loader=yaml.FullLoader)

    # should send 1 email
    assert len(emails) == 1

    # should send it for expired, but not the deprecated one
    assert "Glean: Expired metrics in expired" == emails[0]["subject"]
    assert "Glean: Expired metrics in expired-deprecated" != emails[0]["subject"]

    assert "example.duration on 2019-01-01" in emails[0]["body"]

    assert set(emails[0]["to"].split(",")) == {
        # Metrics owners
        "bob@example.com",
        # Repo owners
        "repo_alice@example.com",
        # Everything goes here
        "glean-team@mozilla.com",
    }


def test_repo_default_main_branch(
    cache_dir: Path, out_dir: Path, repositories_file: str, main_repo: Path
):
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
        None,
    )

    path = out_dir / "glean" / normal_repo_name / "metrics"

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

    path = out_dir / "glean" / normal_repo_name / "dependencies"

    with open(path, "r") as data:
        dependencies = json.load(data)

    assert len(dependencies) == 2
