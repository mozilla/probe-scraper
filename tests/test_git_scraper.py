# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from git import Repo
from probe_scraper import runner
from probe_scraper.emailer import EMAIL_FILE
from probe_scraper.transform_probes import HISTORY_KEY, COMMITS_KEY
import datetime
from pathlib import Path
import json
import os
import pytest
import shutil
import time
import yaml


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


def get_repo(repo_name):
    directory = os.path.join(test_dir, repo_name)
    repo = Repo.init(directory)

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
        commit_date = commit_date[:commit_date.find('.')]
        repo.index.commit(
            "Commit {index}".format(index=i),
            commit_date=commit_date
        )

    return directory


@pytest.fixture
def normal_repo():
    location = get_repo(normal_repo_name)
    repositories_info = {
        normal_repo_name: {
            "app_id": "normal_app_name",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "metrics_files": ["metrics.yaml"],
            "dependencies": [
                'org.mozilla.components:service-glean',
                'org.mozilla.components:lib-crash',
            ]
        },
        "glean": {
            "app_id": "glean",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "library_names": ["org.mozilla.components:service-glean"]
        },
        "lib-crash": {
            "app_id": "lib-crash",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "library_names": ["org.mozilla.components:lib-crash"]
        }
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


@pytest.fixture
def improper_metrics_repo():
    location = get_repo(improper_repo_name)
    repositories_info = {
        improper_repo_name: {
            "app_id": "improper_app_name",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "metrics_files": ["metrics.yaml"]
        }
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


def test_normal_repo(normal_repo):
    runner.main(cache_dir, out_dir, None, None, False, True, repositories_file, True, None, None)

    path = os.path.join(out_dir, "glean", normal_repo_name, "metrics")

    with open(path, 'r') as data:
        metrics = json.load(data)

    # there are 2 metrics
    assert len(metrics) == 2

    duration = 'example.duration'
    os_metric = 'example.os'

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

    with open(path, 'r') as data:
        dependencies = json.load(data)

    assert len(dependencies) == 2


def test_improper_metrics_repo(improper_metrics_repo):
    runner.main(cache_dir, out_dir, None, None, False, True, repositories_file, True, None, None)

    path = os.path.join(out_dir, "glean", improper_repo_name, "metrics")
    with open(path, 'r') as data:
        metrics = json.load(data)

    # should be empty output, since it was an improper file
    assert not metrics

    with open(EMAIL_FILE, 'r') as email_file:
        emails = yaml.load(email_file)

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
        normal_repo_name: {
            "app_id": "normal_app_name",
            "notification_emails": ["repo_alice@example.com"],
            "url": normal_duplicate_repo,
            "metrics_files": ["metrics.yaml"],
            "dependencies": ["duplicate_library"]
        },
        duplicate_repo_name: {
            "app_id": "duplicate_library_name",
            "notification_emails": ["repo_bob@example.com"],
            "url": duplicate_repo,
            "metrics_files": ["metrics.yaml"],
            "library_names": ["duplicate_library"]
        }
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    try:
        runner.main(cache_dir, out_dir, None, False, True, repositories_file, True, None)
    except ValueError:
        pass
    else:
        assert False, "Expected exception"

    with open(EMAIL_FILE, 'r') as email_file:
        emails = yaml.load(email_file)

    # should send 1 email
    assert len(emails) == 1

    assert "'example.duration' defined more than once" in emails[0]['body']
    assert "example.os" not in emails[0]['body']

    assert set(emails[0]['recipients'].split(',')) == set([
        # Metrics owners
        'alice@example.com',
        'bob@example.com',
        'charlie@example.com',
        # Repo owners
        'repo_alice@example.com',
        'repo_bob@example.com',
        # Everything goes here
        'dev-telemetry-alerts@mozilla.com'
    ])
