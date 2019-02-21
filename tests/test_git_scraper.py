# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from git import Repo
from probe_scraper import runner
from probe_scraper.emailer import EMAIL_FILE
from probe_scraper.transform_probes import HISTORY_KEY, COMMITS_KEY
import json
import os
import pytest
import shutil
import yaml


# Where the test files are located
base_dir = "tests/test_repo_files"

# Where we will build the test git repo
test_dir = ".test_git_repositories"

# Where we will write the repositories file
repositories_file = ".repositories.yaml"

cache_dir = ".cache"
out_dir = ".out"

# names of the test repos
normal_repo_name = "normal"
improper_repo_name = "improper"


def rm_if_exists(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)


@pytest.yield_fixture(autouse=True)
def run_before_tests():
    rm_if_exists(EMAIL_FILE)
    rm_if_exists(cache_dir)
    rm_if_exists(out_dir)
    os.mkdir(cache_dir)
    os.mkdir(out_dir)
    yield
    rm_if_exists(cache_dir)
    rm_if_exists(out_dir)
    rm_if_exists(test_dir)


def get_repo(repo_name):
    directory = "{test_dir}/{repo_name}".format(test_dir=test_dir, repo_name=repo_name)
    repo = Repo.init(directory)

    base_path = "{base_dir}/{repo_name}".format(base_dir=base_dir, repo_name=repo_name)
    for i in range(1000):
        files_dir = "{base_path}/{index}".format(base_path=base_path, index=i)
        if not os.path.exists(files_dir):
            break

        files = os.listdir(files_dir)
        for filename in files:
            path = "{base_path}/{index}/{filename}".format(
                   base_path=base_path, index=i, filename=filename)
            destination = "{directory}/{filename}".format(
                          directory=directory, filename=filename)
            shutil.copyfile(path, destination)

        repo.index.add("*")
        repo.index.commit("Commit {index}".format(index=i))

    return directory


@pytest.fixture
def normal_repo():
    location = get_repo(normal_repo_name)
    repositories_info = {
        normal_repo_name: {
            "app_name": "normal_app_name",
            "os": "Android",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "scalar_file_paths": ["Scalars.yaml"]
        }
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


@pytest.fixture
def improper_scalar_repo():
    location = get_repo(improper_repo_name)
    repositories_info = {
        improper_repo_name: {
            "app_name": "improper_app_name",
            "os": "Android",
            "notification_emails": ["frank@mozilla.com"],
            "url": location,
            "scalar_file_paths": ["Scalars.yaml"]
        }
    }

    with open(repositories_file, "w") as f:
        f.write(yaml.dump(repositories_info))

    return location


def test_normal_repo(normal_repo):
    runner.main(cache_dir, out_dir, False, True, repositories_file, True)

    path = "{out_dir}/{repo_name}/mobile-metrics/all_probes".format(
           out_dir=out_dir,
           repo_name=normal_repo_name)

    with open(path, 'r') as data:
        scalars = json.load(data)

    # there are 2 scalars
    assert len(scalars) == 2

    bool_id = 'scalar/example.boolean_kind'
    str_id = 'scalar/example.string_kind'
    # they each have one definition
    assert len(scalars[bool_id][HISTORY_KEY][normal_repo_name]) == 1
    assert len(scalars[str_id][HISTORY_KEY][normal_repo_name]) == 1

    # this was in 2 commits
    assert len(set(scalars[bool_id][HISTORY_KEY][normal_repo_name][0][COMMITS_KEY].values())) == 1

    # this was in 1 commit
    assert len(set(scalars[str_id][HISTORY_KEY][normal_repo_name][0][COMMITS_KEY].values())) == 2

    # There should have been no errors
    assert not os.path.exists(EMAIL_FILE)


def test_improper_scalar_repo(improper_scalar_repo):
    runner.main(cache_dir, out_dir, False, True, repositories_file, True)

    # should be no output, since it was an improper file
    scalar_path = "{out_dir}/{repo_name}/mobile-metrics/all_probes".format(
                  out_dir=out_dir,
                  repo_name=normal_repo_name)

    assert not os.path.exists(scalar_path)

    with open(EMAIL_FILE, 'r') as email_file:
        emails = yaml.load(email_file)

    # should send 1 email
    assert len(emails) == 1
