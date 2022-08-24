# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import datetime
import os
import time
import unittest.mock
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from git import Head, Repo

from probe_scraper import glean_push


@contextmanager
def pushd(path: Path):
    cwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(cwd)


@pytest.fixture(autouse=True)
def empty_output_bucket():
    with unittest.mock.patch.dict(os.environ, {"OUTPUT_BUCKET": ""}):
        yield


@pytest.fixture
def test_dir(tmp_path_factory) -> Path:
    # Where we will build the test git repo
    return tmp_path_factory.mktemp("test_git_repositories")


@pytest.fixture
def repositories_file(test_dir: Path) -> Path:
    # Where we will write the repositories file
    return test_dir / "repositories.yaml"


def generate_repo(
    test_dir: Path,
    repo_name: str,
    branch: str = "main",
    num_commits: int = 1,
    base_dir: Path = Path("tests/resources/test_repo_files"),
) -> Path:
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


def test_missing_metrics_file(test_dir: Path, repositories_file: Path):
    repo_path = generate_repo(test_dir, "normal")
    commit = Repo(repo_path).head.commit.hexsha
    data = {"url": str(repo_path), "commit": commit, "branch": ""}
    request = Mock(get_json=Mock(return_value=data))

    repositories_info = {
        "version": "2",
        "libraries": [],
        "applications": [
            {
                "app_name": "example",
                "canonical_app_name": "Example",
                "app_description": "foo",
                "url": str(repo_path),
                "notification_emails": ["nobody@example.com"],
                "metrics_files": ["missing/metrics.yaml"],
                "channels": [
                    {
                        "v1_name": "example",
                        "app_id": "app-id",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }
    repositories_file.write_text(yaml.dump(repositories_info))
    with pushd(repositories_file.parent):
        response = glean_push.main(request)
    assert response.status_code == 400
    assert (
        response.data.decode()
        == f"Error: missing/metrics.yaml not found in commit {commit} for app-id\n"
    )

    repositories_info["applications"][0]["deprecated"] = True
    repositories_file.write_text(yaml.dump(repositories_info))
    with pushd(repositories_file.parent):
        response = glean_push.main(request)
    assert response.status_code == 200
    assert response.data.decode() == f"update is valid, but not published\n"
