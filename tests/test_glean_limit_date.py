# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import git
import pytest
import yaml
from git import Head, Repo

import probe_scraper.runner


@pytest.fixture
def test_dir(tmp_path_factory) -> Path:
    # Where we will build the test git repo
    return tmp_path_factory.mktemp("test_git_repositories")


def generate_repo(
    test_dir: Path,
    repo_name: str,
    branch: str = "main",
    skip_commits: int = 0,
    num_commits: int = 1,
    base_dir: Path = Path("tests/resources/test_repo_files"),
    base_datetime: datetime = datetime.utcnow(),
) -> Path:
    directory = test_dir / f"{repo_name}-{uuid4().hex}"
    repo = Repo.init(directory)
    # Ensure the default branch is using a fixed name.
    # User config could change that,
    # breaking tests with implicit assumptions further down the line.
    repo.head.reference = Head(repo, f"refs/heads/{branch}")

    base_path = base_dir / repo_name
    for i in range(skip_commits, skip_commits + num_commits):
        files_dir = base_path / str(i)
        if not files_dir.exists():
            break

        for path in files_dir.iterdir():
            print(f"Copying file {path.name}")
            destination = directory / path.name
            destination.write_bytes(path.read_bytes())

        repo.index.add("*")
        # We need to synthesize the timestamps of commits to each be a second
        # apart, otherwise the commits may be at exactly the same second, which
        # means they won't always sort in order, and thus the merging of identical
        # metrics in adjacent commits may not happen correctly.
        commit_date = f"{base_datetime + timedelta(seconds=i):%Y-%m-%dT%H:%M:%S}"
        repo.index.commit(f"Commit {i}", commit_date=commit_date)

    return directory


def test_single_commit(test_dir: Path):
    today_date = datetime.utcnow().date()
    today_datetime = datetime(*today_date.timetuple()[:3])
    repo_path = generate_repo(
        test_dir,
        "normal",
        num_commits=2,
        # each commit after the first adds 1 second to base_datetime, so setting
        # glean_limit_date=today_date and base_datetime to 1 second before that will
        # only collect the second commit
        base_datetime=today_datetime - timedelta(seconds=1),
    )

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
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": "example",
                        "app_id": "example",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }
    repositories_file = test_dir / "repositories.yaml"
    repositories_file.write_text(yaml.dump(repositories_info))

    # generate output with date limit
    actual_kwargs = dict(
        cache_dir=test_dir / "cache",
        out_dir=test_dir / "actual",
        firefox_version=None,
        min_firefox_version=None,
        process_moz_central_probes=False,
        process_glean_metrics=True,
        repositories_file=repositories_file,
        dry_run=True,
        glean_repos=None,
        firefox_channel=None,
        output_bucket="",
        cache_bucket=None,
        env="dev",
        bugzilla_api_key=None,
        glean_urls=[str(repo_path)],
        glean_commit=None,
        glean_commit_branch=None,
        email_file=test_dir / "emails.txt",
        update=True,
        glean_limit_date=today_date,
    )
    probe_scraper.runner.main(**actual_kwargs)

    # shallow clone repo with single commit to generate expected output
    original_repo_path = repo_path.parent / f"{repo_path.name}-original"
    os.rename(repo_path, original_repo_path)
    # must use file:// or git will ignore --depth
    git.Repo.clone_from(f"file://{original_repo_path.absolute()}", repo_path, depth=1)
    expect_kwargs = {
        **actual_kwargs,
        "update": False,
        "out_dir": test_dir / "expect",
        "glean_limit_date": None,
    }
    probe_scraper.runner.main(**expect_kwargs)
    # validate
    expect_metrics = json.loads(
        (test_dir / "expect" / "glean" / "example" / "metrics").read_text()
    )
    actual_metrics = json.loads(
        (test_dir / "actual" / "glean" / "example" / "metrics").read_text()
    )
    assert expect_metrics == actual_metrics


def test_add_commit(test_dir: Path):
    today_date = datetime.utcnow().date()
    today_datetime = datetime(*today_date.timetuple()[:3])
    repo_path = generate_repo(
        test_dir,
        "normal",
        num_commits=2,
        # each commit after the first adds 1 second to base_datetime, so setting
        # glean_limit_date=today_date and base_datetime to 1 second before that will
        # only collect the second commit
        base_datetime=today_datetime - timedelta(seconds=1),
    )

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
                "metrics_files": ["metrics.yaml"],
                "channels": [
                    {
                        "v1_name": "example",
                        "app_id": "example",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }
    repositories_file = test_dir / "repositories.yaml"
    repositories_file.write_text(yaml.dump(repositories_info))

    # generate expected output without date limit
    expect_kwargs = dict(
        cache_dir=test_dir / "cache",
        out_dir=test_dir / "expect",
        firefox_version=None,
        min_firefox_version=None,
        process_moz_central_probes=False,
        process_glean_metrics=True,
        repositories_file=repositories_file,
        dry_run=True,
        glean_repos=None,
        firefox_channel=None,
        output_bucket="",
        cache_bucket=None,
        env="dev",
        bugzilla_api_key=None,
        glean_urls=[str(repo_path)],
        glean_commit=None,
        glean_commit_branch=None,
        email_file=test_dir / "emails.txt",
        update=False,
        glean_limit_date=None,
    )
    probe_scraper.runner.main(**expect_kwargs)

    # clone repo with only first commit to initialize state before updating
    actual_kwargs = {**expect_kwargs, "out_dir": test_dir / "actual"}
    original_repo_path = repo_path.parent / f"{repo_path.name}-original"
    os.rename(repo_path, original_repo_path)
    repo = git.Repo.clone_from(original_repo_path, repo_path)
    repo.git.reset("HEAD~", hard=True)
    probe_scraper.runner.main(**actual_kwargs)
    # validate files are initially different
    expect_metrics = json.loads(
        (test_dir / "expect" / "glean" / "example" / "metrics").read_text()
    )
    actual_metrics = json.loads(
        (test_dir / "actual" / "glean" / "example" / "metrics").read_text()
    )
    assert expect_metrics != actual_metrics

    # update with second commit and date limit
    repo.git.pull()
    actual_kwargs["update"] = True
    actual_kwargs["glean_limit_date"] = today_date
    probe_scraper.runner.main(**actual_kwargs)
    # validate files are now equivalent
    expect_metrics = json.loads(
        (test_dir / "expect" / "glean" / "example" / "metrics").read_text()
    )
    for metric in expect_metrics:
        for element in expect_metrics[metric]["history"]:
            for index in ("first", "last"):
                # reflog index is expected to be inaccurate in update mode
                element["reflog-index"][index] = 0
    actual_metrics = json.loads(
        (test_dir / "actual" / "glean" / "example" / "metrics").read_text()
    )
    assert expect_metrics == actual_metrics
