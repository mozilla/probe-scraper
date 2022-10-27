from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pytest

from probe_scraper import fog_checks, transform_probes
from probe_scraper.parsers.repositories import Repository
from probe_scraper.scrapers.git_scraper import Commit

FAKE_METRIC = {
    "type": "string",
    "expires": "never",
    "notification_emails": ["bar@foo.com"],
    "bugs": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1701769"],
}

FAKE_REPO_META = {
    "notification_emails": ["foo@bar.com"],
}


@pytest.fixture
def fake_latest_nightly_version() -> str:
    return "100"


@pytest.fixture
def fake_metrics(fake_latest_nightly_version) -> Dict[str, Dict]:
    return {
        "category.name.metric_name": FAKE_METRIC,
        "expired.category.name.metric_name": {**FAKE_METRIC, "expires": "expired"},
    }


@pytest.fixture
def fake_commit_timestamp() -> int:
    return int(datetime.now().timestamp())


@pytest.fixture
def fake_metrics_by_commit(
    fake_commit_timestamp, fake_metrics
) -> Dict[str, Dict[str, Dict]]:
    return {
        Commit(
            hash="deadcode",
            timestamp=fake_commit_timestamp,
            reflog_index=0,
            is_head=True,
        ): {
            **fake_metrics,
            "newer.category.name.metric_name": FAKE_METRIC,
        },
        Commit(
            hash="decafcaf",
            timestamp=fake_commit_timestamp,
            reflog_index=1,
            is_head=False,
        ): fake_metrics,
    }


@pytest.fixture
def fake_commits(fake_commit_timestamp) -> Dict[Commit, List[Path]]:
    # `decafcaf` should remain the most recent SHA.
    return {
        Commit(
            hash="decafcaf",
            timestamp=fake_commit_timestamp,
            reflog_index=1,
            is_head=False,
        ): [],
        Commit(
            hash="deadcode",
            timestamp=fake_commit_timestamp,
            reflog_index=0,
            is_head=True,
        ): [],
    }


@pytest.fixture
def fake_metrics_by_repo_by_commit(
    fake_metrics_by_commit, fake_repos
) -> Dict[str, Dict[Commit, Dict[str, Dict]]]:
    return {
        repo.name: {
            commit: {
                f"{metric_name}_{repo.name}": metric
                for metric_name, metric in metrics.items()
            }
            for commit, metrics in fake_metrics_by_commit.items()
        }
        for repo in fake_repos
    }


@pytest.fixture
def fake_metrics_by_repo(
    fake_metrics_by_repo_by_commit,
) -> Dict[str, Dict[str, Dict[str, Dict]]]:
    return transform_probes.transform_metrics_by_hash(fake_metrics_by_repo_by_commit)


@pytest.fixture
def fake_repos() -> List[Repository]:
    return [
        Repository("glean-core", dict(FAKE_REPO_META, library_names=["glean-core"])),
        Repository("firefox-desktop", dict(FAKE_REPO_META, dependencies=["gecko"])),
        Repository("gecko", dict(FAKE_REPO_META, dependencies=["glean-core"])),
    ]


@pytest.fixture
def fake_commits_by_repo(
    fake_repos, fake_commits
) -> Dict[str, Dict[Commit, List[Path]]]:
    return {repo.name: fake_commits for repo in fake_repos}


def test_get_current_metrics(fake_metrics_by_repo):
    current_metrics_by_repo = fog_checks.get_current_metrics_by_repo(
        fake_metrics_by_repo
    )
    assert (
        "newer.category.name.metric_name_glean-core"
        in current_metrics_by_repo["glean-core"]
    )


def test_get_expiring_metrics(fake_metrics, fake_latest_nightly_version):
    expiring_metrics = fog_checks.get_expiring_metrics(
        {
            **fake_metrics,
            "expiring.metric_name": {
                **FAKE_METRIC,
                "expires": str(int(fake_latest_nightly_version) + 1),
            },
        },
        fake_latest_nightly_version,
    )
    assert "expired.category.name.metric_name" in expiring_metrics
    assert "expiring.metric_name" in expiring_metrics
    assert "category.name.metric_name" not in expiring_metrics


def test_fbagefem_does_nothing_with_no_fog_repos(fake_metrics_by_repo, fake_repos):
    fake_repos = [repo for repo in fake_repos if repo.name not in fog_checks.FOG_REPOS]
    fake_metrics_by_repo = {
        repo_name: metrics
        for repo_name, metrics in fake_metrics_by_repo.items()
        if repo_name not in fog_checks.FOG_REPOS
    }
    expiry_emails = fog_checks.file_bugs_and_get_emails_for_expiring_metrics(
        fake_repos, fake_metrics_by_repo, None, True
    )
    assert expiry_emails is None


@pytest.mark.web_dependency  # fbagefem gets the latest nightly version from product-info
def test_fbagefem_returns_emails_for_expiring_metrics(fake_metrics_by_repo, fake_repos):
    expiry_emails = fog_checks.file_bugs_and_get_emails_for_expiring_metrics(
        fake_repos,
        fake_metrics_by_repo,
        None,
        True,
    )
    for fog_repo in fog_checks.FOG_REPOS:
        assert f"expired_metrics_{fog_repo}" in expiry_emails
        assert len(expiry_emails[f"expired_metrics_{fog_repo}"]["emails"]) == 1


def test_bug_number_from_url():
    assert (
        fog_checks.bug_number_from_url(
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1701769"
        )
        == 1701769
    )
    # Parser isn't smart, but it's Best Effort anyway so just make sure it returns None
    assert (
        fog_checks.bug_number_from_url(
            "https://bugzilla.mozilla.org/show_bug.cgi?id=1701769#c1"
        )
        is None
    )
    assert fog_checks.bug_number_from_url("https://bugzil.la/1701769") is None
    # Parser shouldn't give a good number for github urls
    assert (
        fog_checks.bug_number_from_url(
            "https://github.com/mozilla/probe-scraper/pull/382"
        )
        is None
    )
