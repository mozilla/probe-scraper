# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import tempfile
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import git

from probe_scraper.parsers.repositories import Repository

GIT_HASH_PATTERN = re.compile("([A-Fa-f0-9]){40}")

# WARNING!
# Changing these dates can cause files that had metrics to
# stop being scraped. When the probe-info-service
# stops reporting those files, the schema-generator
# will not add them to the schemas, resulting in a
# schema-incompatible change that breaks the pipeline.
FENIX_DATE = "2019-06-04 00:00:00"
MIN_DATES = {
    # Previous versions of the file were not schema-compatible
    "glean": "2019-04-11 00:00:00",
    "fenix": FENIX_DATE,
    "fenix-nightly": FENIX_DATE,
    "firefox-android-nightly": FENIX_DATE,
    "firefox-android-beta": FENIX_DATE,
    "firefox-android-release": FENIX_DATE,
    "reference-browser": "2019-04-01 00:00:00",
    "firefox-desktop": "2020-07-29 00:00:00",
    "glean-js": "2020-09-21 13:35:00",
    "mozilla-vpn": "2021-05-25 00:00:00",
    "mozilla-vpn-android": "2021-05-25 00:00:00",
    "rally-markup-fb-pixel-hunt": "2021-12-04 00:00:00",
    "rally-citp-search-engine-usage": "2022-04-15 00:00:00",
}

# Some commits in projects might contain invalid metric files.
# When we know these problems are fixed in later commits we can skip them.
SKIP_COMMITS = {
    "engine-gecko": [
        "9bd9d7fa6c679f35d8cbeb157ff839c63b21a2e6"  # Missing schema update from v1 to v2
    ],
    "engine-gecko-beta": [
        "9bd9d7fa6c679f35d8cbeb157ff839c63b21a2e6"  # Missing schema update from v1 to v2
    ],
    "gecko": [
        "43d8cf138695faae2fca0adf44c94f47fdadfca8",  # Missing gfx/metrics.yaml
        "340c8521a54ad4d4a32dd16333676a6ff85aaec2",  # Missing toolkit/components/glean/pings.yaml
        "4520632fe0664572c5f70688595b7721d167e2d0",  # Missing toolkit/components/glean/pings.yaml
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
    ],
    "firefox-desktop": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        "3e81d4efd88a83e89da56b690f39ca2a78623810",  # No browser/components/newtab/metrics.yaml
    ],
    "firefox-desktop-background-update": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
    ],
    "firefox-translations": [
        # Invalid extension/model/telemetry/metrics.yaml
        "02dc27b663178746499d092a987ec08c026ee560",
    ],
    "pine": [
        "c5d5f045aaba41933622b5a187c39da0d6ab5d80",  # Missing toolkit/components/glean/tags.yaml
        "3e81d4efd88a83e89da56b690f39ca2a78623810",  # No browser/components/newtab/metrics.yaml
    ],
    "rally-core": [
        "4df4dc23317e155bf1b605d04b466c27d78537fa",  # Missing web-platform/glean/metrics.yaml
        "69559324f775b79c9a39c6a95fdb3657c184ed0e",  # Bug 1769579 omit deleted onboarding ping
        "f633df7676b6ef64e496fea1b3687eff22680d49",  # Missing web-platform/glean/pings.yaml
    ],
}


class InvalidCommitError(ValueError):
    pass


def _file_in_commit(repo: git.Repo, filename: Path, ref: str) -> bool:
    # adapted from https://stackoverflow.com/a/25961128
    subtree = repo.commit(ref).tree
    for path_element in filename.parts[:-1]:
        try:
            subtree = subtree[path_element]
        except KeyError:
            return False  # subdirectory not in tree
    return str(filename) in subtree


def get_commits(
    repo: git.Repo, filename: Path, ref: str, max_count: Optional[int] = None
) -> Dict[str, Tuple[int, int]]:
    sep = ":"
    log_format = f"--format=%H{sep}%ct"
    # include "--" to prevent error for filename not in current tree
    args = [ref, log_format, "--", filename]
    if max_count is not None:
        args = [f"--max-count={max_count}"] + args
    log = repo.git.log(args)
    # filter out empty strings
    change_commits = filter(None, log.split("\n"))
    commits = set(enumerate(change_commits))
    if max_count is None and _file_in_commit(repo, filename, ref):
        # include ref when it contains filename
        commits |= set(
            enumerate(repo.git.log(ref, "--max-count=1", log_format).split("\n"))
        )

    # Store the index in the ref-log as well as the timestamp, so that the
    # ordering of commits will be deterministic and always in the correct
    # order.
    result = {}
    for index, entry in commits:
        commit, timestamp = entry.split(sep)
        result[commit] = (int(timestamp), index)

    return result


def get_file_at_hash(repo: git.Repo, _hash: str, filename: Path) -> str:
    return repo.git.show(f"{_hash}:{filename}")


def utc_timestamp(d: datetime) -> float:
    # See https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
    # for why we're calculating this UTC timestamp explicitly
    return (d - datetime(1970, 1, 1)) / timedelta(seconds=1)


def retrieve_files(
    repo_info: Repository,
    cache_dir: Path,
    commit: Optional[str] = None,
    commit_branch: Optional[str] = None,
) -> Tuple[Dict[str, Tuple[int, int]], Dict[str, List[Path]], bool]:
    results = defaultdict(list)
    timestamps = dict()
    base_path = cache_dir / repo_info.name
    org_name, repo_name = repo_info.url.rstrip("/").split("/")[-2:]
    repo_path = cache_dir / org_name / f"{repo_name}.git"

    min_date = None
    if repo_info.name in MIN_DATES:
        min_date = utc_timestamp(datetime.fromisoformat(MIN_DATES[repo_info.name]))

    skip_commits = SKIP_COMMITS.get(repo_info.name, [])

    if repo_path.exists():
        print(f"Pulling commits into {repo_path}")
        repo = git.Repo(repo_path)
        if set(repo.remote("origin").urls) != {repo_info.url}:
            raise Exception(
                f"invalid cache: git repo at {repo_path} is not for {repo_info.url}"
            )
    else:
        print(f"Cloning {repo_info.url} into {repo_path}")
        repo = git.Repo.clone_from(
            repo_info.url, repo_path, bare=True, depth=1 if commit else None
        )

    repo_is_shallow = repo.git.rev_parse(is_shallow_repository=True) == "true"
    branch = repo_info.branch or repo.active_branch
    if commit is None:
        repo.git.fetch(
            "origin", f"{branch}:{branch}", force=True, unshallow=repo_is_shallow
        )
        # pass ref around to avoid updating repo.active_branch, so that it
        # can be preserved for other glean repos with the same git url
        ref = f"refs/heads/{branch}"
        upload_repo = True
    elif GIT_HASH_PATTERN.fullmatch(commit) is None:
        raise InvalidCommitError("must be full length git hash")
    else:
        repo.git.fetch(
            "origin", commit, force=True, depth=1 if repo_is_shallow else None
        )
        ref = commit
        upload_repo = str(branch) == commit_branch
        # When commit_branch is the branch for this repo, verify that commit is on that branch.
        if upload_repo:
            print(f"Verifying that {commit} is in {branch}")
            # doesn't change depth
            repo.git.fetch("origin", f"{branch}:{branch}", force=True)
            branch_ref = f"refs/heads/{branch}"
            if commit != repo.commit(branch_ref).hexsha:
                if repo_is_shallow:
                    repo.git.fetch(
                        "origin", f"{branch}:{branch}", force=True, unshallow=True
                    )
                try:
                    # when commit != branch, check if it's in the history for branch
                    repo.git.merge_base(commit, branch_ref, is_ancestor=True)
                except git.GitCommandError:
                    raise InvalidCommitError(
                        f"Commit {commit} not found in branch {branch} of {repo_info.url}"
                    )

    for rel_path in map(Path, repo_info.get_change_files()):
        hashes = get_commits(repo, rel_path, ref, max_count=1 if commit else None)
        for _hash, (ts, index) in hashes.items():
            if min_date and ts < min_date:
                continue
            if _hash in skip_commits:
                continue

            disk_path = base_path / _hash / rel_path
            if not disk_path.exists():
                contents = get_file_at_hash(repo, _hash, rel_path)

                disk_path.parent.mkdir(parents=True, exist_ok=True)
                disk_path.write_bytes(contents.encode("UTF-8"))

            results[_hash].append(disk_path)
            timestamps[_hash] = (ts, index)

    return timestamps, results, upload_repo


def scrape(
    folder: Optional[Path] = None,
    repos: Optional[List[Repository]] = None,
    commit: Optional[str] = None,
    commit_branch: Optional[str] = None,
) -> Tuple[
    Dict[str, Dict[str, Tuple[int, int]]],
    Dict[str, Dict[str, List[Path]]],
    Dict[str, Dict[str, List[Union[Dict[str, str], str]]]],
    List[str],
]:
    """
    Returns four data structures. The first is the commit timestamps:
    {
      <repo-name>: {
        <commit-hash>: (<commit-timestamp>, <index>)
      }
    }

    Since commits from the same PR may have the save timestamp, we also return
    an index representing its position in the git log so the correct ordering
    of commits can be preserved.

    The second is the probe data:
    {
      <repo-name>: {
        <commit-hash>: [<path>, ...],
      },
    }

    The third is emails:
    {
      <repo-name>: {
        "addresses": [<email>, ...].
        "emails": [
          {
            "subject": <str>,
            "message": <str>,
          },
        ]
      },
    }

    The fourth is the names of repos that are authorized to be uploaded, based on
    whether commit_branch matches the configured branch for that repo. When commit is
    not None but commit_branch is None, this is empty. When commit and commit_branch are
    both None, this includes all repos:
    [<repo-name>, ...]

    Raises InvalidCommitError when commit is not None or a 40 character hex sha.

    Also raises InvalidCommitError when commit and commit_branch are both specified and
    commit_branch matches the configured branch for a repo and commit is not part of the
    history of commit_branch for that repo. This ensures that return values correctly
    indicate repos where commits are authorized to be uploaded.
    """
    if folder is None:
        folder = Path(tempfile.mkdtemp())

    results = {}
    timestamps = {}
    emails = {}
    upload_repos = []

    for repo_info in repos:
        print("Getting commits for repository " + repo_info.name)

        results[repo_info.name] = {}
        emails[repo_info.name] = {
            "addresses": repo_info.notification_emails,
            "emails": [],
        }

        try:
            ts, commits, upload_repo = retrieve_files(
                repo_info, folder, commit, commit_branch
            )
            print("  Got {} commits".format(len(commits)))
            results[repo_info.name] = commits
            timestamps[repo_info.name] = ts
            if upload_repo:
                upload_repos.append(repo_info.name)
        except Exception:
            raise
            emails[repo_info.name]["emails"].append(
                {
                    "subject": "Probe Scraper: Failed Probe Import",
                    "message": traceback.format_exc(),
                }
            )

    return timestamps, results, emails, upload_repos
