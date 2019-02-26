# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from git import Repo
import os
import shutil
import tempfile
import traceback
from datetime import datetime, timedelta


MIN_DATES = {
    # Previous versions of the file were not schema-compatible
    "glean": "2019-01-25 00:00:00"
}

def get_commits(repo, filename):
    sep = ":"
    commits = repo.git.log('--format="%H{}%ct"'.format(sep), filename)
    with_ts = dict((c.strip('"').split(sep) for c in commits.split("\n")))
    return {k: int(v) for k, v in with_ts.items()}


def get_file_at_hash(repo, _hash, filename):
    return repo.git.show("{hash}:{path}".format(hash=_hash, path=filename))


def retrieve_files(repo_info, cache_dir):
    results = defaultdict(list)
    timestamps = dict()
    base_path = os.path.join(cache_dir, repo_info.name)
    metric_files = repo_info.get_metrics_file_paths()

    min_date = None
    if repo_info.name in MIN_DATES:
        # See https://docs.python.org/3/library/datetime.html#datetime.datetime.timestamp
        # for why we're calculating this UTC timestamp explicitly
        min_date = (datetime.fromisoformat(MIN_DATES[repo_info.name]) - datetime(1970, 1, 1)) / timedelta(seconds=1)

    if os.path.exists(repo_info.name):
        shutil.rmtree(repo_info.name)
    repo = Repo.clone_from(repo_info.url, repo_info.name)

    try:
        for rel_path in metric_files:
            hashes = get_commits(repo, rel_path)
            for _hash, ts in hashes.items():
                if (min_date and ts < min_date):
                    continue 
                disk_path = os.path.join(base_path, _hash, rel_path)
                if not os.path.exists(disk_path):
                    contents = get_file_at_hash(repo, _hash, rel_path)

                    dir = os.path.split(disk_path)[0]
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    with open(disk_path, 'wb') as f:
                        f.write(contents.encode("UTF-8"))

                results[_hash].append(disk_path)
                timestamps[_hash] = ts
    except Exception:
        # without this, the error will be silently discarded
        raise
    finally:
        shutil.rmtree(repo_info.name)

    return timestamps, results


def scrape(folder=None, repos=None):
    """
    Returns two data structures. The first is the commit timestamps:
    {
        repo: {
            <commit-hash>: <commit-timestamp>
        }
    }

    The second is the probe data:
    {
      repo: {
        <commit-hash>: [path, ...],
        ...
      },
      ...
    }
    """
    if folder is None:
        folder = tempfile.mkdtemp()

    results = {}
    timestamps = {}
    emails = {}

    for repo_info in repos:
        print("Getting commits for repository " + repo_info.name)

        results[repo_info.name] = {}
        emails[repo_info.name] = {"addresses": repo_info.notification_emails, "emails": []}

        try:
            ts, commits = retrieve_files(repo_info, folder)
            print("  Got {} commits".format(len(commits)))
            results[repo_info.name] = commits
            timestamps[repo_info.name] = ts
        except Exception:
            emails[repo_info.name]["emails"].append({
                "subject": "Probe Scraper: Failed Probe Import",
                "message": traceback.format_exc()
            })

    return timestamps, results, emails
