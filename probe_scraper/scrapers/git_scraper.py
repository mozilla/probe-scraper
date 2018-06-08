# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from git import Repo
import os
import shutil
import tempfile
import traceback


def get_commits(repo, filename):
    sep = ":"
    commits = repo.git.log('--format="%H{}%ct"'.format(sep), filename)
    return dict((c.strip('"').encode('ascii').split(sep) for c in commits.split("\n")))


def get_file_at_hash(repo, _hash, filename):
    return repo.git.show("{hash}:{path}".format(hash=_hash, path=filename))


def retrieve_files(repo_info, cache_dir):
    results = defaultdict(lambda: defaultdict(list))
    timestamps = dict()
    base_path = os.path.join(cache_dir, repo_info.name)
    all_files = repo_info.get_probe_paths()

    if os.path.exists(repo_info.name):
        shutil.rmtree(repo_info.name)
    repo = Repo.clone_from(repo_info.url, repo_info.name)

    try:
        for (ptype, rel_path) in all_files:
            hashes = get_commits(repo, rel_path)
            for _hash, ts in hashes.iteritems():
                disk_path = os.path.join(base_path, _hash, rel_path)
                if not os.path.exists(disk_path):
                    contents = get_file_at_hash(repo, _hash, rel_path)

                    dir = os.path.split(disk_path)[0]
                    if not os.path.exists(dir):
                        os.makedirs(dir)
                    with open(disk_path, 'wb') as f:
                        f.write(contents)

                results[_hash][ptype].append(disk_path)
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
        <commit-hash>: {
          histogram: [path, ...]
          event: [path, ...]
          scalar: [path, ...]
        },
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
        results[repo_info.name] = {}
        emails[repo_info.name] = {"addresses": repo_info.notification_emails, "emails": []}

        try:
            ts, commits = retrieve_files(repo_info, folder)
            results[repo_info.name] = commits
            timestamps[repo_info.name] = ts
        except Exception:
            emails[repo_info.name]["emails"].append({
                "subject": "Probe Scraper: Failed Probe Import",
                "message": traceback.format_exc()
            })

    return timestamps, results, emails
