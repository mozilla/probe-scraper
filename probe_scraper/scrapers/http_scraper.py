# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
import os
import re
import requests
import tempfile
import traceback
import yaml


DATE_REGEX = "^\d{8}$"
REPOSITORIES_FILENAME = "repositories.yaml"
METRIC_KEYS = ["histogram", "scalar", "event"]
URI_KEY = "base_url"
ALERT_EMAILS_KEY = "alert_emails"


def load_repos():
    with open(REPOSITORIES_FILENAME, 'r') as f:
        repos = yaml.load(f)
    return repos


def download_files(date, repo_name, repo_info, cache_dir):
    results = defaultdict(list)
    date_path = os.path.join(cache_dir, repo_name, date)
    base_uri = repo_info[URI_KEY]

    all_files = [(k, x) for k in METRIC_KEYS for x in repo_info.get(k, [])]
    for (ptype, rel_path) in all_files:
        disk_path = os.path.join(date_path, rel_path)
        if not os.path.exists(disk_path):
            uri = base_uri + '/' + rel_path

            req = requests.get(uri)
            if req.status_code != requests.codes.ok:
                raise Exception(
                    ("Repository {repo_name} failed on retrieval of {ptype}s."
                     " Request returned status {status} for {uri}").format(
                     repo_name=repo_name, ptype=ptype, status=str(req.status_code), uri=uri)
                )

            dir = os.path.split(disk_path)[0]
            if not os.path.exists(dir):
                os.makedirs(dir)
            with open(disk_path, 'wb') as f:
                for chunk in req.iter_content(chunk_size=128):
                    f.write(chunk)

        results[ptype].append(disk_path)

    return results


def find_existing_files(repo_name, repo_info, cache_dir):
    """
    Returns data in the format:
    {
        repo_name: {
            historical_date: {
                histogram: [path, ...],
                scalar: [path, ...],
                event: [path, ...]
            }
            ...
        }
    }
    """
    repo_path = os.path.join(cache_dir, repo_name)

    if not os.path.exists(repo_path):
        return {}

    # Get dates of previously retrieved files
    dates = [f for f in os.listdir(repo_path) if not os.path.isfile(f) and re.match(DATE_REGEX, f)]

    # Return data that exists:
    #  - dates if the date has files
    #  - keys if the repo contains that key
    #  - files in those keys
    # If a key is removed from the repo definition, the historical data will no longer be available
    return {
        repo_name: {
            date: {
                key: [
                    os.path.join(repo_path, date, rel_path)
                    for rel_path in repo_info[key]
                    if os.path.isfile(os.path.join(repo_path, date, rel_path))
                ] for key in METRIC_KEYS if repo_info.get(key)
            } for date in dates if os.listdir(os.path.join(repo_path, f))
        }
    }


def scrape(date, folder=None):
    """
    Returns data in the format:
    {
      repo: {
        date: {
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
    repos = load_repos()
    emails = {}

    for repo_name, repo_info in repos.iteritems():
        print "\n" + repo_name + " - loading historical files"

        results[repo_name] = {}
        emails[repo_name] = {"addresses": repo_info["notification_emails"], "emails": []}
        results.update(find_existing_files(repo_name, repo_info, folder))

        print "\n" + repo_name + " - loading files"
        try:
            results[repo_name][date] = download_files(date, repo_name, repo_info, folder)
        except Exception:
            emails[repo_name]["emails"].append({
                "subject": "Probe Scraper: Failed Probe Import",
                "message": traceback.format_exc()
            })

    return results, emails
