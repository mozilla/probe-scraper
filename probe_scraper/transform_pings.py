# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from datetime import datetime

DATES_KEY = "dates"
COMMITS_KEY = "git-commits"
HISTORY_KEY = "history"
NAME_KEY = "name"
TYPE_KEY = "type"


def pretty_ts(ts):
    return datetime.utcfromtimestamp(ts).isoformat(' ')


def make_ping_defn(definition, commit, commit_timestamps):
    if COMMITS_KEY not in definition:
        # This is the first time we've seen this definition
        definition[COMMITS_KEY] = {
            "first": commit,
            "last": commit
        }
        definition[DATES_KEY] = {
            "first": pretty_ts(commit_timestamps[commit]),
            "last": pretty_ts(commit_timestamps[commit])
        }
    else:
        # we've seen this definition, update the `last` commit
        definition[COMMITS_KEY]["last"] = commit
        definition[DATES_KEY]["last"] = pretty_ts(commit_timestamps[commit])

    return definition


def pings_equal(def1, def2):
    return all((
        def1.get(l) == def2.get(l)
        for l in {
            'bugs',
            'data_reviews',
            'description',
            'notification_emails',
            'include_client_id',
            'send_if_empty',
        }
     ))


def update_or_add_ping(repo_pings, commit_hash, ping, definition, commit_timestamps):
    # If we've seen this ping before, check previous definitions
    if ping in repo_pings:
        prev_defns = repo_pings[ping][HISTORY_KEY]
        max_defn_i = max(range(len(prev_defns)),
                         key=lambda i: datetime.fromisoformat(prev_defns[i][DATES_KEY]["last"]))
        max_defn = prev_defns[max_defn_i]

        # If equal to previous commit, update date and commit on existing definition
        if pings_equal(definition, max_defn):
            new_defn = make_ping_defn(max_defn, commit_hash, commit_timestamps)
            repo_pings[ping][HISTORY_KEY][max_defn_i] = new_defn

        # Otherwise, prepend changed definition for existing ping
        else:
            new_defn = make_ping_defn(definition, commit_hash, commit_timestamps)
            repo_pings[ping][HISTORY_KEY] = prev_defns + [new_defn]

    # We haven't seen this ping before, add it
    else:
        defn = make_ping_defn(definition, commit_hash, commit_timestamps)
        repo_pings[ping] = {
            NAME_KEY: ping,
            HISTORY_KEY: [defn]
        }

    return repo_pings


def transform_by_hash(commit_timestamps, ping_data):
    """
    :param commit_timestamps - of the form
      <repo_name>: {
        <commit-hash>: <commit-timestamp>,
        ...
      }

    :param ping_data - of the form
      <repo_name>: {
        <commit-hash>: {
          <ping-name>: {
            ...
          },
        },
        ...
      }

    Outputs deduplicated data of the form
        <repo_name>: {
            <ping_name>: {
                "type": <type>,
                "name": <name>,
                "history": [
                    {
                        "bugs": [<bug#>, ...],
                        ...other pings.yaml info...,
                        "git-commits": {
                            "first": <hash>,
                            "last": <hash>
                        },
                        "dates": {
                            "first": <datetime>,
                            "last": <datetime>
                        }
                    },
                ]
            }
        }
    """

    all_pings = {}
    for repo_name, commits in ping_data.items():
        repo_pings = {}

        # iterate through commits, sorted by timestamp of the commit
        sorted_commits = sorted(iter(commits.items()),
                                key=lambda x_y: commit_timestamps[repo_name][x_y[0]])

        for commit_hash, pings in sorted_commits:
            for ping, definition in pings.items():
                repo_pings = update_or_add_ping(repo_pings,
                                                commit_hash,
                                                ping,
                                                definition,
                                                commit_timestamps[repo_name])

        all_pings[repo_name] = repo_pings

    return all_pings
