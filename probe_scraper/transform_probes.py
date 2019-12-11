# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from collections import defaultdict
from datetime import datetime

DATES_KEY = "dates"
COMMITS_KEY = "git-commits"
HISTORY_KEY = "history"
NAME_KEY = "name"
TYPE_KEY = "type"


def is_test_probe(probe_type, name):
    if probe_type == 'histogram':
        # These are test-only probes and never sent out.
        return name.startswith("TELEMETRY_TEST_")
    elif probe_type in ['scalar', 'event']:
        return name.startswith("telemetry.test.")

    return False


def get_from_nested_dict(dictionary, path, default=None):
    keys = path.split('/')
    for k in keys[:-1]:
        dictionary = dictionary[k]
    return dictionary.get(keys[-1], default)


def get_probe_id(probe_type, name):
    return probe_type + "/" + name


def probes_equal(probe1, probe2):
    props = [
        # Common.
        "cpp_guard",
        "optout",
        "notification_emails",
        # Histograms & scalars.
        "details/keyed",
        "details/kind",
        # Histograms.
        "details/n_buckets",
        "details/n_values",
        "details/low",
        "details/high",
        "details/record_in_processes",
        "details/labels",
        # Events.
        "details/methods",
        "details/objects",
        "details/extra_keys",
    ]

    for prop in props:
        if get_from_nested_dict(probe1, prop) != get_from_nested_dict(probe2, prop):
            return False
    return True


def extract_node_data(node_id, channel, probe_type, probe_data, result_data,
                      version, break_by_channel):
    """ Extract the probe data and group it by channel.

    :param node_id: the revision the probe data comes from, with th
    :param channel: the channel the probe was found in.
    :param probe_type: the probe type (e.g. 'histogram').
    :param probe_data: the probe data, with the following form:
            {
              node_id: {
                histogram: {
                  name: ...,
                  ...
                },
                scalar: {
                  ...
                },
              },
              ...
            }
    :param result_data: the dictionary to which the processed probe data is appended
           to. Extract probe data will be added to result_data in the form:
            {
              channel: {
                probe_id: {
                  type: 'histogram',
                  name: 'some-name',
                  history: {
                    channel: [
                      {
                        optout: True,
                        ...
                        revisions: {first: ..., last: ...},
                        versions: {first: ..., last: ...}
                      },
                      ...
                      ]
                    }
                }
              }
            }
    :param version: a human readable version string.
    :param break_by_channel: True if probe data for different channels needs to be
           stored separately, False otherwise. If True, probe data will be saved
           to result_data[channel] instead of just result_data.
    """
    for name, probe in probe_data.items():
        # Telemetrys test probes are never submitted to the servers.
        if is_test_probe(probe_type, name):
            continue

        storage = result_data
        if break_by_channel:
            if channel not in result_data:
                result_data[channel] = {}
            storage = result_data[channel]

        probe_id = get_probe_id(probe_type, name)
        if probe_id in storage and channel in storage[probe_id][HISTORY_KEY]:
            # If the probes state didn't change from the previous revision,
            # we just override with the latest state and continue.
            previous = storage[probe_id][HISTORY_KEY][channel][-1]
            if probes_equal(previous, probe):
                previous["revisions"]["first"] = node_id
                previous["versions"]["first"] = version
                continue

        if probe_id not in storage:
            storage[probe_id] = {
                TYPE_KEY: probe_type,
                NAME_KEY: name,
                HISTORY_KEY: {channel: []},
            }

        if channel not in storage[probe_id][HISTORY_KEY]:
            storage[probe_id][HISTORY_KEY][channel] = []

        probe["revisions"] = {
            "first": node_id,
            "last": node_id,
        }

        probe["versions"] = {
            "first": version,
            "last": version,
        }

        storage[probe_id][HISTORY_KEY][channel].append(probe)


def sorted_node_lists_by_channel(node_data):
    channels = defaultdict(list)
    for channel, nodes in node_data.items():
        for node_id, data in nodes.items():
            channels[channel].append({
                'node_id': node_id,
                'version': data['version'],
            })

    for channel, data in channels.items():
        channels[channel] = sorted(data, key=lambda n: int(n["version"]), reverse=True)

    return channels


def sorted_node_lists_by_date(node_data, revision_dates):
    def get_date(revision):
        return revision_dates[channel][revision]["date"]

    channels = defaultdict(list)
    for channel, nodes in node_data.items():
        for node_id, data in nodes.items():
            channels[channel].append({
                'node_id': node_id,
                'version': data['version'],
            })

    for channel, data in channels.items():
        channels[channel] = sorted(data, key=lambda x: get_date(x["node_id"]), reverse=True)

    return channels


def transform(probe_data, node_data, break_by_channel, revision_dates=None):
    """ Transform the probe data into the final format.

    :param probe_data: the preprocessed probe data.
    :param node_data: the raw probe data.
    :param break_by_channel: True if we want the probe output grouped by
           release channel.
    :param revision_dates: (optional) A dictionary of channel-revisions
           and their publish date, used to sort the revisions
    """
    if revision_dates is None:
        channels = sorted_node_lists_by_channel(node_data)
    else:
        channels = sorted_node_lists_by_date(node_data, revision_dates)

    result_data = {}
    for channel, channel_data in channels.items():
        print("\n" + channel + " - transforming probe data:")
        for entry in channel_data:
            node_id = entry['node_id']

            readable_version = str(entry["version"])
            print("  from: " + str({"node": node_id, "version": readable_version}))
            for probe_type, probes in probe_data[channel][node_id].items():
                # Group the probes by the release channel, if requested
                extract_node_data(node_id, channel, probe_type, probes, result_data,
                                  readable_version, break_by_channel)

    return result_data


def get_minimum_date(probe_data, revision_data, revision_dates):
    probe_histories = transform(probe_data, revision_data, break_by_channel=True,
                                revision_dates=revision_dates)
    min_dates = defaultdict(lambda: defaultdict(str))

    for channel, probes in probe_histories.items():
        for probe_id, entry in probes.items():
            dates = []
            for history in entry['history'][channel]:
                revision = history['revisions']['first']
                dates.append(revision_dates[channel][revision]["date"])
            min_dates[probe_id][channel] = min(dates)

    return min_dates


def pretty_ts(ts):
    return datetime.utcfromtimestamp(ts).isoformat(' ')


def make_metric_defn(definition, commit, commit_timestamps):
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


def metrics_equal(def1, def2):
    return all((
        def1.get(l) == def2.get(l)
        for l in {
            'bugs',
            'data_reviews',
            'description',
            'disabled',
            'labeled',
            'labels',
            'lifetime',
            'notification_emails',
            'send_in_pings',
            'time_unit',
            'type',
            'version',
        }
     ))


def metric_ctor(defn, metric):
    return {
        TYPE_KEY: defn[TYPE_KEY],
        NAME_KEY: metric,
        HISTORY_KEY: [defn]
    }


def update_or_add_metric(repo_metrics, commit_hash, metric, definition, commit_timestamps, type_ctor):
    # If we've seen this metric before, check previous definitions
    if metric in repo_metrics:
        prev_defns = repo_metrics[metric][HISTORY_KEY]
        max_defn_i = max(range(len(prev_defns)),
                         key=lambda i: datetime.fromisoformat(prev_defns[i][DATES_KEY]["last"]))
        max_defn = prev_defns[max_defn_i]

        # If equal to previous commit, update date and commit on existing definition
        if metrics_equal(definition, max_defn):
            new_defn = make_metric_defn(max_defn, commit_hash, commit_timestamps)
            repo_metrics[metric][HISTORY_KEY][max_defn_i] = new_defn

        # Otherwise, prepend changed definition for existing metric
        else:
            new_defn = make_metric_defn(definition, commit_hash, commit_timestamps)
            repo_metrics[metric][HISTORY_KEY] = prev_defns + [new_defn]

    # We haven't seen this metric before, add it
    else:
        defn = make_metric_defn(definition, commit_hash, commit_timestamps)
        repo_metrics[metric] = type_ctor(defn, metric)

    return repo_metrics


def transform_by_hash(commit_timestamps, metric_data):
    """
    :param commit_timestamps - of the form
      <repo_name>: {
        <commit-hash>: <commit-timestamp>,
        ...
      }

    :param metric_data - of the form
      <repo_name>: {
        <commit-hash>: {
          <metric-name>: {
            ...
          },
        },
        ...
      }

    Outputs deduplicated data of the form
        <repo_name>: {
            <metric_name>: {
                "type": <type>,
                "name": <name>,
                "history": [
                    {
                        "bugs": [<bug#>, ...],
                        ...other metrics.yaml info...,
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

    all_metrics = {}
    for repo_name, commits in metric_data.items():
        repo_metrics = {}

        # iterate through commits, sorted by timestamp of the commit
        sorted_commits = sorted(iter(commits.items()),
                                key=lambda x_y: commit_timestamps[repo_name][x_y[0]])

        for commit_hash, metrics in sorted_commits:
            for metric, definition in metrics.items():
                repo_metrics = update_or_add_metric(repo_metrics,
                                                    commit_hash,
                                                    metric,
                                                    definition,
                                                    commit_timestamps[repo_name],
                                                    metric_ctor)

        all_metrics[repo_name] = repo_metrics

    return all_metrics
