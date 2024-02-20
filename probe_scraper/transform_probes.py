# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import copy
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from .scrapers.git_scraper import Commit

DATES_KEY = "dates"
COMMITS_KEY = "git-commits"
HISTORY_KEY = "history"
NAME_KEY = "name"
TYPE_KEY = "type"
REFLOG_KEY = "reflog-index"
IN_SOURCE_KEY = "in-source"
SOURCE_URL_KEY = "source_url"


def is_test_probe(probe_type, name):
    if probe_type == "histogram":
        # These are test-only probes and never sent out.
        return name.startswith("TELEMETRY_TEST_")
    elif probe_type in ["scalar", "event"]:
        return name.startswith("telemetry.test.")

    return False


def get_from_nested_dict(dictionary, path, default=None):
    keys = path.split("/")
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


def extract_node_data(
    node_id, channel, probe_type, probe_data, result_data, version, break_by_channel
):
    """Extract the probe data and group it by channel.

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

        probe = copy.deepcopy(probe)

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
            channels[channel].append(
                {
                    "node_id": node_id,
                    "version": data["version"],
                }
            )

    for channel, data in channels.items():
        channels[channel] = sorted(data, key=lambda n: int(n["version"]), reverse=True)

    return channels


def sorted_node_lists_by_date(node_data, revision_dates):
    def get_date(revision):
        return revision_dates[channel][revision]["date"]

    channels = defaultdict(list)
    for channel, nodes in node_data.items():
        for node_id, data in nodes.items():
            channels[channel].append(
                {
                    "node_id": node_id,
                    "version": data["version"],
                }
            )

    for channel, data in channels.items():
        channels[channel] = sorted(
            data, key=lambda x: get_date(x["node_id"]), reverse=True
        )

    return channels


def transform(probe_data, node_data, break_by_channel, revision_dates=None):
    """Transform the probe data into the final format.

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
            node_id = entry["node_id"]

            readable_version = str(entry["version"])
            print("  from: " + str({"node": node_id, "version": readable_version}))
            for probe_type, probes in probe_data[channel][node_id].items():
                # Group the probes by the release channel, if requested
                extract_node_data(
                    node_id,
                    channel,
                    probe_type,
                    probes,
                    result_data,
                    readable_version,
                    break_by_channel,
                )

    return result_data


def get_minimum_date(probe_data, revision_data, revision_dates):
    probe_histories = transform(
        probe_data, revision_data, break_by_channel=True, revision_dates=revision_dates
    )
    min_dates = defaultdict(lambda: defaultdict(str))

    for channel, probes in probe_histories.items():
        for probe_id, entry in probes.items():
            dates = []
            for history in entry["history"][channel]:
                revision = history["revisions"]["first"]
                dates.append(revision_dates[channel][revision]["date"])
            min_dates[probe_id][channel] = min(dates)

    return min_dates


def make_item_defn(definition, commit: Commit, new_source_url: Optional[str] = None):
    if COMMITS_KEY not in definition:
        # This is the first time we've seen this definition
        definition[COMMITS_KEY] = {"first": commit.hash, "last": commit.hash}
        definition[DATES_KEY] = {
            "first": commit.pretty_timestamp,
            "last": commit.pretty_timestamp,
        }
        definition[REFLOG_KEY] = {
            "first": commit.reflog_index,
            "last": commit.reflog_index,
        }
    else:
        # we've seen this definition, update the `last` commit and source url
        last_dt = datetime.fromisoformat(definition[DATES_KEY]["last"])
        last_timestamp = last_dt.replace(tzinfo=timezone.utc).timestamp()
        last_reflog = definition[REFLOG_KEY]["last"]
        # use negative last_reflog to match commit.sort_key()
        if commit.is_head or (last_timestamp, -last_reflog) < commit.sort_key():
            definition[COMMITS_KEY]["last"] = commit.hash
            definition[DATES_KEY]["last"] = commit.pretty_timestamp
            definition[REFLOG_KEY]["last"] = commit.reflog_index
            # only update source url when the last commit changed
            if new_source_url:
                definition[SOURCE_URL_KEY] = new_source_url

    return definition


def tags_equal(def1, def2):
    return def1["description"] == def2["description"]


def metrics_equal(def1, def2):
    return all(
        (
            def1.get(label) == def2.get(label)
            for label in {
                "bugs",
                "data_reviews",
                "data_sensitivity",
                "description",
                "disabled",
                "expires",
                "labeled",
                "labels",
                "lifetime",
                "metadata",
                "notification_emails",
                "send_in_pings",
                "time_unit",
                "type",
                "version",
                "extra_keys",
            }
        )
    )


def ping_equal(def1, def2):
    # Test all keys except the ones the probe-scraper adds
    ignored_keys = {DATES_KEY, COMMITS_KEY, HISTORY_KEY, REFLOG_KEY, SOURCE_URL_KEY}
    all_keys = set(def1.keys()).union(def2.keys()).difference(ignored_keys)

    return all((def1.get(label) == def2.get(label) for label in all_keys))


def tag_constructor(defn, tag):
    return {NAME_KEY: tag, HISTORY_KEY: [defn], IN_SOURCE_KEY: False}


def metric_constructor(defn, metric):
    return {
        TYPE_KEY: defn[TYPE_KEY],
        NAME_KEY: metric,
        HISTORY_KEY: [defn],
        IN_SOURCE_KEY: False,
    }


def ping_constructor(defn, ping):
    return {NAME_KEY: ping, HISTORY_KEY: [defn], IN_SOURCE_KEY: False}


def update_or_add_item(
    repo_items: Dict[str, dict],
    commit: Commit,
    item: str,
    definition: dict,
    equal_fn: Callable[[Any, Any], bool],
    type_ctor: Callable[[dict, str], dict],
):
    # If we've seen this item before, check previous definitions
    if item in repo_items:
        prev_defns = repo_items[item][HISTORY_KEY]

        for i, prev_defn in sorted(
            enumerate(prev_defns),
            key=lambda e: datetime.fromisoformat(e[1][DATES_KEY]["last"]),
        ):
            # If equal to a previous commit, update date and commit on existing definition
            if equal_fn(definition, prev_defn):
                new_defn = make_item_defn(
                    prev_defn, commit, definition.get(SOURCE_URL_KEY)
                )
                repo_items[item][HISTORY_KEY][i] = new_defn
                break
        # Otherwise, prepend changed definition for existing item
        else:
            new_defn = make_item_defn(definition, commit)
            repo_items[item][HISTORY_KEY] = prev_defns + [new_defn]

        # In rare cases the type can change.
        # We always pick the latest one.
        if TYPE_KEY in definition:
            repo_items[item][TYPE_KEY] = definition[TYPE_KEY]
    # We haven't seen this item before, add it
    else:
        defn = make_item_defn(definition, commit)
        repo_items[item] = type_ctor(defn, item)

    return repo_items


def transform_by_hash(
    data: Dict[str, Dict[Commit, Dict[str, dict]]],
    equal_fn: Callable[[Any, Any], bool],
    type_ctor: Callable[[dict, str], dict],
    update_result: Optional[dict] = None,
):
    """
    :param data - of the form
      <repo_name>: {
        <Commit>: {
          <item-name>: {
            ...
          },
        },
        ...
      }

    Outputs deduplicated data of the form
        <repo_name>: {
            <name>: {
                "type": <type>,
                "name": <name>,
                "history": [
                    {
                        "bugs": [<bug#>, ...],
                        ...other info (from metrics.yaml or pings.yaml)...,
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

    result = {} if update_result is None else update_result
    for repo_name, commits in data.items():
        repo_items = result.get(repo_name, {})

        # iterate through commits, sorted by Commit.sort_key()
        sorted_commits = sorted(
            iter(commits.items()),
            key=lambda x_y: x_y[0].sort_key(),
        )
        for commit, items in sorted_commits:
            for item, definition in items.items():
                repo_items = update_or_add_item(
                    repo_items,
                    commit,
                    item,
                    definition,
                    equal_fn,
                    type_ctor,
                )

            if commit.is_head:
                # if this commit is the first one, we use it to mark whether items are
                # "in-source" (aka in the source code and not removed)
                for item in repo_items:
                    repo_items[item][IN_SOURCE_KEY] = item in items

        result[repo_name] = repo_items
    return result


def transform_tags_by_hash(
    tag_data: Dict[str, Dict[Commit, Dict[str, dict]]],
    update_result: Optional[dict] = None,
):
    return transform_by_hash(tag_data, tags_equal, tag_constructor, update_result)


def transform_metrics_by_hash(
    metric_data: Dict[str, Dict[Commit, Dict[str, dict]]],
    update_result: Optional[dict] = None,
):
    return transform_by_hash(
        metric_data, metrics_equal, metric_constructor, update_result
    )


def transform_pings_by_hash(
    ping_data: Dict[str, Dict[Commit, Dict[str, dict]]],
    update_result: Optional[dict] = None,
):
    return transform_by_hash(ping_data, ping_equal, ping_constructor, update_result)
