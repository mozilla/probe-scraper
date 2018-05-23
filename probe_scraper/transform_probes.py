# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from collections import defaultdict


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


def get_probe_id(ptype, name):
    return ptype + "/" + name


def probes_equal(probe1, probe2):
    props = [
        # Common.
        "cpp_guard",
        "optout",
        # Histograms & scalars.
        "details/keyed",
        "details/kind",
        # Histograms.
        "details/n_buckets",
        "details/n_values",
        "details/low",
        "details/high",
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
    for name, probe in probe_data.iteritems():
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
    for channel, nodes in node_data.iteritems():
        for node_id, data in nodes.iteritems():
            channels[channel].append({
                'node_id': node_id,
                'version': data['version'],
            })

    for channel, data in channels.iteritems():
        channels[channel] = sorted(data, key=lambda n: int(n["version"]), reverse=True)

    return channels


def transform(probe_data, node_data, break_by_channel):
    """ Transform the probe data into the final format.

    :param probe_data: the preprocessed probe data.
    :param node_data: the raw probe data.
    :param break_by_channel: True if we want the probe output grouped by
           release channel.
    """
    channels = sorted_node_lists_by_channel(node_data)

    result_data = {}
    for channel, channel_data in channels.iteritems():
        print "\n" + channel + " - transforming probe data:"
        for entry in channel_data:
            node_id = entry['node_id']
            readable_version = entry["version"]
            print "  from: " + str({"node": node_id, "version": readable_version})
            for probe_type, probes in probe_data[channel][node_id].iteritems():
                # Group the probes by the release channel, if requested
                extract_node_data(node_id, channel, probe_type, probes, result_data,
                                  readable_version, break_by_channel)

    return result_data


def make_commit_hash_probe_definition(definition, commit, timestamp):
    if COMMITS_KEY not in definition:
        definition[COMMITS_KEY] = {
            "first": commit,
            "last": commit
        }
    else:
        definition[COMMITS_KEY]["last"] = commit

    return definition


def transform_by_hash(commit_timestamps, probe_data):
    """
    :param commit_timestamps - of the form
      <repo_name> -> {
        <commit-hash> -> <commit-timestamp>,
        ...
      }

    :param probe_data - of the form
      <repo_name> -> {
        <commit-hash> -> {
          "histogram": {
            <histogram_name>: {
              ...
            },
            ...
          },
          "scalar": {
            ...
          },
        },
        ...
      }

    Outputs deduplicated data of the form
        <repo_name>: {
            <probe_slug>: {
                    "type": <type>,
                    "name": <name>,
                    "history": {
                        <repo_name>: [
                            {
                                "description": <description>,
                                "details": {
                                    "high": <high>,
                                    "low": <low>,
                                    "keyed": <boolean>,
                                    "kind": <kind>,
                                    ...
                                }
                                "release": <boolean>,
                                "commits": {
                                    "first": <date>,
                                    "last": <date>
                                },
                                ...
                            },
                            ...
                        ]
                    }
                }
            }
        }
    """

    all_probes = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for repo_name, commits in probe_data.iteritems():
        sorted_commits = sorted(commits.iteritems(),
                                key=lambda (x, y): int(commit_timestamps[repo_name][x]))
        for commit_hash, probes in sorted_commits:
            timestamp = commit_timestamps[repo_name][commit_hash]
            for ptype, ptype_probes in probes.iteritems():
                for probe, definition in ptype_probes.iteritems():
                    probe_id = get_probe_id(ptype, probe)

                    if probe_id in all_probes[repo_name]:
                        prev_defns = all_probes[repo_name][probe_id][HISTORY_KEY][repo_name]

                        # If equal to previous commit, update date and commit on existing definition
                        if probes_equal(definition, prev_defns[0]):
                            new_defn = make_commit_hash_probe_definition(prev_defns[0],
                                                                         commit_hash,
                                                                         timestamp)
                            all_probes[repo_name][probe_id][HISTORY_KEY][repo_name][0] = new_defn

                        # Otherwise, Append changed definition for existing probe
                        else:
                            new_defn = make_commit_hash_probe_definition(definition,
                                                                         commit_hash,
                                                                         timestamp)
                            all_probes[repo_name][probe_id][HISTORY_KEY][repo_name] = \
                                [new_defn] + prev_defns

                    # Otherwise, add new probe
                    else:
                        defn = make_commit_hash_probe_definition(definition, commit_hash, timestamp)
                        all_probes[repo_name][probe_id] = {
                            TYPE_KEY: ptype,
                            NAME_KEY: probe,
                            HISTORY_KEY: {repo_name: [defn]}
                        }

    return all_probes
