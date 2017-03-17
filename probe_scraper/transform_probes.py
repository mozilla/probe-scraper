# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pprint
from collections import defaultdict

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

def probes_equal(probe_type, probe1, probe2):
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

# incoming probe_data is of the form:
#   node_id -> {
#     histograms: {
#       name: ...,
#       ...
#     },
#     scalars: {
#       ...
#     },
#   }
#
# node_data is of the form:
#   node_id -> {
#     version: ...
#     channel: ...
#   }

def extract_node_data(node_id, channel, probe_type, probe_data, result_data):
    for name,probe in probe_data.iteritems():
        if is_test_probe(probe_type, name):
            continue

        id = probe_type + "/" + name
        if id in result_data and channel in result_data[id]["history"]:
            # If the probes state didn't change from the previous revision,
            # we just override with the latest state and continue.
            previous = result_data[id]["history"][channel][-1]
            if probes_equal(probe_type, previous, probe):
                previous["revisions"]["first"] = node_id
                continue
        if not id in result_data:
            result_data[id] = {
                "type": probe_type,
                "name": name,
                "history": {channel: []},
            }
        if not channel in result_data[id]["history"]:
            result_data[id]["history"][channel] = []

        probe["revisions"] = {
            "first": node_id,
            "last": node_id
        }
        result_data[id]["history"][channel].append(probe)

def sorted_node_lists_by_channel(node_data):
    channels = defaultdict(list)
    for node_id,data in node_data.iteritems():
        channels[data['channel']].append({
            'node_id': node_id,
            'version': data['version'],
        })

    for channel,data in channels.iteritems():
        channels[channel] = sorted(data, key=lambda n: int(n["version"]), reverse=True)

    return channels

def transform(probe_data, node_data):
    channels = sorted_node_lists_by_channel(node_data)

    result_data = {}
    for channel,channel_data in channels.iteritems():
        print "\n" + channel + " - transforming probe data:"
        for entry in channel_data:
            node_id = entry['node_id']
            print "  from: " + str({"node": node_id, "version": node_data[node_id]["version"]})
            for probe_type,probes in probe_data[node_id].iteritems():
                extract_node_data(node_id, channel, probe_type, probes, result_data)

    return result_data
