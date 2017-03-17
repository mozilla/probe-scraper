# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict

def is_test_probe():
    if ptype == 'histogram' and name.startswith("TELEMETRY_TEST_"):
        # These are test-only probes and never sent out.
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
        id = probe_type + "/" + name

        if id in result_data and channel in result_data[id]:
            # If the probes state didn't change from the previous revision,
            # let's continue.
            previous = result_data[id]["history"][channel][-1]
            if histograms_equal(previous, probe):
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

        probe["revisions"] = {"first": node_id, "last": node_id}
        result_data[id]["history"][channel].append(probe)

def sorted_node_lists_by_channel(node_data):
    channels = defaultdict(list)
    for node_id,data in node_data.iteritems():
        channels[data['channel']].append({
            'node_id': node_id,
            'version': data['version'],
        })

    for channel,data in channels.iteritems():
        channels[channel] = sorted(data, key=lambda n: int(n["version"]))

    return channels

def transform(probe_data, node_data):
    channels = sorted_node_lists_by_channel(node_data)

    result_data = {}
    for channel,channel_data in channels.iteritems():
        for entry in channel_data:
            node_id = entry['node_id']
            for probe_type,probes in probe_data[node_id].iteritems():
                extract_node_data(node_id, channel, probe_type, probes, result_data)

    return result_data
