# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict


def transform(node_data):
    results = defaultdict(dict)
    for channel, nodes in node_data.items():
        for node_id, details in nodes.items():
            results[channel][node_id] = {
                "version": details.get("version"),
                "date": details.get("date"),
            }

    return results
