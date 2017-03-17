# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def transform(node_data):
    results = {}
    for node_id, details in node_data.iteritems():
        results[node_id] = {
            'channel': details['channel'],
            'version': details['version'],
        }
    return results
