# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tempfile
import scraper

class DummyParser:
    def parse(self, files):
        return {}

PARSERS = {
    # This lists the available probe registry parsers:
    # parser type -> parser
    'histograms': DummyParser(),
    'scalars': DummyParser(),
    'events': DummyParser(),
}

def main(target_dir = tempfile.mkdtemp()):
    nodes = scraper.scrape(target_dir)
    probes = {
        # node_id -> {
        #   histograms: {
        #     name: ...,
        #     ...
        #   },
        #   scalars: {
        #     ...
        #   },
        # }
    }
    def add_probe_data(node_id, probe_type, probe_data):
        if not node_id in probes:
            probes[node_id] = {}
        probes[node_id][probe_type] = probe_data

    for node_id,details in nodes.iteritems():
        for probe_type,paths in details['registries'].iteritems():
            probe_data = PARSERS[probe_type].parse(paths)
            add_probe_data(node_id, probe_type, probe_data)

if __name__ == "__main__":
    main('_tmp')
