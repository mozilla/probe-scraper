# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import tempfile
import scraper
from parsers.histograms import HistogramsParser
from parsers.scalars import ScalarsParser

class DummyParser:
    def parse(self, files):
        return {}

PARSERS = {
    # This lists the available probe registry parsers:
    # parser type -> parser
    'histograms': HistogramsParser(),
    'scalars': ScalarsParser(),
    'events': DummyParser(),
}

def main(target_dir = tempfile.mkdtemp()):
    nodes = scraper.scrape(target_dir)

    for node_id,details in nodes.iteritems():
        for ptype,paths in details['registries'].iteritems():
            probes = PARSERS[ptype].parse(paths)

if __name__ == "__main__":
    main('_tmp')
