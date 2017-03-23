# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
import datetime
import tempfile
import argparse
from collections import defaultdict

import scraper
from parsers.histograms import HistogramsParser
from parsers.scalars import ScalarsParser
from parsers.events import EventsParser
import transform_revisions
import transform_probes


class DummyParser:
    def parse(self, files):
        return {}


PARSERS = {
    # This lists the available probe registry parsers:
    # parser type -> parser
    'histograms': HistogramsParser(),
    'scalars': ScalarsParser(),
    'events': EventsParser(),
}


def general_data():
    return {
        "lastUpdate": datetime.date.today().isoformat(),
    }


def main(temp_dir, out_dir):
    # Scrape probe data from repositories.
    nodes = scraper.scrape(temp_dir)

    # Parse probe data from files into the form:
    # node_id -> {
    #   histograms: {
    #     name: ...,
    #     ...
    #   },
    #   scalars: {
    #     ...
    #   },
    # }
    probes = defaultdict(dict)
    for node_id, details in nodes.iteritems():
        for probe_type, paths in details['registries'].iteritems():
            results = PARSERS[probe_type].parse(paths, details["version"])
            probes[node_id][probe_type] = results

    # Transform extracted data.
    revisions = transform_revisions.transform(nodes)
    probe_data = transform_probes.transform(probes, nodes)

    # Serialize extracted data.
    def dump_json(data, file_name):
        with open(os.path.join(out_dir, file_name), 'w') as f:
            json.dump(data, f, sort_keys=True, indent=2)

    print "\n... writing output files to " + out_dir
    dump_json(revisions, 'revisions.json')
    dump_json(probe_data, 'probes.json')
    dump_json(general_data(), 'general.json')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tempdir',
                        help='Temporary directory to work in.',
                        action='store',
                        default=tempfile.mkdtemp())
    parser.add_argument('--outdir',
                        help='Directory to store output files in.',
                        action='store',
                        default='.')

    args = parser.parse_args()
    main(args.tempdir, args.outdir)
