# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
import datetime
from dateutil.tz import tzlocal
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
    'histogram': HistogramsParser(),
    'scalar': ScalarsParser(),
    'event': EventsParser(),
}


def general_data():
    return {
        "lastUpdate": datetime.datetime.now(tzlocal()).isoformat(),
    }


def main(temp_dir, out_dir):
    # Scrape probe data from repositories.
    node_data = scraper.scrape(temp_dir)

    # Parse probe data from files into the form:
    # channel_name -> {
    #   node_id -> {
    #     histogram: {
    #       name: ...,
    #       ...
    #     },
    #     scalar: {
    #       ...
    #     },
    #   },
    #   ...
    # }
    probes = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for channel, nodes in node_data.iteritems():
        for node_id, details in nodes.iteritems():
            for probe_type, paths in details['registries'].iteritems():
                results = PARSERS[probe_type].parse(paths, details["version"])
                probes[channel][node_id][probe_type] = results

    # Transform extracted data.
    revisions = transform_revisions.transform(node_data)
    probe_data = transform_probes.transform(probes, node_data)

    # Serialize extracted data.
    def dump_json(data, file_name):
        path = os.path.join(out_dir, file_name)
        with open(path, 'w') as f:
            print "  " + path
            json.dump(data, f, sort_keys=True, indent=2)

    print "\nwriting output:"
    dump_json(revisions, 'revisions.json')
    dump_json(probe_data, 'probes.json')
    dump_json(general_data(), 'general.json')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tempdir',
                        help='Temporary directory to work in. This serves as a cache if reused.',
                        action='store',
                        default=tempfile.mkdtemp())
    parser.add_argument('--outdir',
                        help='Directory to store output files in.',
                        action='store',
                        default='.')

    args = parser.parse_args()
    main(args.tempdir, args.outdir)
