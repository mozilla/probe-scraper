# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import defaultdict
from dateutil.tz import tzlocal
import argparse
import datetime
import errno
import json
import os
import tempfile
import traceback

from emailer import send_ses
from parsers.events import EventsParser
from parsers.histograms import HistogramsParser
from parsers.scalars import ScalarsParser
from scrapers import http_scraper, moz_central_scraper
import transform_probes
import transform_revisions


class DummyParser:
    def parse(self, files):
        return {}


FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "dev-telemetry-alerts@mozilla.com"


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


def dump_json(data, out_dir, file_name):
    # Make sure that the output directory exists. This also creates
    # intermediate directories if needed.
    try:
        os.makedirs(out_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    path = os.path.join(out_dir, file_name)
    with open(path, 'w') as f:
        print "  " + path
        json.dump(data, f, sort_keys=True, indent=2)


def write_moz_central_probe_data(probe_data, revisions, out_dir):
    # Save all our files to "outdir/firefox/..." to mimic a REST API.
    base_dir = os.path.join(out_dir, "firefox")

    print "\nwriting output:"
    dump_json(general_data(), base_dir, "general")
    dump_json(revisions, base_dir, "revisions")

    # Break down the output by channel. We don"t need to write a revisions
    # file in this case, the probe data will contain human readable version
    # numbers along with revision numbers.
    for channel, channel_probes in probe_data.iteritems():
        data_dir = os.path.join(base_dir, channel, "main")
        dump_json(channel_probes, data_dir, "all_probes")


def write_external_probe_data(repo_data, out_dir):
    # Save all our files to "outdir/<repo>/..." to mimic a REST API.
    for repo, probe_data in repo_data.iteritems():
        base_dir = os.path.join(out_dir, repo)

        print "\nwriting output:"
        dump_json(general_data(), base_dir, "general")

        data_dir = os.path.join(base_dir, "mobile-metrics")
        dump_json(probe_data, data_dir, "all_probes")


def load_moz_central_probes(cache_dir, out_dir):
    # Scrape probe data from repositories.
    node_data = moz_central_scraper.scrape(cache_dir)

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

    # Transform extracted data: get both the monolithic and by channel probe data.
    revisions = transform_revisions.transform(node_data)
    probes_by_channel = transform_probes.transform(probes, node_data,
                                                   break_by_channel=True)
    probes_by_channel["all"] = transform_probes.transform(probes, node_data,
                                                          break_by_channel=False)

    # Serialize the probe data to disk.
    write_moz_central_probe_data(probes_by_channel, revisions, out_dir)


def load_http_probes(date, cache_dir="cache", out_dir="output"):
    repos_probes_data, emails = http_scraper.scrape(date, cache_dir)

    # Parse probe data from files into the form:
    # <repo_name> -> {
    #   <date> -> {
    #     "histogram": {
    #       <histogram_name>: {
    #         ...
    #       },
    #       ...
    #     },
    #     "scalar": {
    #       ...
    #     },
    #   },
    #   ...
    # }
    probes = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for repo_name, dates in repos_probes_data.iteritems():
        for date, probe_types in dates.iteritems():
            for probe_type, paths in probe_types.iteritems():
                try:
                    results = PARSERS[probe_type].parse(paths)
                except Exception:
                    msg = "Improper file in {}\n{}".format(', '.join(paths), traceback.format_exc())
                    emails[repo_name]["emails"].append({
                        "subject": "Probe Scraper: Improper File",
                        "message": msg
                    })
                probes[repo_name][date][probe_type] = results

    probes_by_repo = transform_probes.transform_by_date(probes)

    write_external_probe_data(probes_by_repo, out_dir)

    for repo_name, email_info in emails.items():
        addresses = email_info["addresses"] + [DEFAULT_TO_EMAIL]
        for email in email_info["emails"]:
            send_ses(FROM_EMAIL, email["subject"], email["message"], addresses)


def main(date, cache_dir, out_dir, process_moz_central, process_http):
    if process_moz_central:
        load_moz_central_probes(cache_dir, out_dir)
    if process_http:
        load_http_probes(date, cache_dir, out_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date',
                        help='The date being run',
                        action='store',
                        required=True)
    parser.add_argument('--cache-dir',
                        help='Cache directory. If empty, will be filled with the probe files.',
                        action='store',
                        default=tempfile.mkdtemp())
    parser.add_argument('--out-dir',
                        help='Directory to store output files in.',
                        action='store',
                        default='.')
    parser.add_argument('--skip-moz-central-probes',
                        help='Directory to store output files in.',
                        action='store_false',
                        default=True)
    parser.add_argument('--skip-http-probes',
                        help='Directory to store output files in.',
                        action='store_false',
                        default=True)

    args = parser.parse_args()
    main(args.date,
         args.cache_dir,
         args.out_dir,
         args.skip_moz_central_probes,
         args.skip_http_probes)
