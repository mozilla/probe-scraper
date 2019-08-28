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

from . import transform_probes
from . import transform_revisions
from .emailer import send_ses
from .parsers.events import EventsParser
from .parsers.histograms import HistogramsParser
from .parsers.metrics import GleanMetricsParser
from .parsers.repositories import RepositoriesParser
from .parsers.scalars import ScalarsParser
from .scrapers import git_scraper, moz_central_scraper
from schema import And, Optional, Schema


class DummyParser:
    def parse(self, files):
        return {}


FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "dev-telemetry-alerts@mozilla.com"
FIRST_APPEARED_DATE_KEY = "first_added"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


PARSERS = {
    # This lists the available probe registry parsers:
    # parser type -> parser
    'event': EventsParser(),
    'histogram': HistogramsParser(),
    'scalar': ScalarsParser(),
}

GLEAN_PARSER = GleanMetricsParser()
GLEAN_METRICS_FILENAME = 'metrics.yaml'


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
        print("  " + path)
        json.dump(data, f, sort_keys=True, indent=2)


def write_moz_central_probe_data(probe_data, revisions, out_dir):
    # Save all our files to "outdir/firefox/..." to mimic a REST API.
    base_dir = os.path.join(out_dir, "firefox")

    print("\nwriting output:")
    dump_json(general_data(), base_dir, "general")
    dump_json(revisions, base_dir, "revisions")

    # Break down the output by channel. We don't need to write a revisions
    # file in this case, the probe data will contain human readable version
    # numbers along with revision numbers.
    for channel, channel_probes in probe_data.items():
        data_dir = os.path.join(base_dir, channel, "main")
        dump_json(channel_probes, data_dir, "all_probes")


def write_glean_metric_data(metrics, dependencies, out_dir):
    # Save all our files to "outdir/glean/<repo>/..." to mimic a REST API.
    for repo, metrics_data in metrics.items():
        dependencies_data = dependencies[repo]

        base_dir = os.path.join(out_dir, "glean", repo)

        print("\nwriting output:")

        dump_json(general_data(), base_dir, "general")
        dump_json(metrics_data, base_dir, "metrics")
        dump_json(dependencies_data, base_dir, "dependencies")


def write_repositories_data(repos, out_dir):
    json_data = [r.to_dict() for r in repos]
    dump_json(json_data, os.path.join(out_dir, "glean"), "repositories")


def parse_moz_central_probes(scraped_data):
    """
    Parse probe data from files into the form:
    channel_name: {
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
    """

    probes = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for channel, revisions in scraped_data.items():
        for revision, details in revisions.items():
            for probe_type, paths in details["registries"].items():
                results = PARSERS[probe_type].parse(paths, details["version"], channel)
                probes[channel][revision][probe_type] = results

    return probes


def add_first_appeared_dates(probes_by_channel, first_appeared_dates):
    for channel, probes in probes_by_channel.items():
        for probe_id, info in probes.items():
            if channel == "all":
                dates = first_appeared_dates[probe_id]
            else:
                dates = {k: v for k, v in first_appeared_dates[probe_id].items() if k == channel}

            dates = {k: v.strftime(DATE_FORMAT) for k, v in dates.items()}
            probes_by_channel[channel][probe_id][FIRST_APPEARED_DATE_KEY] = dates

    return probes_by_channel


def load_moz_central_probes(cache_dir, out_dir, fx_version):

    # Scrape probe data from repositories.
    node_data = moz_central_scraper.scrape(cache_dir,
                                           min_fx_version=fx_version,
                                           max_fx_version=fx_version)

    probes = parse_moz_central_probes(node_data)

    # Transform extracted data: get both the monolithic and by channel probe data.
    revisions = transform_revisions.transform(node_data)
    probes_by_channel = transform_probes.transform(probes, node_data,
                                                   break_by_channel=True)
    probes_by_channel["all"] = transform_probes.transform(probes, node_data,
                                                          break_by_channel=False)

    # Scrape all revisions from buildhub
    revision_data = moz_central_scraper.scrape_channel_revisions(cache_dir,
                                                                 min_fx_version=fx_version,
                                                                 max_fx_version=fx_version)
    revision_probes = parse_moz_central_probes(revision_data)

    # Get the minimum revision and date per probe-channel
    revision_dates = transform_revisions.transform(revision_data)
    first_appeared_dates = transform_probes.get_minimum_date(revision_probes, revision_data,
                                                             revision_dates)

    # Add in the first appeared dates
    probes_by_channel_with_dates = add_first_appeared_dates(probes_by_channel, first_appeared_dates)

    # Serialize the probe data to disk.
    write_moz_central_probe_data(probes_by_channel_with_dates, revisions, out_dir)


def check_glean_metric_structure(data):
    schema = Schema({
        str: {
            Optional(And(str, lambda x: len(x) == 40)): [And(str, lambda x: os.path.exists(x))]
        }
    })

    schema.validate(data)


def load_glean_metrics(cache_dir, out_dir, repositories_file, dry_run, glean_repo):
    repositories = RepositoriesParser().parse(repositories_file, glean_repo)
    commit_timestamps, repos_metrics_data, emails = git_scraper.scrape(cache_dir, repositories)
    repositories_by_name = dict((x.name, x) for x in repositories)

    check_glean_metric_structure(repos_metrics_data)

    # Parse metric data from files into the form:
    # <repo_name>:  {
    #   <commit-hash>:  {
    #     <metric-name>: {
    #       ...
    #     },
    #   },
    #   ...
    # }
    metrics = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    dependencies = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for repo_name, commits in repos_metrics_data.items():
        for commit_hash, paths in commits.items():
            metrics_files = [p for p in paths if p.endswith(GLEAN_METRICS_FILENAME)]

            try:
                config = {'allow_reserved': repo_name == 'glean'}
                if metrics_files:
                    results, errs = GLEAN_PARSER.parse(metrics_files, config)
                    metrics[repo_name][commit_hash] = results
                    dependencies[repo_name][commit_hash] = dict(
                        (x, {'type': 'dependency'})
                        for x in repositories_by_name[repo_name].dependencies
                    )
            except Exception:
                msg = "Improper file in {}\n{}".format(', '.join(metrics_files),
                                                       traceback.format_exc())
                emails[repo_name]["emails"].append({
                    "subject": "Probe Scraper: Improper File",
                    "message": msg
                })
            else:
                if errs:
                    msg = ("Error in processing commit {}\n"
                           "Errors: [{}]").format(commit_hash, ".".join(errs))
                    emails[repo_name]["emails"].append({
                        "subject": "Probe Scraper: Error on Metric Parsing",
                        "message": msg
                    })

    metrics_by_repo = {repo: {} for repo in repos_metrics_data}
    metrics_by_repo.update(transform_probes.transform_by_hash(commit_timestamps, metrics))

    dependencies_by_repo = {repo: {} for repo in repos_metrics_data}
    dependencies_by_repo.update(transform_probes.transform_by_hash(commit_timestamps, dependencies))

    write_glean_metric_data(metrics_by_repo, dependencies_by_repo, out_dir)

    write_repositories_data(repositories, out_dir)

    for repo_name, email_info in list(emails.items()):
        addresses = email_info["addresses"] + [DEFAULT_TO_EMAIL]
        for email in email_info["emails"]:
            send_ses(FROM_EMAIL, email["subject"], email["message"], addresses, dryrun=dry_run)


def main(cache_dir,
         out_dir,
         firefox_version,
         process_moz_central_probes,
         process_glean_metrics,
         repositories_file,
         dry_run,
         glean_repo):

    process_both = not (process_moz_central_probes or process_glean_metrics)
    if process_moz_central_probes or process_both:
        load_moz_central_probes(cache_dir, out_dir, firefox_version)
    if process_glean_metrics or process_both:
        load_glean_metrics(cache_dir, out_dir, repositories_file, dry_run, glean_repo)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache-dir',
                        help='Cache directory. If empty, will be filled with the probe files.',
                        action='store',
                        default=tempfile.mkdtemp())
    parser.add_argument('--out-dir',
                        help='Directory to store output files in.',
                        action='store',
                        default='.')
    parser.add_argument('--firefox-version',
                        help='Version of Firefox to scrape',
                        action='store',
                        type=int,
                        required=False)
    parser.add_argument('--repositories-file',
                        help='Repositories YAML file location.',
                        action='store',
                        default='repositories.yaml')
    parser.add_argument('--dry-run',
                        help='Whether emails should be sent.',
                        action='store_true')
    parser.add_argument('--glean-repo',
                        help='The Glean Repository to scrape. If unspecified, scrapes all.',
                        type=str,
                        required=False)

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--moz-central',
                       help='Only scrape moz-central probes',
                       action='store_true')
    group.add_argument('--glean',
                       help='Only scrape metrics in remote glean repos',
                       action='store_true')

    args = parser.parse_args()
    main(args.cache_dir,
         args.out_dir,
         args.firefox_version,
         args.moz_central,
         args.glean,
         args.repositories_file,
         args.dry_run,
         args.glean_repo)
