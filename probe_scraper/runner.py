# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import datetime
import errno
import gzip
import json
import os
import sys
import tempfile
import traceback
from collections import defaultdict

from dateutil.tz import tzlocal

from . import glean_checks, transform_probes, transform_revisions
from .emailer import send_ses
from .parsers.events import EventsParser
from .parsers.histograms import HistogramsParser
from .parsers.metrics import GleanMetricsParser
from .parsers.pings import GleanPingsParser
from .parsers.repositories import RepositoriesParser
from .parsers.scalars import ScalarsParser
from .scrapers import git_scraper, moz_central_scraper


class DummyParser:
    def parse(self, files):
        return {}


FROM_EMAIL = "telemetry-alerts@mozilla.com"
DEFAULT_TO_EMAIL = "glean-team@mozilla.com"
FIRST_APPEARED_DATE_KEY = "first_added"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


PARSERS = {
    # This lists the available probe registry parsers:
    # parser type -> parser
    "event": EventsParser(),
    "histogram": HistogramsParser(),
    "scalar": ScalarsParser(),
}

GLEAN_PARSER = GleanMetricsParser()
GLEAN_PINGS_PARSER = GleanPingsParser()
GLEAN_METRICS_FILENAME = "metrics.yaml"
GLEAN_PINGS_FILENAME = "pings.yaml"


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

    def date_serializer(o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

    path = os.path.join(out_dir, file_name)
    with open(path, "w") as f:
        print("  " + path)
        json.dump(
            data,
            f,
            sort_keys=True,
            indent=2,
            separators=(",", ": "),
            default=date_serializer,
        )


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


def write_general_data(out_dir):
    dump_json(general_data(), out_dir, "general")
    with open(os.path.join(out_dir, "index.html"), "w") as f:
        f.write(
            """
            <html><head><title>Mozilla Probe Info</title></head>
            <body>This site contains metadata used by Mozilla's data collection
            infrastructure, for more information see
            <a href=\"https://mozilla.github.io/probe-scraper/\">the generated documentation</a>.
            </body></html>
            """
        )


def write_glean_metric_data(metrics, dependencies, out_dir):
    # Save all our files to "outdir/glean/<repo>/..." to mimic a REST API.
    for repo, metrics_data in metrics.items():
        dependencies_data = dependencies[repo]

        base_dir = os.path.join(out_dir, "glean", repo)

        print("\nwriting output:")

        dump_json(general_data(), base_dir, "general")
        dump_json(metrics_data, base_dir, "metrics")
        dump_json(dependencies_data, base_dir, "dependencies")


def write_glean_ping_data(pings, out_dir):
    # Save all our files to "outdir/glean/<repo>/..." to mimic a REST API.
    for repo, pings_data in pings.items():
        base_dir = os.path.join(out_dir, "glean", repo)
        dump_json(pings_data, base_dir, "pings")


def write_repositories_data(repos, out_dir):
    json_data = [r.to_dict() for r in repos]
    dump_json(json_data, os.path.join(out_dir, "glean"), "repositories")


def write_v2_data(repos, out_dir):
    dump_json(
        repos["app-listings"], os.path.join(out_dir, "v2", "glean"), "app-listings"
    )
    dump_json(
        repos["library-variants"], os.path.join(out_dir, "v2", "glean"), "library-variants"
    )


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

    lookup_table = {}

    def dedupe_probes(results):
        # Most probes have exactly the same contents across revisions, so we
        # can get significant memory savings by deduplicating them across the
        # entire history.
        deduped = {}
        for key, value in results.items():
            # Get a stable hash for a dict, by sorting the keys when writing
            # out values.
            probe_hash = hash(json.dumps(value, sort_keys=True))
            lookup_for_name = lookup_table.get(key, None)
            if lookup_for_name is None:
                lookup_table[key] = {probe_hash: value}
                deduped[key] = value
            else:
                existing_probe = lookup_for_name.get(probe_hash, None)
                if existing_probe is None:
                    lookup_for_name[probe_hash] = value
                    deduped[key] = value
                else:
                    deduped[key] = existing_probe

        return deduped

    probes = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for channel, revisions in scraped_data.items():
        for revision, details in revisions.items():
            for probe_type, paths in details["registries"].items():
                results = PARSERS[probe_type].parse(paths, details["version"], channel)
                deduped = dedupe_probes(results)
                probes[channel][revision][probe_type] = deduped

    return probes


def add_first_appeared_dates(probes_by_channel, first_appeared_dates):
    for channel, probes in probes_by_channel.items():
        for probe_id, info in probes.items():
            if channel == "all":
                dates = first_appeared_dates[probe_id]
            else:
                dates = {
                    k: v
                    for k, v in first_appeared_dates[probe_id].items()
                    if k == channel
                }

            dates = {k: v.strftime(DATE_FORMAT) for k, v in dates.items()}
            probes_by_channel[channel][probe_id][FIRST_APPEARED_DATE_KEY] = dates

    return probes_by_channel


def load_moz_central_probes(
    cache_dir, out_dir, fx_version, min_fx_version, firefox_channel
):

    if fx_version:
        min_fx_version = fx_version
        max_fx_version = fx_version
    else:
        max_fx_version = None

    if firefox_channel:
        channels = [firefox_channel]
    else:
        channels = None

    # Scrape all revisions from buildhub
    revision_data = moz_central_scraper.scrape_channel_revisions(
        cache_dir,
        min_fx_version=min_fx_version,
        max_fx_version=max_fx_version,
        channels=channels,
    )
    revision_probes = parse_moz_central_probes(revision_data)

    # Get the minimum revision and date per probe-channel
    revision_dates = transform_revisions.transform(revision_data)
    first_appeared_dates = transform_probes.get_minimum_date(
        revision_probes, revision_data, revision_dates
    )

    probes_by_channel = transform_probes.transform(
        revision_probes,
        revision_data,
        break_by_channel=True,
        revision_dates=revision_dates,
    )
    probes_by_channel["all"] = transform_probes.transform(
        revision_probes,
        revision_data,
        break_by_channel=False,
        revision_dates=revision_dates,
    )

    # Add in the first appeared dates
    probes_by_channel_with_dates = add_first_appeared_dates(
        probes_by_channel, first_appeared_dates
    )

    # Serialize the probe data to disk.
    write_moz_central_probe_data(probes_by_channel_with_dates, revision_dates, out_dir)


def load_glean_metrics(cache_dir, out_dir, repositories_file, dry_run, glean_repos):
    repositories = RepositoriesParser().parse(repositories_file, glean_repos)
    commit_timestamps, repos_metrics_data, emails = git_scraper.scrape(
        cache_dir, repositories
    )

    glean_checks.check_glean_metric_structure(repos_metrics_data)

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
    pings = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for repo_name, commits in repos_metrics_data.items():
        for commit_hash, paths in commits.items():
            metrics_files = [p for p in paths if p.endswith(GLEAN_METRICS_FILENAME)]
            pings_files = [p for p in paths if p.endswith(GLEAN_PINGS_FILENAME)]

            try:
                config = {"allow_reserved": repo_name.startswith("glean")}
                repo = next(r for r in repositories if r.name == repo_name).to_dict()

                if metrics_files:
                    results, errs = GLEAN_PARSER.parse(
                        metrics_files, config, repo["url"], commit_hash
                    )
                    metrics[repo_name][commit_hash] = results

                if pings_files:
                    results, errs = GLEAN_PINGS_PARSER.parse(
                        pings_files, config, repo["url"], commit_hash
                    )
                    pings[repo_name][commit_hash] = results
            except Exception:
                files = metrics_files + pings_files
                msg = "Improper file in {}\n{}".format(
                    ", ".join(files), traceback.format_exc()
                )
                emails[repo_name]["emails"].append(
                    {"subject": "Probe Scraper: Improper File", "message": msg}
                )
            else:
                if errs:
                    msg = ("Error in processing commit {}\n" "Errors: [{}]").format(
                        commit_hash, ".".join(errs)
                    )
                    emails[repo_name]["emails"].append(
                        {
                            "subject": "Probe Scraper: Error on parsing metric or ping files",
                            "message": msg,
                        }
                    )

    abort_after_emails = False

    metrics_by_repo = {repo: {} for repo in repos_metrics_data}
    metrics_by_repo.update(
        transform_probes.transform_metrics_by_hash(commit_timestamps, metrics)
    )

    pings_by_repo = {repo: {} for repo in repos_metrics_data}
    pings_by_repo.update(
        transform_probes.transform_pings_by_hash(commit_timestamps, pings)
    )

    dependencies_by_repo = {}
    for repo in repositories:
        dependencies = {}
        for dependency in repo.dependencies:
            dependencies[dependency] = {"type": "dependency", "name": dependency}
        dependencies_by_repo[repo.name] = dependencies

    if glean_repos is None or len(glean_repos) > 1:
        # Don't check for duplicate metrics if we're only parsing
        # one glean repository (this will almost always crash, since
        # the libraries that most repositories depend on will not
        # be specified)
        abort_after_emails |= glean_checks.check_for_duplicate_metrics(
            repositories, metrics_by_repo, emails
        )
    glean_checks.check_for_expired_metrics(
        repositories, metrics, commit_timestamps, emails
    )

    write_glean_metric_data(metrics_by_repo, dependencies_by_repo, out_dir)
    write_glean_ping_data(pings_by_repo, out_dir)
    write_repositories_data(repositories, out_dir)
    write_general_data(out_dir)

    repos_v2 = RepositoriesParser().parse_v2(repositories_file)
    write_v2_data(repos_v2, out_dir)

    for repo_name, email_info in list(emails.items()):
        addresses = email_info["addresses"] + [DEFAULT_TO_EMAIL]
        for email in email_info["emails"]:
            send_ses(
                FROM_EMAIL,
                email["subject"],
                email["message"],
                addresses,
                dryrun=dry_run,
            )

    if abort_after_emails:
        raise ValueError("Errors processing Glean metrics")


def setup_output_and_cache_dirs(output_bucket, cache_bucket, out_dir, cache_dir):
    # Create the output directory
    os.mkdir(out_dir)

    # Sync the cache directory
    cache_loc = f"s3://{cache_bucket}/cache/probe-scraper"
    print("Syncing cache from {} with {}".format(cache_loc, cache_dir))
    sync_command = f"aws s3 sync {cache_loc} {cache_dir}"
    os.system(sync_command)
    return cache_loc


def sync_output_and_cache_dirs(
    output_bucket, cache_bucket, out_dir, cache_dir, cache_path
):
    # Check output dir and then sync with cloudfront
    if not os.listdir(out_dir):
        print("{} is empty".format(out_dir))
        sys.exit(1)
    else:
        print("Syncing output dir {}/ with s3://{}/".format(out_dir, output_bucket))

        # cloudfront is supposed to automatically gzip objects, but it won't do that
        # if the object size is > 10 megabytes (https://webmasters.stackexchange.com/a/111734)
        # which our files sometimes are. to work around this, we'll regzip the contents into a
        # temporary directory, and upload that with a special content encoding
        with tempfile.TemporaryDirectory() as tmpdirname:
            for root, dirnames, filenames in os.walk(out_dir):
                rel_root = os.path.relpath(root, start=out_dir)
                for dirname in dirnames:
                    os.mkdir(os.path.join(tmpdirname, rel_root, dirname))
                for filename in filenames:
                    in_filename = os.path.join(root, filename)
                    out_filename = os.path.join(tmpdirname, rel_root, filename)
                    with open(in_filename, "rb") as f1:
                        with gzip.open(out_filename, "wb") as f2:
                            f2.write(f1.read())

            sync_cft_cmd = (
                f"aws s3 sync {tmpdirname}/ s3://{output_bucket}/ "
                f"--delete "
                f"--content-type 'application/json' "
                f"--content-encoding 'gzip' "
                f"--cache-control 'max-age=28800' "
                f"--acl public-read"
            )
            os.system(sync_cft_cmd)

        # Sync cache data
        print("Syncing cache dir {}/ with s3://{}/".format(cache_dir, cache_path))
        sync_cache_cmd = f"aws s3 sync {cache_dir} {cache_path}"
        os.system(sync_cache_cmd)


def main(
    cache_dir,
    out_dir,
    firefox_version,
    min_firefox_version,
    process_moz_central_probes,
    process_glean_metrics,
    repositories_file,
    dry_run,
    glean_repos,
    firefox_channel,
    output_bucket,
    cache_bucket,
    env,
):

    # Sync dirs with s3 if we are not running pytest or local dryruns
    if env == "prod":
        cache_path = setup_output_and_cache_dirs(
            output_bucket, cache_bucket, out_dir, cache_dir
        )

    process_both = not (process_moz_central_probes or process_glean_metrics)
    if process_moz_central_probes or process_both:
        load_moz_central_probes(
            cache_dir, out_dir, firefox_version, min_firefox_version, firefox_channel
        )
    if process_glean_metrics or process_both:
        load_glean_metrics(cache_dir, out_dir, repositories_file, dry_run, glean_repos)

    # Sync results with s3 if we are not running pytest or local dryruns
    if env == "prod":
        sync_output_and_cache_dirs(
            output_bucket, cache_bucket, out_dir, cache_dir, cache_path
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-dir",
        help="Cache directory. If empty, will be filled with the probe files.",
        action="store",
        default=tempfile.mkdtemp(),
    )
    parser.add_argument(
        "--out-dir",
        help="Directory to store output files in.",
        action="store",
        default=".",
    )
    parser.add_argument(
        "--repositories-file",
        help="Repositories YAML file location.",
        action="store",
        default="repositories.yaml",
    )
    parser.add_argument(
        "--dry-run", help="Whether emails should be sent.", action="store_true"
    )
    parser.add_argument(
        "--glean-repo",
        help="The Glean Repositories to scrape (may be specified multiple times). "
        "If unspecified, scrapes all.",
        type=str,
        dest="glean_repos",
        action="append",
        required=False,
    )
    parser.add_argument(
        "--firefox-channel",
        help="The Fx channel to scrape. If unspecified, scrapes all.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--output-bucket",
        help="The output s3 cloudfront bucket where out-dir will be syncd.",
        type=str,
        default="net-mozaws-prod-us-west-2-data-pitmo",
    )
    parser.add_argument(
        "--cache-bucket",
        help="The cache bucket for probe scraper.",
        type=str,
        default="telemetry-airflow-cache",
    )
    parser.add_argument(
        "--env",
        help="We set this to 'prod' when we need to run actual s3 syncs",
        type=str,
        choices=["dev", "prod"],
        default="dev",
    )

    application = parser.add_mutually_exclusive_group()
    application.add_argument(
        "--moz-central", help="Only scrape moz-central probes", action="store_true"
    )
    application.add_argument(
        "--glean", help="Only scrape metrics in remote glean repos", action="store_true"
    )

    versions = parser.add_mutually_exclusive_group()
    versions.add_argument(
        "--firefox-version",
        help="Version of Firefox to scrape",
        action="store",
        type=int,
        required=False,
    )
    versions.add_argument(
        "--min-firefox-version",
        help="Min version of Firefox to scrape",
        action="store",
        type=int,
        required=False,
    )

    args = parser.parse_args()

    main(
        args.cache_dir,
        args.out_dir,
        args.firefox_version,
        args.min_firefox_version,
        args.moz_central,
        args.glean,
        args.repositories_file,
        args.dry_run,
        args.glean_repos,
        args.firefox_channel,
        args.output_bucket,
        args.cache_bucket,
        args.env,
    )
