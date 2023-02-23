# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import copy
import datetime
import errno
import json
import os
import sys
import tempfile
import traceback
from collections import defaultdict
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional

from dateutil.tz import tzlocal

from . import fog_checks, glean_checks, transform_probes, transform_revisions
from .emailer import send_ses
from .exc import ProbeScraperInvalidRequest
from .parsers.events import EventsParser
from .parsers.histograms import HistogramsParser
from .parsers.metrics import GleanMetricsParser
from .parsers.pings import GleanPingsParser
from .parsers.repositories import RepositoriesParser, Repository
from .parsers.scalars import ScalarsParser
from .parsers.tags import GleanTagsParser
from .remote_storage import remote_storage_pull, remote_storage_push
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
GLEAN_TAGS_PARSER = GleanTagsParser()
GLEAN_METRICS_FILENAME = "metrics.yaml"
GLEAN_PINGS_FILENAME = "pings.yaml"
GLEAN_TAGS_FILENAME = "tags.yaml"


def date_or_none(value: str):
    if value.lower() in ("", "none"):
        return None
    return datetime.date.fromisoformat(value)


def general_data() -> Dict[str, str]:
    return {
        "lastUpdate": datetime.datetime.now(tzlocal()).isoformat(),
    }


def dump_json(data: Any, out_dir: Path, file_name: str) -> Path:
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

    path = out_dir / file_name
    print(f"  {path}")
    path.write_text(
        json.dumps(
            data,
            sort_keys=True,
            indent=2,
            separators=(",", ": "),
            default=date_serializer,
        )
    )
    return path


def load_json(out_dir: Path, file_name: str, default: Any = None) -> Any:
    try:
        text = (out_dir / file_name).read_text()
    except FileNotFoundError:
        return default
    return json.loads(text)


def write_moz_central_probe_data(
    probe_data: Dict[str, Any], revisions: Any, out_dir: Path
) -> List[Path]:
    # Save all our files to "out_dir/firefox/..." to mimic a REST API.
    base_dir = out_dir / "firefox"

    print("\nwriting output:")
    dump_json(general_data(), base_dir, "general")
    dump_json(revisions, base_dir, "revisions")

    # Break down the output by channel. We don't need to write a revisions
    # file in this case, the probe data will contain human readable version
    # numbers along with revision numbers.
    for channel, channel_probes in probe_data.items():
        data_dir = base_dir / channel / "main"
        dump_json(channel_probes, data_dir, "all_probes")
    return [base_dir]


def write_general_data(out_dir: Path) -> List[Path]:
    general_path = dump_json(general_data(), out_dir, "general")
    index_path = out_dir / "index.html"
    index_path.write_text(
        dedent(
            """
            <html><head><title>Mozilla Probe Info</title></head>
            <body>This site contains metadata used by Mozilla's data collection
            infrastructure, for more information see
            <a href=\"https://mozilla.github.io/probe-scraper/\">the generated documentation</a>.
            </body></html>
            """
        )
    )
    return [general_path, index_path]


def write_glean_data_by_repo(
    data_by_repo: Dict[str, Any], out_dir: Path, file_name: str
) -> List[Path]:
    # Save all our files to "out_dir/glean/<repo>/..." to mimic a REST API.
    return [
        dump_json(data, out_dir / "glean" / repo, file_name)
        for repo, data in data_by_repo.items()
    ]


def write_repositories_data(repos: List[Repository], out_dir: Path) -> List[Path]:
    json_data = [r.to_dict() for r in repos]
    return [dump_json(json_data, out_dir / "glean", "repositories")]


def write_v2_data(repos: Dict[str, Any], out_dir: Path) -> List[Path]:
    base_dir = out_dir / "v2" / "glean"
    dump_json(repos["app-listings"], base_dir, "app-listings")
    dump_json(
        repos["library-variants"],
        base_dir,
        "library-variants",
    )
    return [base_dir]


def parse_moz_central_probes(
    scraped_data: Dict[str, Dict[str, dict]]
) -> Dict[str, Dict[str, dict]]:
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

    def dedupe_probes(results: Dict[str, Any]) -> Dict[str, Any]:
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

    probes: Dict[str, Dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    for channel, revisions in scraped_data.items():
        for revision, details in revisions.items():
            for probe_type, paths in details["registries"].items():
                results = PARSERS[probe_type].parse(paths, details["version"], channel)
                deduped = dedupe_probes(results)
                probes[channel][revision][probe_type] = deduped

    return probes


def apply_ping_specific_metadata(metadata, ping_metadata):
    """apply_ping_specific_metadata recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``ping_metadata`` is merged into
    ``metadata``.
    :param metadata: dict onto which the merge is executed
    :param ping_metadata: dct merged into metadata
    :return: None
    """
    for k, v in ping_metadata.items():
        if (
            k in metadata
            and isinstance(metadata[k], dict)
            and isinstance(ping_metadata[k], dict)
        ):
            apply_ping_specific_metadata(metadata[k], ping_metadata[k])
        else:
            metadata[k] = ping_metadata[k]


def add_pipeline_metadata_defaults(repositories):
    for repo in repositories:
        metadata_defaults = repo.moz_pipeline_metadata_defaults
        metadata_defaults["bq_dataset_family"] = repo.app_id.replace("-", "_")
        metadata_defaults["bq_metadata_format"] = (
            "pioneer" if repo.app_id.startswith("rally") else "structured"
        )


def add_pipeline_metadata(pings_by_repo, repositories):
    for repo in repositories:
        metadata_defaults = repo.moz_pipeline_metadata_defaults

        current_pings = pings_by_repo.get(repo.name)
        for ping_name, ping in current_pings.items():
            ping_metadata = repo.moz_pipeline_metadata.get(ping_name, {})
            pipeline_metadata = copy.deepcopy(metadata_defaults)
            pipeline_metadata["bq_table"] = ping_name.replace("-", "_") + "_v1"

            apply_ping_specific_metadata(pipeline_metadata, ping_metadata)

            if pipeline_metadata:
                ping["moz_pipeline_metadata"] = pipeline_metadata


def add_first_appeared_dates(
    probes_by_channel: Dict[str, Dict[str, dict]],
    first_appeared_dates: Dict[str, Dict[str, datetime.datetime]],
) -> Dict[str, Dict[str, dict]]:
    for channel, probes in probes_by_channel.items():
        for probe_id, _ in probes.items():
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
    cache_dir: Path,
    out_dir: Path,
    fx_version: int,
    min_fx_version: int,
    firefox_channel: str,
    update: bool = False,
) -> List[Path]:
    if update:
        raise NotImplementedError("Updates are not supported for moz central probes")

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
    return write_moz_central_probe_data(
        probes_by_channel_with_dates, revision_dates, out_dir
    )


def load_glean_metrics(
    cache_dir: Path,
    out_dir: Path,
    repositories_file: Path,
    dry_run: bool,
    glean_repos: Optional[List[str]],
    bugzilla_api_key: Optional[str],
    glean_urls: Optional[List[str]] = None,
    glean_commit: Optional[str] = None,
    glean_commit_branch: Optional[str] = None,
    update: bool = False,
    output_bucket: Optional[str] = None,
    email_file: Optional[Path] = None,
    glean_limit_date: Optional[datetime.date] = None,
    check_expiry: bool = False,
    check_fog_expiry: bool = False,
) -> List[Path]:
    emails = {}
    found_duplicate_metrics = False
    upload_paths = []
    all_repos = repositories = RepositoriesParser().parse(repositories_file)
    add_pipeline_metadata_defaults(repositories)
    if glean_urls:
        repositories = [r for r in repositories if r.url in glean_urls]
    elif glean_repos:
        repositories = [r for r in repositories if r.name in glean_repos]
    if not repositories:
        raise ProbeScraperInvalidRequest(
            "No glean repos matched --glean-repo or --glean-url"
        )

    filter_repos = glean_urls or glean_repos
    scrape_commits = filter_repos or glean_limit_date or not update
    generate_metadata = not filter_repos or not update

    tags_by_repo = {}
    metrics_by_repo = {}
    pings_by_repo = {}
    if update and output_bucket:
        remote_storage_pull(
            f"{output_bucket.rstrip('/')}/glean/",
            out_dir / "glean",
            decompress=True,
        )
    # init cache for all repos to ensure deps are available for duplicate metric checks
    for repo in all_repos:
        if update:
            repo_dir = out_dir / "glean" / repo.name
            tags_by_repo[repo.name] = load_json(repo_dir, "tags", default={})
            metrics_by_repo[repo.name] = load_json(repo_dir, "metrics", default={})
            pings_by_repo[repo.name] = load_json(repo_dir, "pings", default={})
        else:
            tags_by_repo[repo.name] = {}
            metrics_by_repo[repo.name] = {}
            pings_by_repo[repo.name] = {}

    if scrape_commits:
        commits_by_repo, emails, upload_repos = git_scraper.scrape(
            cache_dir,
            repositories,
            glean_commit,
            glean_commit_branch,
            glean_limit_date,
        )

        glean_checks.check_glean_metric_structure(commits_by_repo)

        # Parse metric data from files into the form:
        # <repo_name>:  {
        #   <commit-hash>:  {
        #     <metric-name>: {
        #       ...
        #     },
        #   },
        #   ...
        # }
        tags = defaultdict(dict)
        metrics = defaultdict(dict)
        pings = defaultdict(dict)
        for repo_name, commits in commits_by_repo.items():
            for commit, paths in commits.items():
                tags_files = [p for p in paths if p.name == GLEAN_TAGS_FILENAME]
                metrics_files = [p for p in paths if p.name == GLEAN_METRICS_FILENAME]
                pings_files = [p for p in paths if p.name == GLEAN_PINGS_FILENAME]

                try:
                    config = {"allow_reserved": repo_name.startswith("glean")}
                    repo = next(
                        r for r in repositories if r.name == repo_name
                    ).to_dict()

                    errs = []
                    if tags_files:
                        results, tag_errs = GLEAN_TAGS_PARSER.parse(
                            tags_files, config, repo["url"], commit.hash
                        )
                        tags[repo_name][commit] = results
                        errs += tag_errs

                    if metrics_files:
                        results, metric_errs = GLEAN_PARSER.parse(
                            metrics_files, config, repo["url"], commit.hash
                        )
                        metrics[repo_name][commit] = results
                        errs += metric_errs

                    if pings_files:
                        results, ping_errs = GLEAN_PINGS_PARSER.parse(
                            pings_files, config, repo["url"], commit.hash
                        )
                        pings[repo_name][commit] = results
                        errs += ping_errs
                except Exception:
                    files = metrics_files + pings_files
                    msg = "Improper file in {}\n{}".format(
                        ", ".join(map(str, files)), traceback.format_exc()
                    )
                    emails[repo_name]["emails"].append(
                        {"subject": "Probe Scraper: Improper File", "message": msg}
                    )
                else:
                    if errs:
                        msg = ("Error in processing commit {}\n" "Errors: [{}]").format(
                            commit.hash, ".".join(errs)
                        )
                        emails[repo_name]["emails"].append(
                            {
                                "subject": "Probe Scraper: Error on parsing metric or ping files",
                                "message": msg,
                            }
                        )

        transform_probes.transform_tags_by_hash(tags, update_result=tags_by_repo)
        transform_probes.transform_metrics_by_hash(
            metrics, update_result=metrics_by_repo
        )
        transform_probes.transform_pings_by_hash(pings, update_result=pings_by_repo)

        # must be checked for all repos to ensure dependencies are available
        found_duplicate_metrics |= glean_checks.check_for_duplicate_metrics(
            all_repos, metrics_by_repo, emails
        )

    # currently always true, but left in for clarity
    if scrape_commits or generate_metadata:
        print("\nwriting output:")

        # write metadata by repo to ensure changes in repositories.yaml are published
        # nightly, instead of just when a repo's commits are next scraped (maybe never)
        dependencies_by_repo = {
            repo.name: {
                dependency: {"type": "dependency", "name": dependency}
                for dependency in repo.dependencies
            }
            for repo in repositories
        }
        metadata_by_repo_paths = write_glean_data_by_repo(
            dependencies_by_repo, out_dir, "dependencies"
        )

        general_by_repo = {repo.name: general_data() for repo in repositories}
        metadata_by_repo_paths += write_glean_data_by_repo(
            general_by_repo, out_dir, "general"
        )

        # pings contain both commits and metadata
        add_pipeline_metadata(pings_by_repo, repositories)
        metadata_by_repo_paths += write_glean_data_by_repo(
            pings_by_repo, out_dir, "pings"
        )

        if scrape_commits:
            # tags and metrics don't contain metadata from repositories.yaml,
            # so these files are only updated when scraping commits.
            write_glean_data_by_repo(tags_by_repo, out_dir, "tags")
            write_glean_data_by_repo(metrics_by_repo, out_dir, "metrics")

            # only include files for authorized repos
            upload_paths += [
                out_dir / "glean" / repo_name for repo_name in upload_repos
            ]
        else:
            # only include metadata files
            upload_paths += metadata_by_repo_paths

    if generate_metadata:
        repositories_data_paths = write_repositories_data(repositories, out_dir)
        general_data_paths = write_general_data(out_dir)

        repos_v2 = RepositoriesParser().parse_v2(repositories_file)
        v2_data_paths = write_v2_data(repos_v2, out_dir)

        if not filter_repos:
            # only upload these paths when repositories were not filtered
            upload_paths += repositories_data_paths
            upload_paths += general_data_paths
            upload_paths += v2_data_paths

    if check_expiry:
        glean_checks.check_for_expired_metrics(repositories, metrics_by_repo, emails)

    if check_fog_expiry:
        # FOG repos (e.g. firefox-desktop, gecko) use a different expiry mechanism.
        # Also, expired metrics in FOG repos can have bugs auto-filed for them.
        fog_emails_by_repo = fog_checks.file_bugs_and_get_emails_for_expiring_metrics(
            repositories, metrics_by_repo, bugzilla_api_key, dry_run
        )
        if fog_emails_by_repo is not None:
            emails.update(fog_emails_by_repo)

    for email_info in emails.values():
        addresses = email_info["addresses"] + [DEFAULT_TO_EMAIL]
        for email in email_info["emails"]:
            send_ses(
                FROM_EMAIL,
                email["subject"],
                email["message"],
                addresses,
                dryrun=dry_run,
                email_file=email_file,
            )

    if found_duplicate_metrics:
        raise ValueError("Found duplicate Glean metrics, check email for details")

    return upload_paths


def setup_output_and_cache_dirs(
    output_bucket: Optional[str],
    cache_bucket: Optional[str],
    out_dir: Path,
    cache_dir: Path,
    update: bool,
):
    # Create the output directory
    out_dir.mkdir(parents=True, exist_ok=True)

    # Updates are expected not to benefit from what is stored in cache_bucket.
    if cache_bucket and not update:
        # Sync the cache directory
        print(f"Syncing cache from {cache_bucket} with {cache_dir}")
        remote_storage_pull(cache_bucket, cache_dir)


def push_output_and_cache_dirs(
    output_bucket: Optional[str],
    cache_bucket: Optional[str],
    out_dir: Path,
    cache_dir: Path,
    upload_paths: Optional[List[Path]] = None,
    update: bool = False,
):
    # Check output dir and then sync with remote storage
    if not os.listdir(out_dir):
        print("{} is empty".format(out_dir))
        sys.exit(1)
    else:
        if output_bucket:
            for src in [out_dir] if upload_paths is None else upload_paths:
                if src is out_dir:
                    dst = output_bucket
                else:
                    dst = f"{output_bucket.rstrip('/')}/{src.relative_to(out_dir)}"
                    if src.is_dir():
                        dst += "/"
                remote_storage_push(
                    src=src,
                    dst=dst,
                    compress=True,
                    delete=src.is_dir(),
                    cache_control="max-age=28800",
                    acl="public-read",
                )
        if cache_bucket and not update:
            remote_storage_push(src=cache_dir, dst=cache_bucket, exclude=("*.git/*",))


def main(
    cache_dir: Path,
    out_dir: Path,
    firefox_version: int,
    min_firefox_version: int,
    process_moz_central_probes: bool,
    process_glean_metrics: bool,
    repositories_file: Path,
    dry_run: bool,
    glean_repos: Optional[List[str]],
    firefox_channel: str,
    output_bucket: Optional[str],
    cache_bucket: Optional[str],
    env: str,
    bugzilla_api_key: Optional[str],
    glean_urls: Optional[List[str]] = None,
    glean_commit: Optional[str] = None,
    glean_commit_branch: Optional[str] = None,
    update: bool = False,
    email_file: Optional[Path] = None,
    glean_limit_date: Optional[datetime.date] = None,
    check_expiry: bool = False,
    check_fog_expiry: bool = False,
) -> List[Path]:

    # Sync dirs with remote storage if we are not running pytest or local dryruns
    if env == "prod":
        setup_output_and_cache_dirs(
            output_bucket, cache_bucket, out_dir, cache_dir, update
        )

    upload_paths = []
    if not (process_moz_central_probes or process_glean_metrics):
        process_moz_central_probes = process_glean_metrics = True
    if process_moz_central_probes:
        upload_paths += load_moz_central_probes(
            cache_dir,
            out_dir,
            firefox_version,
            min_firefox_version,
            firefox_channel,
            update=update,
        )
    if process_glean_metrics:
        upload_paths += load_glean_metrics(
            cache_dir,
            out_dir,
            repositories_file,
            dry_run,
            glean_repos,
            bugzilla_api_key,
            glean_urls=glean_urls,
            glean_commit=glean_commit,
            glean_commit_branch=glean_commit_branch,
            update=update,
            output_bucket=output_bucket,
            email_file=email_file,
            glean_limit_date=glean_limit_date,
            check_expiry=check_expiry,
            check_fog_expiry=check_fog_expiry,
        )

    # Sync results if we are not running pytest or local dryruns
    if env == "prod":
        out_dir_is_complete = (
            process_moz_central_probes
            and process_glean_metrics
            and glean_urls is None
            and glean_repos is None
            and glean_commit is None
        )
        push_output_and_cache_dirs(
            output_bucket,
            cache_bucket,
            out_dir,
            cache_dir,
            # only use upload paths if out_dir is not complete
            upload_paths=(None if out_dir_is_complete else upload_paths),
            update=update,
        )
    return upload_paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-dir",
        help="Cache directory. If empty, will be filled with the probe files.",
        type=Path,
        action="store",
        default=tempfile.mkdtemp(),
    )
    parser.add_argument(
        "--out-dir",
        help="Directory to store output files in.",
        type=Path,
        action="store",
        default=".",
    )
    parser.add_argument(
        "--repositories-file",
        help="Repositories YAML file location.",
        type=Path,
        action="store",
        default="repositories.yaml",
    )
    parser.add_argument(
        "--check-expiry",
        help="Send non-FOG expiry emails. Only scheduled on Mondays, to avoid daily spamming.",
        action="store_true",
    )
    parser.add_argument(
        "--check-fog-expiry",
        help="Send FOG expiry emails. Only scheduled on Wednesdays, to avoid daily spamming and"
        " merge days, which are Monday or Tuesday.",
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        help="Write emails to a file instead of sending via SES, and don't file bugs.",
        action="store_true",
    )
    parser.add_argument(
        "--firefox-channel",
        help="The Fx channel to scrape. If unspecified, scrapes all.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--output-bucket",
        help="The output remote storage bucket where out-dir will be syncd.",
        type=str,
    )
    parser.add_argument(
        "--cache-bucket",
        help="The cache bucket for probe scraper.",
        type=str,
    )
    parser.add_argument(
        "--env",
        help="We set this to 'prod' when we need to run actual remote storage syncs",
        type=str,
        choices=["dev", "prod"],
        default="dev",
    )
    parser.add_argument(
        "--bugzilla-api-key",
        help="The bugzilla API key used to find and file bugs for FOG repos."
        " If not provided, no bugs will be filed.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--glean-commit",
        help="The glean commit to scrape. If unspecified, scrapes all. If specified, only upload"
        " per repo files to --output-bucket for repos where --glean-commit-branch matches the"
        " branch for that repo. If --glean-commit-branch is not specified or does not match the"
        " branch for any repo, no per repo files will be uploaded. Per repo files not uploaded to"
        " --output-bucket will still be written to --out-dir. When no branch is specified in"
        " --repositories-file, the branch for that repo is the git default branch.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--glean-commit-branch",
        help="The git branch that --glean-commit is on. If specified with --glean-commit, and"
        " matches the branch for a glean repo, verify that --glean-commit is actually on that"
        " branch.",
        type=str,
        required=False,
    )
    parser.add_argument(
        "--glean-limit-date",
        help="UTC date to limit commit timestamp when scraping Glean repos. All "
        "commits on or after this date are scraped. This behavior is to account for "
        "nightly runs that don't occur on weekends, in which case this flag must be "
        "set to the date for friday when it is run on monday morning.",
        type=date_or_none,
        required=False,
    )
    parser.add_argument(
        "--update",
        help="If specified without --glean-repo or --glean-url, scrape nothing and don't write any"
        " per glean repo files to --out-dir. If specified with --glean-repo or --glean-url, merge"
        " results with previous results from --output-bucket, and only write per glean repo files"
        " to --out-dir. Not implemented for --moz-central.",
        action=argparse.BooleanOptionalAction,
        required=False,
    )

    glean_filter = parser.add_mutually_exclusive_group()
    glean_filter.add_argument(
        "--glean-repo",
        help="The Names of Glean Repositories to scrape (may be specified multiple times)."
        " If neither --glean-repo nor --glean-url are specified, scrapes all.",
        type=str,
        dest="glean_repos",
        action="append",
    )
    glean_filter.add_argument(
        "--glean-url",
        help="The URLs of Glean Repositories to scrape (may be specified multiple times)."
        " If neither --glean-repo nor --glean-url are specified, scrapes all.",
        type=str,
        dest="glean_urls",
        action="append",
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
        args.bugzilla_api_key,
        glean_urls=args.glean_urls,
        glean_commit=args.glean_commit,
        glean_commit_branch=args.glean_commit_branch,
        update=args.update,
        glean_limit_date=args.glean_limit_date,
        check_expiry=args.check_expiry,
        check_fog_expiry=args.check_fog_expiry,
    )
