# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import os
import json
import tempfile
import requests
import requests_cache
from .buildhub import Buildhub

from collections import defaultdict


REGISTRY_FILES = {
    'histogram': [
        'toolkit/components/telemetry/Histograms.json',
        'dom/base/UseCounters.conf',
        'dom/base/nsDeprecatedOperationList.h',
    ],
    'scalar': [
        'toolkit/components/telemetry/Scalars.yaml',
    ],
    'event': [
        'toolkit/components/telemetry/Events.yaml',
    ],
}

CHANNELS = {
    'nightly': {
        'base_uri': 'https://hg.mozilla.org/mozilla-central/',
        'tag_regex': '^FIREFOX_(AURORA|BETA)_[0-9]+_BASE$',
    },
    'beta': {
        'base_uri': 'https://hg.mozilla.org/releases/mozilla-beta/',
        'tag_regex': '^FIREFOX_BETA_[0-9]+_BASE$',
    },
    'release': {
        'base_uri': 'https://hg.mozilla.org/releases/mozilla-release/',
        'tag_regex': '^FIREFOX_[0-9]+_0_RELEASE$',
    },
}

MIN_FIREFOX_VERSION = 30
ERROR_CACHE_FILENAME = 'probe_scraper_errors_cache.json'


def load_tags(channel):
    uri = CHANNELS[channel]['base_uri'] + "json-tags"
    r = requests.get(uri)
    if r.status_code != requests.codes.ok:
        raise Exception("Request returned status " + str(r.status_code) + " for " + uri)

    content_type = r.headers['content-type']
    if content_type != 'application/json':
        raise Exception("Request didn't return JSON: " + content_type + " (" + uri + ")")

    data = r.json()
    if not data or "node" not in data or "tags" not in data:
        raise Exception("Result JSON doesn't have the right format for " + uri)

    return data


def extract_major_version(version_str):
    """
    Given a version string, e.g. "62.0a1",
    extract the major version as an int.
    """
    search = re.search(r"^(\d+)\.", version_str)
    if search is not None:
        return int(search.group(1))
    else:
        raise Exception("Invalid version string " + version_str)


def extract_tag_version(channel, version_str):
    """
    Given a tag, e.g. FIREFOX_65_0_RELEASE,
    extract the major version as an int.
    """
    if channel == "release":
        return int(version_str.split('_')[1])
    elif channel in ["beta", "nightly"]:
        return int(version_str.split('_')[2])
    else:
        raise Exception("Unsupported channel " + channel)


def adjust_version(channel, version):
    """
    We work with tags that are the start of version N.
    We want to treat those revisions as the end of version N-1 instead.
    Nightly only has tags of the type FIREFOX_AURORA_NN_BASE, so it doesn't
    need this.
    """
    if channel != "nightly":
        return version - 1
    return version


def extract_tag_data(tag_data, channel, min_fx_version, max_fx_version):
    tag_regex = CHANNELS[channel]['tag_regex']
    tip_node_id = tag_data["node"]
    tags = [t for t in tag_data["tags"] if re.match(tag_regex, t["tag"])]
    results = []
    latest_version = -1

    for tag in tags:
        version = extract_tag_version(channel, tag["tag"])
        version = adjust_version(channel, version)
        latest_version = max(version, latest_version)

        if (version >= min_fx_version and
           (max_fx_version is None or version <= max_fx_version)):
            results.append({
                "node": tag["node"],
                "version": version,
            })

    results = sorted(results, key=lambda r: r["version"])
    latest_version += 1

    # Add tip revision, if we're including the most recent version
    if (tip_node_id != results[-1]["node"] and
       (max_fx_version is None or latest_version <= max_fx_version)):
        results.append({
            "node": tip_node_id,
            "version": latest_version,
        })

    return results


def download_files(channel, node, temp_dir, error_cache):
    base_uri = CHANNELS[channel]['base_uri'] + 'raw-file/' + node + '/'
    node_path = os.path.join(temp_dir, 'hg', node)

    results = {}

    def add_result(ptype, disk_path):
        if ptype not in results:
            results[ptype] = []
        results[ptype].append(disk_path)

    all_files = [(k, x) for k, l in list(REGISTRY_FILES.items()) for x in l]
    for (ptype, rel_path) in all_files:
        disk_path = os.path.join(node_path, rel_path)
        if os.path.exists(disk_path):
            add_result(ptype, disk_path)
            continue

        uri = base_uri + rel_path
        # requests_cache doesn't cache on error status codes.
        # We just use our own cache for these for now.
        if uri in error_cache:
            continue

        req = requests.get(uri)
        if req.status_code != requests.codes.ok:
            if os.path.basename(rel_path) == 'Histograms.json':
                raise Exception("Request returned status " + str(req.status_code) + " for " + uri)
            else:
                error_cache[uri] = req.status_code
                continue

        dir = os.path.split(disk_path)[0]
        if not os.path.exists(dir):
            os.makedirs(dir)
        with open(disk_path, 'wb') as f:
            for chunk in req.iter_content(chunk_size=128):
                f.write(chunk)

        add_result(ptype, disk_path)

    return results


def load_error_cache(folder):
    path = os.path.join(folder, ERROR_CACHE_FILENAME)
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return json.load(f)


def save_error_cache(folder, error_cache):
    path = os.path.join(folder, ERROR_CACHE_FILENAME)
    with open(path, 'w') as f:
        json.dump(error_cache, f, sort_keys=True, indent=2)


def scrape(folder=None, min_fx_version=None, max_fx_version=None):
    """
    Returns data in the format:
    {
      <channel>: {
        <revision>: {
          "channel": <channel>,
          "version": <major-version>,
          "registries": {
            "event": [<path>, ...],
            "histogram": [<path>, ...],
            "scalar": [<path>, ...]
          }
        },
        ...
      },
      ...
    }
    """
    if min_fx_version is None:
        min_fx_version = MIN_FIREFOX_VERSION
    if folder is None:
        folder = tempfile.mkdtemp()

    error_cache = load_error_cache(folder)
    requests_cache.install_cache('probe_scraper_cache')
    results = defaultdict(dict)

    for channel in CHANNELS.keys():
        tags = load_tags(channel)
        versions = extract_tag_data(tags, channel, min_fx_version, max_fx_version)
        save_error_cache(folder, error_cache)

        print("\n" + channel + " - extracted version data:")
        for v in versions:
            print("  " + str(v))

        print("\n" + channel + " - loading files:")
        for v in versions:
            print("  from: " + str(v))
            files = download_files(channel, v['node'], folder, error_cache)
            results[channel][v['node']] = {
                'channel': channel,
                'version': v['version'],
                'registries': files,
            }
            save_error_cache(folder, error_cache)

    return results


def scrape_channel_revisions(folder=None, min_fx_version=None, max_fx_version=None, channels=None):
    """
    Returns data in the format:
    {
      <channel>: {
        <revision>: {
          "date": <date>,
          "version": <version>,
          "registries": {
            "histogram": [path, ...],
            "event": [path, ...],
            "scalar": [path, ...]
          }
        }
      },
      ...
    }
    """
    if min_fx_version is None:
        min_fx_version = MIN_FIREFOX_VERSION

    error_cache = load_error_cache(folder)
    bh = Buildhub()
    results = defaultdict(dict)

    if channels is None:
        channels = CHANNELS.keys()

    for channel in channels:

        print("\nRetreiving Buildhub results for channel " + channel)

        revision_dates = bh.get_revision_dates(channel, min_fx_version, max_version=max_fx_version)
        num_revisions = len(revision_dates)

        print("  " + str(num_revisions) + " revisions found")

        for i, rd in enumerate(revision_dates):
            revision = rd["revision"]

            print("  Downloading files for revision number " + str(i+1) + "/" + str(num_revisions))
            files = download_files(channel, revision, folder, error_cache)

            results[channel][revision] = {
                'date': rd['date'],
                'version': extract_major_version(rd["version"]),
                'registries': files
            }

    return results
