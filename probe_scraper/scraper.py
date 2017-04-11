# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import os
import json
import tempfile
import requests
import requests_cache

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
        'tag_regex': '^FIREFOX_AURORA_[0-9]+_BASE$',
    },
    'aurora': {
        'base_uri': 'https://hg.mozilla.org/releases/mozilla-aurora/',
        'tag_regex': '^FIREFOX_AURORA_[0-9]+_BASE$',
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


def extract_tag_data(tag_data, channel):
    tag_regex = CHANNELS[channel]['tag_regex']
    tip_node_id = tag_data["node"]
    tags = filter(lambda t: re.match(tag_regex, t["tag"]), tag_data["tags"])
    results = []

    for tag in tags:
        version = ""
        if channel == "release":
            version = tag["tag"].split('_')[1]
        elif channel in ["beta", "aurora", "nightly"]:
            version = tag["tag"].split('_')[2]
        else:
            raise Exception("Unsupported channel " + channel)

        # We work with tags that are the start of version N.
        # We want to treat those revisions as the end of version N-1 instead.
        # Nightly only has tags of the type FIREFOX_AURORA_NN_BASE, so it doesn't
        # need this.
        if channel != "nightly":
            version = str(int(version) - 1)

        if int(version) >= MIN_FIREFOX_VERSION:
            results.append({
                "node": tag["node"],
                "version": version,
            })

    results = sorted(results, key=lambda r: int(r["version"]))

    # Add tip revision.
    if tip_node_id != results[-1]["node"]:
        latest_version = str(int(results[-1]["version"]) + 1)
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

    all_files = [(k, x) for k, l in REGISTRY_FILES.items() for x in l]
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


def scrape(folder=None):
    """
    Returns data in the format:
    {
      node_id: {
        channels: [channel_name, ...],
        version: string,
        registries: {
          histogram: [path, ...]
          event: [path, ...]
          scalar: [path, ...]
        }
      },
      ...
    }
    """
    if folder is None:
        folder = tempfile.mkdtemp()
    error_cache = load_error_cache(folder)
    requests_cache.install_cache(os.path.join(folder, 'probe_scraper_cache'))
    results = defaultdict(dict)

    for channel in CHANNELS.iterkeys():
        tags = load_tags(channel)
        versions = extract_tag_data(tags, channel)
        save_error_cache(folder, error_cache)

        print "\n" + channel + " - extracted version data:"
        for v in versions:
            print "  " + str(v)

        print "\n" + channel + " - loading files:"
        for v in versions:
            print "  from: " + str(v)
            files = download_files(channel, v['node'], folder, error_cache)
            results[channel][v['node']] = {
                'channel': channel,
                'version': v['version'],
                'registries': files,
            }
            save_error_cache(folder, error_cache)

    return results
