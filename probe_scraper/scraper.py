# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import os
import json
import tempfile
import requests
import requests_cache

from collections import OrderedDict

requests_cache.install_cache('probe_scraper_cache')


REGISTRY_FILES = {
    'histograms': [
        'toolkit/components/telemetry/Histograms.json',
        'dom/base/UseCounters.conf',
        'dom/base/nsDeprecatedOperationList.h',
    ],
    'scalars': [
        'toolkit/components/telemetry/Scalars.yaml',
    ],
    'events': [
        'toolkit/components/telemetry/Events.yaml',
    ],
}

CHANNELS = {
    # 'nightly': 'https://hg.mozilla.org/mozilla-central/',
    'aurora': {
        'base_uri': 'https://hg.mozilla.org/releases/mozilla-aurora/',
        'tag_regex': '^FIREFOX_AURORA_[0-9]+_BASE$'
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
    if not data or "tags" not in data:
        raise Exception("Result JSON doesn't have the right format for " + uri)

    return data["tags"]


def extract_tag_data(tags, channel):
    tag_regex = CHANNELS[channel]['tag_regex']
    tags = filter(lambda t: re.match(tag_regex, t["tag"]), tags)
    results = []

    for tag in tags:
        version = ""
        if channel == "release":
            version = tag["tag"].split('_')[1]
        elif channel in ["beta", "aurora"]:
            version = tag["tag"].split('_')[2]
        else:
            raise Exception("Unsupported channel " + channel)

        if int(version) >= MIN_FIREFOX_VERSION:
            results.append({
                "node": tag["node"],
                "version": version,
            })

    results = sorted(results, key=lambda r: int(r["version"]))
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


def load_error_cache():
    if not os.path.exists(ERROR_CACHE_FILENAME):
        return {}
    with open(ERROR_CACHE_FILENAME, 'r') as f:
        return json.load(f)


def save_error_cache(error_cache):
        with open(ERROR_CACHE_FILENAME, 'w') as f:
            json.dump(error_cache, f, sort_keys=True, indent=2)


def scrape(folder=None):
    """
    Returns data in the format:
    {
      node_id: {
        channel: string,
        version: string,
        registries: {
          histograms: [path, ...]
          events: [path, ...]
          scalars: [path, ...]
        }
      },
      ...
    }
    """
    if folder is None:
        folder = tempfile.mkdtemp()
    error_cache = load_error_cache()
    results = OrderedDict()

    for channel in CHANNELS.iterkeys():
        tags = load_tags(channel)
        versions = extract_tag_data(tags, channel)
        save_error_cache(error_cache)

        print "\n" + channel + " - extracted version data:"
        for v in versions:
            print "  " + str(v)

        print "\n" + channel + " - loading files:"
        for v in versions:
            print "  from: " + str(v)
            results[v['node']] = {
                'channel': channel,
                'version': v['version'],
                'registries': download_files(channel, v['node'], folder, error_cache),
            }
            save_error_cache(error_cache)

    return results
