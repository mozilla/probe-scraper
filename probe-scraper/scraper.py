# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import re
import os
import json
import tempfile
import requests
import requests_cache

requests_cache.install_cache('probe_scraper_cache')

HISTOGRAM_FILES = [
    'toolkit/components/telemetry/Histograms.json',
    'dom/base/UseCounters.conf',
    'dom/base/nsDeprecatedOperationList.h',
]

SCALAR_FILES = [
    'toolkit/components/telemetry/Scalars.yaml',
]

EVENT_FILES = [
    'toolkit/components/telemetry/Events.yaml',
]

ALL_FILES = HISTOGRAM_FILES + SCALAR_FILES + EVENT_FILES

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


def load_tags(channel):
    uri = CHANNELS[channel]['base_uri'] + "json-tags"
    r = requests.get(uri)
    if r.status_code != requests.codes.ok:
        raise RuntimeError, "Request returned status " + str(r.status_code) + " for " + uri

    ctype = r.headers['content-type']
    if ctype != 'application/json':
        raise RuntimeError, "Request didn't return JSON: " + ctype + " (" + uri + ")"
    
    data = r.json()
    if not data or not "tags" in data:
        raise RuntimeError, "Result JSON doesn't have the right format for " + uri

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
            raise RuntimeError, "Unsupported channel " + channel

        if int(version) >= MIN_FIREFOX_VERSION:
            results.append({
                "node": tag["node"],
                "version": version,
            })

    results = sorted(results, key=lambda r: int(r["version"]))
    return results

def download_files(channel, node, target_dir, error_cache):
    base_uri = CHANNELS[channel]['base_uri'] + 'raw-file/' + node + '/'
    node_path = os.path.join(target_dir, node)
    results = []

    for rel_path in ALL_FILES:
        uri = base_uri + rel_path
        # requests_cache doesn't cache on error status codes.
        # We just use our own cache for these for now.
        if uri in error_cache:
            continue

        req = requests.get(uri)
        if req.status_code != requests.codes.ok:
            if os.path.basename(rel_path) in ['Histograms.json', 'histogram_tools.py']:
                raise RuntimeError, "Request returned status " + str(req.status_code) + " for " + uri
            else:
                error_cache[uri] = req.status_code

        path = os.path.join(node_path, rel_path)
        if not os.path.exists(path):
            dir = os.path.split(path)[0]
            if not os.path.exists(dir):
                os.makedirs(dir)
            with open(path, 'wb') as f:
                for chunk in req.iter_content(chunk_size=128):
                    f.write(chunk)
        results.append(path)

    return results

# returns:
# node_id -> {
#    channel: ,
#    histograms: [path list, ...]
#    events: [path list, ...]
#    scalars: [path list, ...]
# }
def scrape(dir = tempfile.mkdtemp()):
    error_cache = {
        # path -> error code
    }
    if os.path.exists('probe_scraper_errors_cache.json'):
        with open('probe_scraper_errors_cache.json', 'r') as f:
            error_cache = json.load(f)

    def save_cache():
        with open('probe_scraper_errors_cache.json', 'w') as f:
            json.dump(error_cache, f, sort_keys=True, indent=2)

    for channel in CHANNELS.iterkeys():
        tags = load_tags(channel)
        versions = extract_tag_data(tags, channel)
        save_cache()

        print "\n" + channel + " - extracted version data:"
        for v in versions:
            print "  " + str(v)

        print "\n" + channel + " - loading files:"
        for v in versions:
            print "  from: " + str(v)
            download_files(channel, v["node"], dir, error_cache)
            save_cache()

if __name__ == "__main__":
    scrape('_tmp')
