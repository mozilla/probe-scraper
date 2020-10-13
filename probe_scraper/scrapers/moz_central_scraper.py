# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import os
import re
from collections import defaultdict

import requests

from .buildhub import Buildhub

BASE_URI = "https://hg.mozilla.org"

REGISTRY_FILES = {
    "histogram": [
        "toolkit/components/telemetry/Histograms.json",
        "dom/base/UseCounters.conf",
        "dom/base/nsDeprecatedOperationList.h",
        "servo/components/style/properties/counted_unknown_properties.py",
        "devtools/shared/css/generated/properties-db.js",
    ],
    "scalar": [
        "toolkit/components/telemetry/Scalars.yaml",
    ],
    "event": [
        "toolkit/components/telemetry/Events.yaml",
    ],
}


CHANNELS = {
    "nightly": {
        "base_uri": f"{BASE_URI}/mozilla-central",
        "tag_regex": "^FIREFOX_(AURORA|BETA)_[0-9]+_BASE$",
        "artificial_tags": [
            {
                "date": [1567362726.0, 0],
                "node": "fd2934cca1ae7b492f29a4d240915aa9ec5b4977",
                "tag": "FIREFOX_BETA_71_BASE",
            }
        ],
    },
    "beta": {
        "base_uri": f"{BASE_URI}/releases/mozilla-beta",
        "tag_regex": "^FIREFOX_BETA_[0-9]+_BASE$",
    },
    "release": {
        "base_uri": f"{BASE_URI}/releases/mozilla-release",
        "tag_regex": "^FIREFOX_[0-9]+_0_RELEASE$",
    },
}

MIN_FIREFOX_VERSION = 30
ERROR_CACHE_FILENAME = "probe_scraper_errors_cache.json"
ARTIFICIAL_TAG = "artificial"


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


def relative_path_is_in_version(rel_path, version):
    # The devtools file exists in a bunch of versions, but we only care for it
    # since firefox 71 (bug 1578661).
    if (
        rel_path == "devtools/shared/css/generated/properties-db.js"
        or rel_path == "servo/components/style/properties/counted_unknown_properties.py"
    ):
        return version >= 71
    return True


def download_files(channel, node, temp_dir, error_cache, version, tree=None):

    if tree is None:
        uri = CHANNELS[channel]["base_uri"]
    else:
        # mozilla-release and mozilla-beta need to be prefixed with "release/"
        # sometimes they aren't from buildhub, add them if they are missing
        if not tree.startswith("releases/") and tree != "mozilla-central":
            tree = f"releases/{tree}"
        uri = f"{BASE_URI}/{tree}"

    base_uri = f"{uri}/raw-file/{node}/"
    node_path = os.path.join(temp_dir, "hg", node)

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

        if not relative_path_is_in_version(rel_path, int(version)):
            continue

        req = requests.get(uri)
        if req.status_code != requests.codes.ok:
            if os.path.basename(rel_path) == "Histograms.json":
                raise Exception(
                    "Request returned status " + str(req.status_code) + " for " + uri
                )
            else:
                error_cache[uri] = req.status_code
                continue

        dir = os.path.split(disk_path)[0]
        if not os.path.exists(dir):
            os.makedirs(dir)
        with open(disk_path, "wb") as f:
            for chunk in req.iter_content(chunk_size=128):
                f.write(chunk)

        add_result(ptype, disk_path)

    return results


def load_error_cache(folder):
    path = os.path.join(folder, ERROR_CACHE_FILENAME)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def save_error_cache(folder, error_cache):
    path = os.path.join(folder, ERROR_CACHE_FILENAME)
    with open(path, "w") as f:
        json.dump(error_cache, f, sort_keys=True, indent=2, separators=(",", ": "))


def scrape_channel_revisions(
    folder=None, min_fx_version=None, max_fx_version=None, channels=None
):
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

        revision_dates = bh.get_revision_dates(
            channel, min_fx_version, max_version=max_fx_version
        )
        num_revisions = len(revision_dates)

        print("  " + str(num_revisions) + " revisions found")

        for i, rd in enumerate(revision_dates):
            revision = rd["revision"]

            print(
                (
                    f"  Downloading files for revision number {str(i+1)}/{str(num_revisions)}"
                    f" - revision: {revision}, tree: {rd['tree']}, version: {str(rd['version'])}"
                )
            )
            version = extract_major_version(rd["version"])
            files = download_files(
                channel, revision, folder, error_cache, version, tree=rd["tree"]
            )

            results[channel][revision] = {
                "date": rd["date"],
                "version": version,
                "registries": files,
            }
            save_error_cache(folder, error_cache)

    return results
