# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pathlib import Path

from glean_parser.parser import parse_objects

from .pings import normalize_ping_name
from .utils import get_source_url


class GleanMetricsParser:
    """
    Use the [Glean Parser]
    (https://mozilla.github.io/glean_parser)
    to parse the metrics.yaml files.
    """

    def parse(self, filenames, config, repo_url=None, commit_hash=None):
        config = config.copy()
        config["do_not_disable_expired"] = True

        paths = [Path(fname) for fname in filenames]
        paths = [path for path in paths if path.is_file()]
        results = parse_objects(paths, config)
        errors = [err for err in results]

        metrics = {
            metric.identifier(): metric.serialize()
            for category, probes in results.value.items()
            for probe_name, metric in probes.items()
        }

        for v in metrics.values():
            v["send_in_pings"] = [normalize_ping_name(p) for p in v["send_in_pings"]]
            if repo_url and commit_hash:
                v["source_url"] = get_source_url(v["defined_in"], repo_url, commit_hash)
            # the 'defined_in' structure is no longer needed
            del v["defined_in"]
        return metrics, errors
