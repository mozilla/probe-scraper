# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from glean_parser.parser import parse_metrics
from pathlib import Path


class MetricsParser:
    def parse(self, filenames, config):
        paths = [Path(fname) for fname in filenames]
        results = parse_metrics(paths, config)
        errors = [err for err in results]

        return (
            {
                metric.identifier(): metric.serialize()
                for category, probes in results.value.items()
                for probe_name, metric in probes.items()
            },
            errors
        )
