# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from glean_parser.parser import parse_objects
from pathlib import Path


class GleanPingsParser:
    """
    Use the [Glean Parser]
    (https://mozilla.github.io/glean_parser)
    to parse the pings.yaml files.
    """

    def parse(self, filenames, config):
        config = config.copy()
        paths = [Path(fname) for fname in filenames]
        results = parse_objects(paths, config)
        errors = [err for err in results]

        return (
            {
                ping_name: ping_data.serialize()
                for category, pings in results.value.items()
                for ping_name, ping_data in pings.items()
            },
            errors
        )
