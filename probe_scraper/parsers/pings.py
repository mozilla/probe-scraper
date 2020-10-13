# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pathlib import Path

from glean_parser.parser import parse_objects

PING_NAME_NORMALIZATION = {
    "deletion_request": "deletion-request",
    "bookmarks_sync": "bookmarks-sync",
    "history_sync": "history-sync",
    "session_end": "session-end",
}


def normalize_ping_name(name):
    return PING_NAME_NORMALIZATION.get(name, name)


class GleanPingsParser:
    """
    Use the [Glean Parser]
    (https://mozilla.github.io/glean_parser)
    to parse the pings.yaml files.
    """

    def parse(self, filenames, config):
        config = config.copy()
        paths = [Path(fname) for fname in filenames]
        paths = [path for path in paths if path.is_file()]
        results = parse_objects(paths, config)
        errors = [err for err in results]

        return (
            {
                normalize_ping_name(ping_name): ping_data.serialize()
                for category, pings in results.value.items()
                for ping_name, ping_data in pings.items()
            },
            errors,
        )
