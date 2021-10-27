# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from pathlib import Path

from glean_parser.parser import parse_objects

from .utils import get_source_url


class GleanTagsParser:
    """
    Use the [Glean Parser]
    (https://mozilla.github.io/glean_parser)
    to parse tags.yaml files.
    """

    def parse(self, filenames, config, repo_url=None, commit_hash=None):
        config = config.copy()
        paths = [Path(fname) for fname in filenames]
        paths = [path for path in paths if path.is_file()]
        results = parse_objects(paths, config)
        errors = [err for err in results]
        tags = {
            tag_name: tag_data.serialize()
            for tag_name, tag_data in results.value.get("tags", {}).items()
        }

        for v in tags.values():
            if repo_url and commit_hash:
                v["source_url"] = get_source_url(v["defined_in"], repo_url, commit_hash)
            # the 'defined_in' structure is no longer needed
            del v["defined_in"]
        return tags, errors
