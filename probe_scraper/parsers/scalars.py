# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .third_party import parse_scalars
from .utils import get_major_version


def extract_scalar_data(s):

    # External scalars.yaml files have release/prerelease, not opt-in/opt-out
    try:
        optout = s.dataset.endswith("_OPTOUT")
    except KeyError:
        optout = s._definition.get("collect_on_channels", "prerelease") == "release"

    return {
        "description": s.description,
        "expiry_version": get_major_version(s.expires),
        "cpp_guard": s.cpp_guard,
        "optout": optout,
        "bug_numbers": s.bug_numbers,
        "notification_emails": s.notification_emails,
        "details": {
            "keyed": s.keyed,
            "kind": s.kind,
            "record_in_processes": s.record_in_processes,
            "record_into_store": s.record_into_store,
        },
    }


def transform_scalar_info(probes):
    return dict((probe.label, extract_scalar_data(probe)) for probe in probes)


class ScalarsParser:
    def parse(self, filenames, version=None, channel=None):
        if len(filenames) > 1:
            raise Exception("We don't support loading from more than one file.")

        scalars = parse_scalars.load_scalars(filenames[0], strict_type_checks=False)

        # Get the probe information in a standard format.
        return transform_scalar_info(scalars)
