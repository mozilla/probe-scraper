# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from third_party import parse_scalars


def extract_scalar_data(s):
    return {
        "description": s.description,
        "expiry_version": s.expires,
        "cpp_guard": s.cpp_guard,
        "optout": s.dataset.endswith('_OPTOUT'),
        "details": {
            "keyed": s.keyed,
            "kind": s.kind,
            "record_in_processes": s.record_in_processes
        }
    }


def transform_scalar_info(probes):
    return dict((probe.label, extract_scalar_data(probe)) for probe in probes)


class ScalarsParser:
    def parse(self, filenames, version):
        if len(filenames) > 1:
            raise Exception('We don\'t support loading from more than one file.')

        scalars = parse_scalars.load_scalars(filenames[0])

        # Get the probe information in a standard format.
        return transform_scalar_info(scalars)
