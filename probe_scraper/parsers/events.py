# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .third_party import parse_events
from .utils import set_in_nested_dict, get_major_version


def extract_events_data(e):
    props = {
        # source_field: target_field

        # TODO: extract description.
        "description": "description",
        "expiry_version": "expiry_version",
        "expiry_day": "expiry_day",
        "cpp_guard": "cpp_guard",
        "bug_numbers": "bug_numbers",

        "methods": "details/methods",
        "objects": "details/objects",
        "record_in_processes": "details/record_in_processes",
        # TODO: extract key descriptions too.
        "extra_keys": "details/extra_keys",
    }

    defaults = {
        "expiry_version": "never",
        "expiry_day": "never",
        "name": e.methods[0],
        "description": e.description,
        "cpp_guard": None,
        "bug_numbers": [],
    }

    data = {
        "details": {}
    }

    for source_field, target_field in props.items():
        value = getattr(e, source_field, e._definition.get(source_field, None))
        if value is None and source_field in defaults:
            value = defaults[source_field]
        set_in_nested_dict(data, target_field, value)

    # We only care about opt-out or opt-in really.
    optout = getattr(e, "dataset", "").endswith('_OPTOUT')
    data["optout"] = optout

    # Normalize some field values.
    data["expiry_version"] = get_major_version(data["expiry_version"])
    if data["expiry_version"] == "default":
        data["expiry_version"] = "never"

    return data


class EventsParser:
    def parse(self, filenames, version=None, channel=None):
        # Events.yaml had a format change in 53, see bug 1329620.
        # We don't have important event usage yet, so lets skip
        # backwards compatibility for now.
        if (version and channel) and (
          ((channel != "nightly" and version < 53)
           or (channel == "nightly" and version < 54))):
            return {}

        if len(filenames) > 1:
            raise Exception('We don\'t support loading from more than one file.')

        events = parse_events.load_events(filenames[0], strict_type_checks=False)

        # Get the probe information in a standard format.
        out = {}
        for e in events:
            full_name = e.category + "." + e.methods[0]
            if getattr(e, "name", None):
                full_name += "#" + e.name
            out[full_name] = extract_events_data(e)

        return out
