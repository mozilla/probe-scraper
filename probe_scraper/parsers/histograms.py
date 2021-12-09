# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .third_party import histogram_tools
from .utils import get_major_version, set_in_nested_dict


def extract_histogram_data(histogram, version):
    props = {
        # source_field: target_field
        "cpp_guard": "cpp_guard",
        "description": "description",
        "expiration": "expiry_version",
        "bug_numbers": "bug_numbers",
        "alert_emails": "notification_emails",
        "n_buckets": "details/n_buckets",
        "low": "details/low",
        "high": "details/high",
        "keyed": "details/keyed",
        "kind": "details/kind",
        "record_in_processes": "details/record_in_processes",
        "record_into_store": "details/record_into_store",
    }

    defaults = {
        "cpp_guard": None,
        "keyed": False,
        "expiration": "never",
        "bug_numbers": [],
        "alert_emails": [],
    }

    data = {"details": {}}

    for source_field, target_field in props.items():
        value = None
        if hasattr(histogram, source_field):
            value = getattr(histogram, source_field)()
        elif source_field in histogram._definition:
            value = histogram._definition.get(source_field)
        elif source_field in defaults:
            value = defaults[source_field]
        set_in_nested_dict(data, target_field, value)

    # Only include labels if the histogram is categorical.
    if histogram.kind() == "categorical":
        set_in_nested_dict(data, "details/labels", histogram.labels())

    # We only care about opt-out or opt-in really.
    optout = False
    if hasattr(histogram, "dataset"):
        optout = getattr(histogram, "dataset")().endswith("_OPTOUT")

    # Use Counters are shipped on release since 65.
    # If the parsers would set this flag, we couldn't differentiate between versions.
    if int(version) >= 65:
        if histogram.name().startswith("USE_COUNTER2_"):
            optout = True

    data["optout"] = optout

    # Normalize some field values.
    data["expiry_version"] = get_major_version(data["expiry_version"])
    if data["expiry_version"] == "default":
        data["expiry_version"] = "never"
    if data["details"]["keyed"] == "true":
        data["details"]["keyed"] = True

    # TODO: Fixup old non-number values & expressions.
    # History: bug 920169, bug 1245910
    # "JS::gcreason::NUM_TELEMETRY_REASONS"
    # "JS::gcreason::NUM_TELEMETRY_REASONS+1"
    # "mozilla::StartupTimeline::MAX_EVENT_ID"

    return data


def transform_probe_info(probes, version):
    return dict(
        (probe.name(), extract_histogram_data(probe, version)) for probe in probes
    )


class HistogramsParser:
    def parse(self, filenames, version=None, channel=None):
        # Call the histogram tools for each file.
        parsed_probes = list(histogram_tools.from_files(filenames))

        # Get the probe information in a standard format.
        return transform_probe_info(parsed_probes, version)
