from probe_scraper.parsers.scalars import ScalarsParser


def is_string(s):
    return isinstance(s, str)


def test_scalar_parser():
    # Parse the histograms from the test definitions.
    parser = ScalarsParser()
    parsed_scalars = parser.parse(["tests/test_scalars.yaml"], "55")

    # Make sure we loaded all the scalars.
    assert len(parsed_scalars) == 16

    # Make sure each of them contains all the required fields and details.
    REQUIRED_FIELDS = [
        "cpp_guard", "description", "details", "expiry_version", "optout", "bug_numbers"
    ]
    REQUIRED_DETAILS = [
        "keyed", "kind", "record_in_processes"
    ]

    for name, data in parsed_scalars.items():
        assert is_string(name)

        # Make sure we have all the required fields and details.
        for field in REQUIRED_FIELDS:
            assert field in data

        for field in REQUIRED_DETAILS:
            assert field in data["details"]
