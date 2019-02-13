from probe_scraper.parsers.events import EventsParser


def is_string(s):
    return isinstance(s, str)


def test_event_parser():
    # Parse the events from the test definitions.
    parser = EventsParser()
    parsed_events = parser.parse(["tests/resources/test_events.yaml"], "55")

    # Make sure we loaded all the events.
    assert len(parsed_events) == 5

    # Make sure each of them contains all the required fields and details.
    REQUIRED_FIELDS = [
      "cpp_guard", "description", "details", "expiry_version", "optout", "bug_numbers"
    ]
    REQUIRED_DETAILS = [
      "methods", "objects", "extra_keys", "record_in_processes"
    ]

    for name, data in parsed_events.items():
        assert is_string(name)

        # Make sure we have all the required fields and details.
        for field in REQUIRED_FIELDS:
            assert field in data

        for field in REQUIRED_DETAILS:
            assert field in data["details"]


def parse(channel, version):
    parser = EventsParser()
    return parser.parse(["tests/test_events.yaml"], version, channel)


def test_channel_version_ignore():
    assert parse("release", 52) == {}
    assert parse("release", 53) != {}

    assert parse("beta", 52) == {}
    assert parse("beta", 53) != {}

    assert parse("nightly", 52) == {}
    assert parse("nightly", 53) == {}
    assert parse("nightly", 54) != {}
