from probe_scraper.parsers.events import EventsParser


def is_string(s):
    return isinstance(s, basestring)


def test_event_parser():
    # Parse the events from the test definitions.
    parser = EventsParser()
    parsed_events = parser.parse(["tests/test_events.yaml"], "55")

    # Make sure we loaded all the events.
    assert len(parsed_events) == 4

    # Make sure each of them contains all the required fields and details.
    REQUIRED_FIELDS = ["cpp_guard", "description", "details", "expiry_version", "optout"]
    REQUIRED_DETAILS = ["methods", "objects", "extra_keys", "record_in_processes"]

    for name, data in parsed_events.iteritems():
        assert is_string(name)

        # Make sure we have all the required fields and details.
        for field in REQUIRED_FIELDS:
            assert field in data

        for field in REQUIRED_DETAILS:
            assert field in data["details"]
