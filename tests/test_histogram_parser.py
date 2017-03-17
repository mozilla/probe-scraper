from probe_scraper.parsers.histograms import HistogramsParser


def is_string(s):
    return isinstance(s, basestring)


def test_histogram_parser():
    # Parse the histograms from the test definitions.
    parser = HistogramsParser()
    parsed_histograms = parser.parse(["tests/Histograms.json"])

    # Make sure each of them contains all the required fields and details.
    REQUIRED_FIELDS = [
        "cpp_guard", "description", "details", "expiry_version", "optout"
    ]

    REQUIRED_DETAILS = [
        "low", "high", "keyed", "kind", "n_buckets"
    ]

    for name, data in parsed_histograms.iteritems():
        assert is_string(name)

        # Check that we have all the required fields for each probe.
        for field in REQUIRED_FIELDS:
            assert field in data

        # Check that we have all the needed details.
        for field in REQUIRED_DETAILS:
            assert field in data['details']
