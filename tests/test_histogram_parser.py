from probe_scraper.parsers.histograms import HistogramsParser


def is_string(s):
    return isinstance(s, basestring)


def test_histogram_parser():
    FILES = [
        "tests/Histograms.json",
        "tests/nsDeprecatedOperationList.h",
        "tests/UseCounters.conf",
    ]

    HISTOGRAMS = [
        "TELEMETRY_TEST_FLAG",
        "TELEMETRY_TEST_COUNT",
        "TELEMETRY_TEST_COUNT2",
        "TELEMETRY_TEST_COUNT_INIT_NO_RECORD",
        "TELEMETRY_TEST_CATEGORICAL",
        "TELEMETRY_TEST_CATEGORICAL_OPTOUT",
        "TELEMETRY_TEST_CATEGORICAL_NVALUES",
        "TELEMETRY_TEST_KEYED_COUNT_INIT_NO_RECORD",
        "TELEMETRY_TEST_KEYED_FLAG",
        "TELEMETRY_TEST_KEYED_COUNT",
        "TELEMETRY_TEST_KEYED_BOOLEAN",
        "TELEMETRY_TEST_RELEASE_OPTOUT",
        "TELEMETRY_TEST_RELEASE_OPTIN",
        "TELEMETRY_TEST_KEYED_RELEASE_OPTIN",
        "TELEMETRY_TEST_KEYED_RELEASE_OPTOUT",
        "TELEMETRY_TEST_EXPONENTIAL",
        "TELEMETRY_TEST_LINEAR",
        "TELEMETRY_TEST_BOOLEAN",
        "TELEMETRY_TEST_EXPIRED",
    ]

    USE_COUNTERS = [
        "USE_COUNTER2_SVGSVGELEMENT_GETELEMENTBYID_DOCUMENT",
        "USE_COUNTER2_SVGSVGELEMENT_GETELEMENTBYID_PAGE",
        "USE_COUNTER2_SVGSVGELEMENT_CURRENTSCALE_getter_DOCUMENT",
        "USE_COUNTER2_SVGSVGELEMENT_CURRENTSCALE_getter_PAGE",
        "USE_COUNTER2_SVGSVGELEMENT_CURRENTSCALE_setter_DOCUMENT",
        "USE_COUNTER2_SVGSVGELEMENT_CURRENTSCALE_setter_PAGE",
        "USE_COUNTER2_PROPERTY_FILL_DOCUMENT",
        "USE_COUNTER2_PROPERTY_FILL_PAGE",
    ]

    DEPRECATED_OPERATIONS = [
        "USE_COUNTER2_DEPRECATED_GetAttributeNode_DOCUMENT",
        "USE_COUNTER2_DEPRECATED_GetAttributeNode_PAGE",
        "USE_COUNTER2_DEPRECATED_SetAttributeNode_DOCUMENT",
        "USE_COUNTER2_DEPRECATED_SetAttributeNode_PAGE",
    ]

    # Parse the histograms from the test definitions.
    parser = HistogramsParser()
    parsed_histograms = parser.parse(FILES, "55")

    # Check that all expected histogram keys are present.
    ALL_KEYS = HISTOGRAMS + USE_COUNTERS + DEPRECATED_OPERATIONS
    assert set(ALL_KEYS) == set(parsed_histograms.iterkeys())

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
