from probe_scraper.parsers.histograms import HistogramsParser


def is_string(s):
    return isinstance(s, str)


def histogram_parser(version, usecounter_optout):
    FILES = [
        "tests/resources/Histograms.json",
        "tests/resources/nsDeprecatedOperationList.h",
        "tests/resources/UseCounters.conf",
    ]

    HISTOGRAMS = [
        "TELEMETRY_TEST_FLAG",
        "TELEMETRY_TEST_COUNT",
        "TELEMETRY_TEST_COUNT2",
        "TELEMETRY_TEST_COUNT_INIT_NO_RECORD",
        "TELEMETRY_TEST_CATEGORICAL",
        "TELEMETRY_TEST_CATEGORICAL_OPTOUT",
        "TELEMETRY_TEST_CATEGORICAL_NVALUES",
        "TELEMETRY_TEST_CATEGORICAL_EMPTY_LABELS",
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
        "TELEMETRY_TEST_ALL_CHILDREN",
        "TELEMETRY_TEST_ALL_CHILDS",
        "EXPRESSION_IN_LOW_HIGH_ATTRIBUTE",
        "NON_INTEGER_IN_HIGH_ATTRIBUTE",
        "HISTOGRAM_WITH_MULTISTORE",
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
    parsed_histograms = parser.parse(FILES, version)

    # Check that all expected histogram keys are present.
    ALL_KEYS = HISTOGRAMS + USE_COUNTERS + DEPRECATED_OPERATIONS
    assert set(ALL_KEYS) == set(parsed_histograms.keys())

    # Make sure each of them contains all the required fields and details.
    REQUIRED_FIELDS = [
        "cpp_guard",
        "description",
        "details",
        "expiry_version",
        "optout",
        "bug_numbers",
    ]

    REQUIRED_DETAILS = [
        "low",
        "high",
        "keyed",
        "kind",
        "n_buckets",
        "record_in_processes",
        "record_into_store",
    ]

    for name, data in parsed_histograms.items():
        assert is_string(name)

        # Check that we have all the required fields for each probe.
        for field in REQUIRED_FIELDS:
            assert field in data

        # Check that we have all the needed details.
        for field in REQUIRED_DETAILS:
            assert field in data["details"]

        # If multiple stores set, they should be both listed
        if name == "HISTOGRAM_WITH_MULTISTORE":
            assert ["main", "store2"] == data["details"]["record_into_store"]
        else:
            # Default multistore if unspecified is just "main"
            assert ["main"] == data["details"]["record_into_store"]

        # Categorical histograms should have a non-empty `details["labels"]`.
        if data["details"]["kind"] == "categorical":
            assert "labels" in data["details"].keys() and isinstance(
                data["details"]["labels"], list
            )
        else:
            assert "labels" not in data["details"].keys()

        if name.startswith("USE_COUNTER2_"):
            assert data["optout"] == usecounter_optout


# Test for an old Firefox version.
def test_histogram_parser_old():
    histogram_parser("55", usecounter_optout=False)


# Test for a newer Firefox version with Use Counters on release
def test_histogram_parser_new():
    histogram_parser("70", usecounter_optout=True)
