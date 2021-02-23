from probe_scraper.parsers.metrics import GleanMetricsParser
import pytest


def is_string(s):
    return isinstance(s, str)


def test_metrics_parser():
    # Parse the histograms from the test definitions.
    parser = GleanMetricsParser()
    parsed_metrics, errs = parser.parse(["tests/resources/metrics.yaml"], {})

    assert errs == []

    # Make sure we loaded all the metrics.
    # Notably, we do not check the contents; that is left up to the
    # glean parser to handle.
    assert len(parsed_metrics) == 2
    for name, data in parsed_metrics.items():
        assert is_string(name)

    # Check that ping names are normalized
    assert "session-end" in parsed_metrics["example.os"]["send_in_pings"]


def test_source_url():
    parser = GleanMetricsParser()
    parsed_metrics, errs = parser.parse(
        ["tests/resources/metrics.yaml"], {}, "test.com/foo", "tests"
    )

    assert (
        parsed_metrics["example.duration"]["source_url"]
        == "test.com/foo/blob/tests/resources/metrics.yaml#L4"
    )
    assert (
        parsed_metrics["example.os"]["source_url"]
        == "test.com/foo/blob/tests/resources/metrics.yaml#L19"
    )
    with pytest.raises(KeyError):
        parsed_metrics["example.os"]["defined_in"]
