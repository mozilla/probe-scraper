from probe_scraper.parsers.metrics import MetricsParser


def is_string(s):
    return isinstance(s, str)


def test_metrics_parser():
    # Parse the histograms from the test definitions.
    parser = MetricsParser()
    parsed_metrics, errs = parser.parse(["tests/resources/metrics.yaml"], "55")

    assert errs == []

    # Make sure we loaded all the metrics.
    # Notably, we do not check the contents; that is left up to the
    # glean parser to handle.
    assert len(parsed_metrics) == 2
    for name, data in parsed_metrics.items():
        assert is_string(name)
