from probe_scraper import runner
from copy import deepcopy
from datetime import datetime


def test_add_first_appeared_dates():
    probes_by_channel = {
        "all": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {
                        "revisions": {
                            "first": "rev-a",
                            "last": "rev-b"
                        }
                    },
                    "release": {
                        "revisions": {
                            "first": "rev-c",
                            "last": "rev-d"
                        }
                    }
                }
            }
        },
        "nightly": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {
                        "revisions": {
                            "first": "rev-a",
                            "last": "rev-b"
                        }
                    }
                }
            }
        },
        "release": {
            "histogram/test_int_histogram": {
                "history": {
                    "release": {
                        "revisions": {
                            "first": "rev-c",
                            "last": "rev-d"
                        }
                    }
                }
            }
        }
    }

    first_appeared_dates = {
        "histogram/test_int_histogram": {
            "release": datetime(2019, 1, 1, 0, 0, 0),
            "nightly": datetime(2018, 12, 1, 0, 0, 0)
        }
    }

    expected = deepcopy(probes_by_channel)
    expected["all"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
        "nightly": "2018-12-01 00:00:00"
    }
    expected["release"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
    }
    expected["nightly"]["histogram/test_int_histogram"]["first_added"] = {
        "nightly": "2018-12-01 00:00:00"
    }

    assert runner.add_first_appeared_dates(probes_by_channel, first_appeared_dates) == expected
