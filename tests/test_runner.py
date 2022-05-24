from copy import deepcopy
from datetime import datetime

from probe_scraper import runner


def test_add_first_appeared_dates():
    probes_by_channel = {
        "all": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {"revisions": {"first": "rev-a", "last": "rev-b"}},
                    "release": {"revisions": {"first": "rev-c", "last": "rev-d"}},
                }
            }
        },
        "nightly": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {"revisions": {"first": "rev-a", "last": "rev-b"}}
                }
            }
        },
        "release": {
            "histogram/test_int_histogram": {
                "history": {
                    "release": {"revisions": {"first": "rev-c", "last": "rev-d"}}
                }
            }
        },
    }

    first_appeared_dates = {
        "histogram/test_int_histogram": {
            "release": datetime(2019, 1, 1, 0, 0, 0),
            "nightly": datetime(2018, 12, 1, 0, 0, 0),
        }
    }

    expected = deepcopy(probes_by_channel)
    expected["all"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
        "nightly": "2018-12-01 00:00:00",
    }
    expected["release"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
    }
    expected["nightly"]["histogram/test_int_histogram"]["first_added"] = {
        "nightly": "2018-12-01 00:00:00"
    }

    assert (
        runner.add_first_appeared_dates(probes_by_channel, first_appeared_dates)
        == expected
    )


def test_trailing_space(tmp_path):
    """Test cases to check the output of json.dumps has no trailing spaces"""
    test_cases = [
        {
            "test1": 12,
            "test2": 31,
        }
    ]

    DIR_NAME = tmp_path / "output_dir"
    FILE_NAME = "file_name.txt"

    for test_case in test_cases:
        trailing_spaces = 0  # Counts no of trailing spaces
        runner.dump_json(test_case, DIR_NAME, FILE_NAME)
        path = DIR_NAME / FILE_NAME
        with open(path, "r") as file:
            for line in file.readline():
                if line[-1] == " ":
                    trailing_spaces += 1

        assert not trailing_spaces
