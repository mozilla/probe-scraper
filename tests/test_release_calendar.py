from dataclasses import dataclass
from unittest import mock
import datetime
import json

from probe_scraper.scrapers import release_calendar


@mock.patch("requests.get")
def test_get_release_date(requests_get):
    @dataclass
    class ResponseWrapper:
        json_value: str

        def json(self):
            return self.json_value

    with open("tests/resources/release_calendar.json") as f:
        requests_get.return_value = ResponseWrapper(json.load(f))

    release_dates = release_calendar.get_release_dates()

    expected = {
        "nightly": {
            "76": datetime.date(2020, 3, 10),
            "75": datetime.date(2020, 2, 11),
            "69": datetime.date(2019, 5, 21),
            "68": datetime.date(2019, 3, 19),
        },
        "beta": {
            "75": datetime.date(2020, 3, 10),
            "74": datetime.date(2020, 2, 11),
            "68": datetime.date(2019, 5, 21),
            "67": datetime.date(2019, 3, 19),
        },
        "release": {
            "74": datetime.date(2020, 3, 10),
            "73": datetime.date(2020, 2, 11),
            "67": datetime.date(2019, 5, 21),
            "66": datetime.date(2019, 3, 19),
        },
    }

    assert release_dates == expected
