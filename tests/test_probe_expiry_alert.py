from dataclasses import dataclass
from unittest import mock
import datetime

from probe_scraper import probe_expiry_alert


@dataclass
class ResponseWrapper:
    text: str

    def text(self):
        return self.text


def test_find_expiring_probes_no_expiring():
    probes = {
        "p1": {
            "expiry_version": "never"
        }
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        "75",
    )
    expected = {}
    assert expiring_probes == expected


def test_find_expiring_probes_channel_no_probes():
    probes = {}
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        "75",
    )
    expected = {}
    assert expiring_probes == expected


def test_find_expiring_probes_expiring():
    probes = {
        "p1": {
            "expiry_version": "74",
            "notification_emails": ["test@email.com"],
        },
        "p2": {
            "expiry_version": "75",
            "notification_emails": ["test@email.com"],
        },
        "p3": {
            "expiry_version": "75"
        },
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        "75",
    )
    expected = {
        "p2": ["test@email.com"],
        "p3": [],
    }
    assert expiring_probes == expected


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.file_bugs")
def test_main_runs_once_per_week(mock_file_bugs, mock_get_version,
                                 mock_download_file, mock_scalars_parser,
                                 mock_histograms_parser, mock_events_parser):
    mock_events_parser.return_value = {}
    mock_histograms_parser.return_value = {}
    mock_scalars_parser.return_value = {}
    mock_get_version.return_value = "75"
    for weekday in range(7):
        base_date = datetime.date(2020, 1, 1)
        probe_expiry_alert.main(base_date + datetime.timedelta(days=weekday), False, "")

    mock_file_bugs.assert_has_calls([mock.call({}, "76", "", create_bugs=False)] * 6 +
                                    [mock.call({}, "76", "", create_bugs=True)],
                                    any_order=True)
    assert mock_file_bugs.call_count == 7


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.file_bugs")
def test_dryrun_doesnt_create_bugs(mock_file_bugs, mock_get_version,
                                 mock_download_file, mock_scalars_parser,
                                 mock_histograms_parser, mock_events_parser):
    mock_events_parser.return_value = {}
    mock_histograms_parser.return_value = {}
    mock_scalars_parser.return_value = {}
    mock_get_version.return_value = "75"
    for weekday in range(7):
        base_date = datetime.date(2020, 1, 1)
        probe_expiry_alert.main(base_date + datetime.timedelta(days=weekday), True, "")

    mock_file_bugs.assert_has_calls([mock.call({}, "76", "", create_bugs=False)] * 7,
                                    any_order=True)
    assert mock_file_bugs.call_count == 7


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.file_bugs")
def test_main_run(mock_file_bugs, mock_get_version, mock_download_file,
                  mock_scalars_parser, mock_histograms_parser, mock_events_parser):
    mock_events_parser.return_value = {
        "p1": {
            "expiry_version": "76",
            "notification_emails": ["test@email.com"],
        }
    }
    mock_histograms_parser.return_value = {
        "p2": {
            "expiry_version": "75",
            "notification_emails": ["test@email.com"],
        }
    }
    mock_scalars_parser.return_value = {
        "p3": {
            "expiry_version": "77",
            "notification_emails": ["test@email.com"],
        }
    }
    mock_get_version.return_value = "75"

    probe_expiry_alert.main(datetime.date(2020, 1, 8), True, "")

    expected_expiring_probes = {
        "p1": [
            "test@email.com",
        ]
    }
    mock_file_bugs.assert_called_once_with(expected_expiring_probes, "76", "", create_bugs=False)


def test_bugs_created_for_new_probes():
    pass


def test_bugs_not_created_for_existing_bugs():
    pass


def test_bugs_do_not_contain_invalid_accounts():
    pass


def test_search_bugs_param_creation():
    pass


def test_create_bug_param_creation():
    pass

