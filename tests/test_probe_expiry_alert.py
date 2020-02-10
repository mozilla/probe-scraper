from dataclasses import dataclass
from collections import defaultdict
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
        "p2": ["test@email.com", probe_expiry_alert.DEFAULT_TO_EMAIL],
        "p3": [probe_expiry_alert.DEFAULT_TO_EMAIL],
    }
    assert expiring_probes == expected


@mock.patch("boto3.client")
def test_send_email_dryrun_doesnt_send(mock_boto_client):
    expiring_probes = {
        "p1": ["email"]
    }
    probe_expiry_alert.send_emails_for_expiring_probes(
        {},
        expiring_probes,
        "75",
        dryrun=False,
    )
    # make sure send_raw_email is the right method
    mock_boto_client().send_raw_email.assert_called_once()

    probe_expiry_alert.send_emails_for_expiring_probes(
        {},
        expiring_probes,
        "75",
        dryrun=True,
    )
    mock_boto_client().send_raw_email.assert_called_once()


@mock.patch("probe_scraper.emailer.send_ses")
def test_send_email(mock_send_email):
    expired_probes = {
        "expired_probe_1": ["email1", "email2"],
        "expired_probe_2": ["email1"],
    }
    expiring_probes = {
        "expiring_probe_1": ["email1"],
        "expiring_probe_2": ["email1"],
    }

    send_email_args = defaultdict(list)

    def update_call_args(*args, **kwargs):
        send_email_args[kwargs["recipients"]].append(kwargs["body"])

    mock_send_email.side_effect = update_call_args

    probe_expiry_alert.send_emails_for_expiring_probes(
        expired_probes,
        expiring_probes,
        "75",
        dryrun=True,
    )

    assert mock_send_email.call_count == 2

    assert "email1" in send_email_args.keys()
    assert "email2" in send_email_args.keys()
    assert len(send_email_args["email1"]) == 1
    assert len(send_email_args["email2"]) == 1

    email_body = send_email_args["email2"][0]
    assert email_body.count("expiring_probe_1") == 0
    assert email_body.count("expiring_probe_2") == 0
    assert email_body.count("expired_probe_1") == 1
    assert email_body.count("expired_probe_2") == 0

    email_body = send_email_args["email1"][0]
    assert email_body.count("expiring_probe_1") == 1
    assert email_body.count("expiring_probe_2") == 1
    assert email_body.count("expired_probe_1") == 1
    assert email_body.count("expired_probe_2") == 1


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails_for_expiring_probes")
def test_main_runs_once_per_week(mock_send_emails, mock_get_version, mock_download_file,
                                 mock_scalars_parser, mock_histograms_parser, mock_events_parser):
    mock_events_parser.return_value = {}
    mock_histograms_parser.return_value = {}
    mock_scalars_parser.return_value = {}
    mock_get_version.return_value = "75"
    for weekday in range(7):
        base_date = datetime.date(2020, 1, 1)
        probe_expiry_alert.main(base_date + datetime.timedelta(days=weekday), False)

    mock_send_emails.assert_called_once()


@mock.patch("probe_scraper.parsers.events.EventsParser.parse")
@mock.patch("probe_scraper.parsers.histograms.HistogramsParser.parse")
@mock.patch("probe_scraper.parsers.scalars.ScalarsParser.parse")
@mock.patch("probe_scraper.probe_expiry_alert.download_file")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_nightly_version")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails_for_expiring_probes")
def test_main_run(mock_send_emails, mock_get_version, mock_download_file,
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

    probe_expiry_alert.main(datetime.date(2020, 1, 8), True)

    expected_expired_probes = {
        "p2": [
            "test@email.com",
            probe_expiry_alert.DEFAULT_TO_EMAIL,
        ]
    }
    expected_expiring_probes = {
        "p1": [
            "test@email.com",
            probe_expiry_alert.DEFAULT_TO_EMAIL,
        ]
    }
    mock_send_emails.assert_called_once_with(
        expected_expired_probes, expected_expiring_probes, mock.ANY, True)
