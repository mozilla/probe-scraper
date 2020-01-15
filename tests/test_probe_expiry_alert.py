from dataclasses import dataclass
from collections import defaultdict
from unittest import mock
import datetime

from probe_scraper import probe_expiry_alert

CURRENT_VERSIONS = {
    "nightly": "76",
    "beta": "75",
    "release": "74",
}


@dataclass
class ResponseWrapper:
    json_value: dict

    def json(self):
        return self.json_value


def test_find_expiring_probes_no_expiring():
    probes = {
        "p1": {
            "history": {
                "beta": [{
                    "expiry_version": "never"
                }],
                "release": [{
                    "expiry_version": "never"
                }],
            }
        }
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        CURRENT_VERSIONS,
    )
    expected = {}
    assert expiring_probes == expected


def test_find_expiring_probes_expiring():
    probes = {
        "p1": {
            "name": "p1",
            "history": {
                "beta": [{
                    "expiry_version": "75",
                    "notification_emails": ["test@email.com"],
                }],
                "release": [{
                    "expiry_version": "75",
                    "notification_emails": ["test@email.com"],
                }],
            },
        },
        "p2": {
            "name": "p2",
            "history": {
                "beta": [{
                    "expiry_version": "73",
                    "notification_emails": ["test@email.com"],
                }],
                "release": [{
                    "expiry_version": "74"
                }],
            },
        }
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        CURRENT_VERSIONS,
    )
    expected = {
        "beta": {
            "p1": ["test@email.com", probe_expiry_alert.DEFAULT_TO_EMAIL]
        },
        "release": {
            "p2": [probe_expiry_alert.DEFAULT_TO_EMAIL]
        },
    }
    assert expiring_probes == expected


def test_find_expiring_probes_use_latest_revision():
    probes = {
        "p1": {
            "name": "p1",
            "history": {
                "beta": [
                    {
                        "expiry_version": "75"
                    },
                    {
                        "expiry_version": "never"
                    },
                ],
            },
        }
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        probes,
        CURRENT_VERSIONS,
    )
    expected = {
        "beta": {
            "p1": [probe_expiry_alert.DEFAULT_TO_EMAIL]
        }
    }
    assert expiring_probes == expected


@mock.patch("boto3.client")
def test_send_email_dryrun_doesnt_send(mock_boto_client):
    expiring_probes = {
        "beta": {
            "p1": ["email"]
        }
    }
    probe_expiry_alert.send_emails_for_expiring_probes(
        {},
        expiring_probes,
        CURRENT_VERSIONS,
        dryrun=False,
    )
    # make sure send_raw_email is the right method
    mock_boto_client().send_raw_email.assert_called_once()

    probe_expiry_alert.send_emails_for_expiring_probes(
        {},
        expiring_probes,
        CURRENT_VERSIONS,
        dryrun=True,
    )
    mock_boto_client().send_raw_email.assert_called_once()


@mock.patch("probe_scraper.emailer.send_ses")
def test_send_email(mock_send_email):
    expired_probes = {
        "nightly": {
            "expired_probe_2": ["email1"],
        },
        "release": {
            "expired_probe_1": ["email1", "email2"]
        },
    }
    expiring_probes = {
        "beta": {
            "expiring_probe_1": ["email1"],
            "expiring_probe_2": ["email1"],
        },
        "release": {
            "expiring_probe_2": ["email1", "email2"]
        },
    }

    send_email_args = defaultdict(list)

    def update_call_args(*args, **kwargs):
        send_email_args[kwargs["recipients"]].append(kwargs["body"])

    mock_send_email.side_effect = update_call_args

    probe_expiry_alert.send_emails_for_expiring_probes(
        expired_probes,
        expiring_probes,
        CURRENT_VERSIONS,
        dryrun=True,
    )

    assert mock_send_email.call_count == 2

    assert "email1" in send_email_args.keys()
    assert "email2" in send_email_args.keys()
    assert len(send_email_args["email1"]) == 1
    assert len(send_email_args["email2"]) == 1

    email_body = send_email_args["email2"][0]
    assert email_body.count("expiring_probe_1") == 0
    assert email_body.count("expiring_probe_2") == 1
    assert email_body.count("expired_probe_1") == 1
    assert email_body.count("expired_probe_2") == 0

    email_body = send_email_args["email1"][0]
    assert email_body.count("expiring_probe_1") == 1
    assert email_body.count("expiring_probe_2") == 2
    assert email_body.count("expired_probe_1") == 1
    assert email_body.count("expired_probe_2") == 1


@mock.patch("requests.get")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_firefox_versions")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails_for_expiring_probes")
def test_main_runs_once_per_week(mock_send_emails, mock_get_versions, mock_requests_get):
    mock_requests_get.return_value = ResponseWrapper({})
    mock_get_versions.return_value = CURRENT_VERSIONS
    for weekday in range(7):
        base_date = datetime.date(2020, 1, 1)
        probe_expiry_alert.main(base_date + datetime.timedelta(days=weekday), True)

    mock_send_emails.assert_called_once()


@mock.patch("requests.get")
@mock.patch("probe_scraper.probe_expiry_alert.get_latest_firefox_versions")
@mock.patch("probe_scraper.probe_expiry_alert.send_emails_for_expiring_probes")
def test_main_run(mock_send_emails, mock_get_versions, mock_requests_get):
    probes = {
        "p1": {
            "name": "p1",
            "history": {
                "beta": [{
                    "expiry_version": "76",
                    "notification_emails": ["test@email.com"]
                }]
            },
        },
        "p2": {
            "name": "p2",
            "history": {
                "beta": [{
                    "expiry_version": "75",
                    "notification_emails": ["test@email.com"],
                }]
            },
        }
    }
    mock_requests_get.return_value = ResponseWrapper(probes)
    mock_get_versions.return_value = CURRENT_VERSIONS

    probe_expiry_alert.main(datetime.date(2020, 1, 7), True)

    expected_expired_probes = {
        "beta": {
            "p2": [
                "test@email.com",
                probe_expiry_alert.DEFAULT_TO_EMAIL,
            ]
        }
    }
    expected_expiring_probes = {
        "beta": {
            "p1": [
                "test@email.com",
                probe_expiry_alert.DEFAULT_TO_EMAIL,
            ]
        }
    }
    mock_send_emails.assert_called_once_with(
        expected_expired_probes, expected_expiring_probes, mock.ANY, True)
