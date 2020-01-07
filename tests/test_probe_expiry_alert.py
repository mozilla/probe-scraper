from collections import defaultdict
from unittest import mock
import datetime

from probe_scraper import probe_expiry_alert

RELEASE_DATES = {
    "nightly": {
        "76": datetime.date(2020, 3, 10)
    },
    "beta": {
        "75": datetime.date(2020, 3, 10)
    },
    "release": {
        "74": datetime.date(2020, 3, 10)
    },
}


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
        datetime.date(2020, 3, 10),
        probes,
        RELEASE_DATES,
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
        datetime.date(2020, 3, 10),
        probes,
        RELEASE_DATES,
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
        datetime.date(2020, 3, 10),
        probes,
        RELEASE_DATES,
    )
    expected = {
        "beta": {
            "p1": [probe_expiry_alert.DEFAULT_TO_EMAIL]
        }
    }
    assert expiring_probes == expected


def test_find_expiring_probes_version_not_found():
    probes = {
        "p1": {
            "history": {
                "beta": [{
                    "expiry_version": "10000"
                }]
            }
        }
    }
    expiring_probes = probe_expiry_alert.find_expiring_probes(
        datetime.date(2020, 3, 10),
        probes,
        RELEASE_DATES,
    )
    expected = {}
    assert expiring_probes == expected


@mock.patch("boto3.client")
def test_send_email_dryrun_doesnt_send(mock_boto_client):
    expiring_probes = {
        "beta": {
            "p1": ["email"]
        }
    }
    probe_expiry_alert.send_emails_for_expiring_probes(
        datetime.date(2020, 1, 1),
        expiring_probes,
        dryrun=False,
    )
    # make sure send_raw_email is the right method
    mock_boto_client().send_raw_email.assert_called_once()

    probe_expiry_alert.send_emails_for_expiring_probes(
        datetime.date(2020, 1, 1),
        expiring_probes,
        dryrun=True,
    )
    mock_boto_client().send_raw_email.assert_called_once()


@mock.patch("probe_scraper.emailer.send_ses")
def test_send_email(mock_send_email):
    expiring_probes = {
        "beta": {
            "expiring_probe_1": ["email1"],
            "expiring_probe_2": ["email1"]
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
        datetime.date(2020, 1, 1),
        expiring_probes,
        dryrun=True,
    )

    assert mock_send_email.call_count == 2

    assert "email1" in send_email_args.keys()
    assert "email2" in send_email_args.keys()
    assert len(send_email_args["email1"]) == 1
    assert len(send_email_args["email2"]) == 1

    email_body = send_email_args["email2"][0]
    assert email_body.count("beta") == 0
    assert email_body.count("release") == 1
    assert email_body.count("expiring_probe_2") == 1

    email_body = send_email_args["email1"][0]
    assert email_body.count("beta") == 1
    assert email_body.count("release") == 1
    assert email_body.count("expiring_probe_1") == 1
    assert email_body.count("expiring_probe_2") == 2
