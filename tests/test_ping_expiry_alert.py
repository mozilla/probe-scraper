import datetime
import re
from unittest.mock import MagicMock, patch

from probe_scraper import ping_expiry_alert

APP_LISTINGS = [
    {
        "bq_dataset_family": "firefox_desktop",
        "moz_pipeline_metadata": {
            "ping-1": {"expiration_policy": {"delete_after_days": 30}},
            "ping-2": {
                "expiration_policy": {
                    "delete_after_days": 40,
                    "collect_through_date": "2024-01-01",
                }
            },
        },
        "notification_emails": ["desktop@mozilla.com"],
        "v1_name": "firefox-desktop",
        "app_name": "firefox_desktop",
    },
    {
        "bq_dataset_family": "firefox_desktop_background_update",
        "notification_emails": ["desktop-update@mozilla.com"],
        "moz_pipeline_metadata": {
            "ping-1": {"expiration_policy": {"delete_after_days": 25}},
        },
        "v1_name": "firefox-desktop-background-update",
        "app_name": "firefox_desktop_background_update",
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {"delete_after_days": 60}
        },
    },
]

PINGS_BY_APP = {
    "firefox-desktop": {
        "ping-1": {
            "moz_pipeline_metadata": {"expiration_policy": {"delete_after_days": 30}},
            "history": [{"notification_emails": ["desktop-1@mozilla.com"]}],
        },
        "ping-2": {
            "moz_pipeline_metadata": {
                "expiration_policy": {
                    "delete_after_days": 40,
                    "collect_through_date": "2024-01-01",
                }
            },
            "history": [{"notification_emails": ["desktop-2@mozilla.com"]}],
        },
    },
    "firefox-desktop-background-update": {},  # pings defined in dependency
}


def mock_request(url: str):
    if url.endswith("/app-listings"):
        return APP_LISTINGS
    elif app_name_match := re.search(
        r"glean/([a-z0-9-]+)/pings$", url, flags=re.IGNORECASE
    ):
        app_name = app_name_match.group(1)
        return PINGS_BY_APP.get(app_name, {})
    else:
        raise ValueError(f"invalid url: {url}")


@patch("probe_scraper.ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_already_expired(mock_client_class):
    """Alerts should not be sent for pings that have already expired."""

    mock_retention = [
        {
            "project_id": "proj",
            "dataset_id": "firefox_desktop_stable",
            "tables": [
                {
                    "table_id": "ping_1_v1",
                    "partition_expiration_days": 30,
                    "actual_partition_expiration_days": 30,
                    "next_deletion_date": datetime.date.fromisoformat("2023-12-22"),
                },
                {
                    "table_id": "ping_2_v1",
                    "partition_expiration_days": 40,
                    "actual_partition_expiration_days": 40,
                    "next_deletion_date": datetime.date.fromisoformat("2023-12-23"),
                },
            ],
        },
    ]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.query_and_wait.return_value = mock_retention

    expiring, errors = ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2023-12-21"),
        project_id="proj",
    )

    assert len(expiring) == 0
    assert len(errors) == 0


@patch("probe_scraper.ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_not_expired(mock_client_class):
    """Alerts should not be sent if future expiry date is out of range."""

    mock_retention = [
        {
            "project_id": "proj",
            "dataset_id": "firefox_desktop_stable",
            "tables": [
                {
                    "table_id": "ping_1_v1",
                    "partition_expiration_days": 30,
                    "actual_partition_expiration_days": 30,
                    "next_deletion_date": datetime.date.fromisoformat("2024-12-22"),
                },
                {
                    "table_id": "ping_2_v1",
                    "partition_expiration_days": 40,
                    "actual_partition_expiration_days": 40,
                    "next_deletion_date": datetime.date.fromisoformat("2025-12-22"),
                },
            ],
        },
    ]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.query_and_wait.return_value = mock_retention

    expiring, errors = ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2023-12-21"),
        project_id="proj",
    )

    assert len(expiring) == 0
    assert len(errors) == 0


@patch("probe_scraper.ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_expired(mock_client_class):
    """Alerts should be sent if the data will start being dropped soon."""

    mock_retention = [
        {
            "project_id": "proj",
            "dataset_id": "firefox_desktop_stable",
            "tables": [
                {
                    "table_id": "ping_1_v1",
                    "partition_expiration_days": 30,
                    "actual_partition_expiration_days": 30,
                    # already expired
                    "next_deletion_date": datetime.date.fromisoformat("2023-12-22"),
                },
                {
                    "table_id": "ping_2_v1",
                    "partition_expiration_days": 40,
                    "actual_partition_expiration_days": 40,
                    # expiring soon
                    "next_deletion_date": datetime.date.fromisoformat("2024-01-01"),
                },
            ],
        },
        {
            "project_id": "proj",
            "dataset_id": "firefox_desktop_background_update_stable",
            "tables": [
                {
                    "table_id": "ping_3_v1",
                    "partition_expiration_days": 60,
                    "actual_partition_expiration_days": 60,
                    # expiring soon
                    "next_deletion_date": datetime.date.fromisoformat("2024-01-02"),
                },
            ],
        },
    ]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.query_and_wait.return_value = mock_retention

    expiring, errors = ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2023-12-21"),
        project_id="proj",
    )

    assert len(expiring) == len(ping_expiry_alert.DEFAULT_EMAILS) + 3

    # firefox-desktop ping-2 expiry
    assert set(expiring["desktop@mozilla.com"].keys()) == {"firefox_desktop"}
    assert len(expiring["desktop@mozilla.com"]["firefox_desktop"]) == 1
    assert (
        "firefox_desktop.ping_2"
        in expiring["desktop@mozilla.com"]["firefox_desktop"][0]
    )
    assert "2024-01-01" in expiring["desktop@mozilla.com"]["firefox_desktop"][0]

    # firefox-desktop-background-update ping-3 expiry
    assert set(expiring["desktop-update@mozilla.com"].keys()) == {
        "firefox_desktop_background_update"
    }
    assert (
        len(expiring["desktop-update@mozilla.com"]["firefox_desktop_background_update"])
        == 1
    )
    assert (
        "firefox_desktop_background_update.ping_3"
        in expiring["desktop-update@mozilla.com"]["firefox_desktop_background_update"][
            0
        ]
    )

    # default email should receive everything
    assert set(expiring[ping_expiry_alert.DEFAULT_EMAILS[0]].keys()) == {
        "firefox_desktop",
        "firefox_desktop_background_update",
    }
    assert len(expiring[ping_expiry_alert.DEFAULT_EMAILS[0]]["firefox_desktop"]) == 1
    assert (
        len(
            expiring[ping_expiry_alert.DEFAULT_EMAILS[0]][
                "firefox_desktop_background_update"
            ]
        )
        == 1
    )

    assert len(errors) == 0


@patch("probe_scraper.ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_retention_days_not_matching(mock_client_class):
    """Errors should be returned if the retention of the table does not match the metadata."""
    mock_retention = [
        {
            "project_id": "proj",
            "dataset_id": "firefox_desktop_stable",
            "tables": [
                {
                    "table_id": "ping_1_v1",
                    "partition_expiration_days": 50,
                    "actual_partition_expiration_days": 50,
                    "next_deletion_date": datetime.date.fromisoformat("2025-01-02"),
                },
                {
                    "table_id": "ping_2_v1",
                    "partition_expiration_days": None,
                    "actual_partition_expiration_days": None,
                    "next_deletion_date": datetime.date.fromisoformat("2025-01-02"),
                },
            ],
        }
    ]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.query_and_wait.return_value = mock_retention

    expiring, errors = ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2024-01-02"),
        project_id="proj",
    )

    assert len(expiring) == 0

    assert len(errors) == 2
    assert "proj.firefox_desktop_stable.ping_1_v1" in errors
    assert "proj.firefox_desktop_stable.ping_2_v1" in errors
