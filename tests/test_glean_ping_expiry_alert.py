import datetime
import re
from typing import Optional
from unittest.mock import MagicMock, patch

from probe_scraper import glean_ping_expiry_alert


def test_is_reaching_retention_limit_expired():
    assert not glean_ping_expiry_alert.is_reaching_retention_limit(
        run_date=datetime.date(year=2024, month=1, day=21),
        retention_days=20,
        oldest_partition_date=datetime.date(year=2024, month=1, day=1),
    )


def test_is_reaching_retention_limit_not_expired():
    assert not glean_ping_expiry_alert.is_reaching_retention_limit(
        run_date=datetime.date(year=2024, month=1, day=2),
        retention_days=30,
        oldest_partition_date=datetime.date(year=2024, month=1, day=1),
    )


def test_is_reaching_retention_limit_expiring():
    assert glean_ping_expiry_alert.is_reaching_retention_limit(
        run_date=datetime.date(year=2024, month=1, day=20),
        retention_days=30,
        oldest_partition_date=datetime.date(year=2024, month=1, day=1),
    )


def test_is_reaching_collect_through_date_expired():
    assert not glean_ping_expiry_alert.is_reaching_collect_through_date(
        run_date=datetime.date(year=2024, month=1, day=20),
        collect_through_date="2024-01-10",
    )


def test_is_reaching_collect_through_date_not_expired():
    assert not glean_ping_expiry_alert.is_reaching_collect_through_date(
        run_date=datetime.date(year=2024, month=1, day=1),
        collect_through_date="2024-01-30",
    )


def test_is_reaching_collect_through_date_expiring():
    assert glean_ping_expiry_alert.is_reaching_collect_through_date(
        run_date=datetime.date(year=2024, month=1, day=20),
        collect_through_date="2024-01-30",
    )


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
    },
    {
        "bq_dataset_family": "firefox_desktop_background_update",
        "notification_emails": ["desktop-update@mozilla.com"],
        "moz_pipeline_metadata": {
            "ping-1": {"expiration_policy": {"delete_after_days": 25}},
        },
        "v1_name": "firefox-desktop-background-update",
    },
]

PINGS_BY_APP = {
    "firefox-desktop": {
        "ping-1": {
            "moz_pipeline_metadata": {"expiration_policy": {"delete_after_days": 30}},
            "history": [{}],
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


class MockTable(MagicMock):
    def __init__(
        self, name: str, dataset: str, retention_days: Optional[int], *args, **kw
    ):
        super().__init__(*args, **kw)
        self.table_id = name
        self.reference = f"proj.{dataset}.{name}"
        self.time_partitioning = MagicMock()
        self.time_partitioning.expiration_ms = (
            retention_days * 24 * 60 * 60 * 1000 if retention_days is not None else None
        )

    def __repr__(self):
        return self.reference


@patch("probe_scraper.glean_ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_already_expired(mock_client_class):
    """Alerts should not be sent for pings that have already expired."""

    def mock_tables(dataset: str):
        return {
            "firefox_desktop_stable": [
                MockTable("ping_1_v1", dataset, 30),
                MockTable("ping_2_v1", dataset, 40),
            ],
            "firefox_desktop_background_update_stable": [],
        }[dataset]

    def mock_partitions(table_id: str):
        return {
            "proj.firefox_desktop_stable.ping_1_v1": [
                "20240101",
                "20240402",
                "20240419",
                "__NULL__",
            ],
            "proj.firefox_desktop_stable.ping_2_v1": [
                "20231221",
                "20240101",
                "20240419",
            ],
        }[table_id]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.list_tables = mock_tables
    mock_client.list_partitions = mock_partitions

    expiring, errors = glean_ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2024-01-31"),
        project_id="proj",
        partition_fallback=False,
    )

    assert len(expiring) == 0
    assert len(errors) == 0


@patch("probe_scraper.glean_ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_not_expired(mock_client_class):
    """Alerts should not be sent if future expiry date is out of range."""

    def mock_tables(dataset: str):
        return {
            "firefox_desktop_stable": [
                MockTable("ping_1_v1", dataset, 30),
                MockTable("ping_2_v1", dataset, 40),
            ],
            "firefox_desktop_background_update_stable": [],
        }[dataset]

    def mock_partitions(table_id: str):
        return {
            "proj.firefox_desktop_stable.ping_1_v1": ["20240101"],
            "proj.firefox_desktop_stable.ping_2_v1": ["20240101"],
        }[table_id]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.list_tables = mock_tables
    mock_client.list_partitions = mock_partitions

    expiring, errors = glean_ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2024-01-02"),
        project_id="proj",
        partition_fallback=False,
    )

    assert len(expiring) == 0
    assert len(errors) == 0


@patch("probe_scraper.glean_ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_expired(mock_client_class):
    """Alerts should be sent if the data will start being dropped soon."""

    def mock_tables(dataset: str):
        return {
            "firefox_desktop_stable": [
                MockTable("ping_1_v1", dataset, 30),
                MockTable("ping_2_v1", dataset, 40),
            ],
            "firefox_desktop_background_update_stable": [
                MockTable("ping_1_v1", dataset, 25),
            ],
        }[dataset]

    def mock_partitions(table_id: str):
        return {
            "proj.firefox_desktop_stable.ping_1_v1": [
                "20231205",
            ],
            "proj.firefox_desktop_stable.ping_2_v1": [
                "20231220",
            ],
            "proj.firefox_desktop_background_update_stable.ping_1_v1": [
                "20231210",
            ],
        }[table_id]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.list_tables = mock_tables
    mock_client.list_partitions = mock_partitions

    expiring, errors = glean_ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2023-12-21"),
        project_id="proj",
        partition_fallback=False,
    )

    assert len(expiring) == 4

    # firefox-desktop ping-1 expiry
    assert set(expiring["desktop@mozilla.com"].keys()) == {"firefox-desktop"}
    assert len(expiring["desktop@mozilla.com"]["firefox-desktop"]) == 1
    assert "ping-1" in expiring["desktop@mozilla.com"]["firefox-desktop"][0]

    # firefox-desktop ping-2 collect through date
    assert set(expiring["desktop-2@mozilla.com"].keys()) == {"firefox-desktop"}
    assert len(expiring["desktop-2@mozilla.com"]["firefox-desktop"]) == 1
    assert "ping-2" in expiring["desktop-2@mozilla.com"]["firefox-desktop"][0]
    assert "2024-01-01" in expiring["desktop-2@mozilla.com"]["firefox-desktop"][0]

    # firefox-desktop-background-update ping-1 expiry
    assert set(expiring["desktop-update@mozilla.com"].keys()) == {
        "firefox-desktop-background-update"
    }
    assert (
        len(expiring["desktop-update@mozilla.com"]["firefox-desktop-background-update"])
        == 1
    )
    assert (
        "ping-1"
        in expiring["desktop-update@mozilla.com"]["firefox-desktop-background-update"][
            0
        ]
    )

    # default email should receive everything
    assert set(expiring[glean_ping_expiry_alert.DEFAULT_EMAILS[0]].keys()) == {
        "firefox-desktop",
        "firefox-desktop-background-update",
    }
    assert (
        len(expiring[glean_ping_expiry_alert.DEFAULT_EMAILS[0]]["firefox-desktop"]) == 2
    )
    assert (
        len(
            expiring[glean_ping_expiry_alert.DEFAULT_EMAILS[0]][
                "firefox-desktop-background-update"
            ]
        )
        == 1
    )

    assert len(errors) == 0


@patch("probe_scraper.glean_ping_expiry_alert.request_get", mock_request)
@patch("google.cloud.bigquery.Client")
def test_retention_days_not_matching(mock_client_class):
    """Errors should be returned if the retention of the table does not match the metadata."""

    def mock_tables(dataset: str):
        return {
            "firefox_desktop_stable": [
                MockTable("ping_1_v1", dataset, 50),
                MockTable("ping_2_v1", dataset, None),
            ],
            "firefox_desktop_background_update_stable": [],
        }[dataset]

    def mock_partitions(table_id: str):
        return {
            "proj.firefox_desktop_stable.ping_1_v1": ["20240101"],
            "proj.firefox_desktop_stable.ping_2_v1": ["20240101"],
        }[table_id]

    mock_client = MagicMock()
    mock_client_class.return_value = mock_client
    mock_client.list_tables = mock_tables
    mock_client.list_partitions = mock_partitions

    expiring, errors = glean_ping_expiry_alert.get_expiring_pings(
        run_date=datetime.date.fromisoformat("2024-01-02"),
        project_id="proj",
        partition_fallback=False,
    )

    assert len(expiring) == 0

    assert len(errors) == 2
    assert "proj.firefox_desktop_stable.ping_1_v1" in errors
    assert "proj.firefox_desktop_stable.ping_2_v1" in errors
