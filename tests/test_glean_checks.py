import pytest

from probe_scraper.glean_checks import (
    check_for_duplicate_metrics,
    check_for_expired_metrics,
)
from probe_scraper.parsers.repositories import Repository

OLD_DATE = "2019-10-04 17:30:42"
NEWER_DATE = "2019-12-04 17:30:42"
NEWEST_DATE = "2020-12-04 17:30:42"

BASE_METADATA = {"notification_emails": ["foo@bar.com"]}


@pytest.fixture
def fake_repositories():
    return [
        Repository("glean-core", dict(BASE_METADATA, library_names=["glean-core"])),
        Repository(
            "glean-android", dict(BASE_METADATA, library_names=["glean-android"])
        ),
        Repository(
            "fake-app",
            dict(BASE_METADATA, dependencies=["glean-core", "glean-android"]),
        ),
        Repository("firefox-desktop", dict(BASE_METADATA, dependencies=["glean-core"])),
    ]


GLEAN_EPOCH = 1  # Not really an epoch


@pytest.fixture
def fake_commit_timestamps():
    return {
        "glean-core": {
            "facade": (GLEAN_EPOCH, 0),
        },
        "glean-android": {
            "decafc0ffee": (GLEAN_EPOCH, 0),
        },
        "fake-app": {
            "coffeecafe": (GLEAN_EPOCH, 0),
        },
        "firefox-desktop": {
            "31337feed": (GLEAN_EPOCH, 0),
        },
    }


@pytest.fixture
def fake_repos_metrics():
    return {
        "glean-core": {
            "facade": {
                "glean.core.metric_name": {
                    "expires": "never",
                    "notification_emails": ["glean-core-team@allizom.com"],
                }
            },
        },
        "glean-android": {
            "decafc0ffee": {
                "glean.android.metric_name": {
                    "expires": "expired",
                    "notification_emails": ["glean-android-team@allizom.com"],
                }
            },
        },
        "fake-app": {
            "coffeecafe": {
                "fake.app.metric_name": {
                    "expires": "never",
                    "notification_emails": ["fake-app-team@allizom.com"],
                }
            },
        },
        "firefox-desktop": {
            "31337feed": {
                "firefox.desktop.metric_name": {
                    "expires": "102",
                    "notification_emails": ["firefox-desktop-team@allizom.com"],
                }
            },
        },
    }


def test_check_duplicate_metrics_no_duplicates(fake_repositories):
    # no overlap between metrics defined by glean-core and glean-android (both used by burnham)
    # check_for_duplicate_metrics should return False
    assert not check_for_duplicate_metrics(
        fake_repositories,
        {
            "glean-core": {
                "app_display_version": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        )
                    ]
                }
            },
            "glean-android": {
                "app_display_version_android": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        )
                    ]
                }
            },
            "fake-app": {},
            "firefox-desktop": {},
        },
        {},
    )


def test_check_duplicate_metrics_duplicates(fake_repositories):
    # glean-core and glean-android define the same metric in the current date
    # check_for_duplicate_metrics should return True
    assert check_for_duplicate_metrics(
        fake_repositories,
        {
            "glean-core": {
                "app_display_version": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        )
                    ]
                }
            },
            "glean-android": {
                "app_display_version": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        )
                    ]
                },
            },
            "fake-app": {},
            "firefox-desktop": {},
        },
        {},
    )


def test_check_duplicate_metrics_duplicates_in_the_past(fake_repositories):
    # glean-core and glean-android define the same metric at one point in the
    # past, but not presently
    # check_for_duplicate_metrics should return False
    assert not check_for_duplicate_metrics(
        fake_repositories,
        {
            "glean-core": {
                "app_display_version": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        )
                    ]
                }
            },
            "glean-android": {
                "app_display_version": {
                    "history": [
                        dict(
                            BASE_METADATA, dates={"first": OLD_DATE, "last": NEWER_DATE}
                        ),
                    ]
                },
                "new_metric": {
                    "history": [
                        # the newer date here implies that app_display_version above was removed
                        dict(
                            BASE_METADATA,
                            dates={"first": OLD_DATE, "last": NEWEST_DATE},
                        ),
                    ]
                },
            },
            "fake-app": {},
            "firefox-desktop": {},
        },
        {},
    )


def test_check_for_expired_metrics(
    fake_repositories, fake_commit_timestamps, fake_repos_metrics
):
    emails = {}

    check_for_expired_metrics(
        fake_repositories, {}, fake_commit_timestamps, emails, 57, 14
    )
    assert len(emails) == 0

    check_for_expired_metrics(
        fake_repositories, fake_repos_metrics, fake_commit_timestamps, emails, 57, 14
    )
    assert len(emails) == 1  # One manually-expired metric

    emails = {}
    check_for_expired_metrics(
        fake_repositories, fake_repos_metrics, fake_commit_timestamps, emails, 101, 14
    )
    assert len(emails) == 2  # One manually-expired metric, and one that expires in 102
