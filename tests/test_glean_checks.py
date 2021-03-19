import pytest

from probe_scraper.glean_checks import check_for_duplicate_metrics
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
    ]


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
        },
        {},
    )
