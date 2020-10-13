from datetime import datetime

import pytest

from probe_scraper.scrapers.buildhub import Buildhub, NoDataFoundException

FX_RELEASE_62_0_3 = {
    "revision": "c9ed11ae5c79df3dcb69075e1c9da0317d1ecb1b",
    "date": datetime(2018, 10, 1, 18, 40, 35),
    "version": "62.0.3rc1",
    "tree": "releases/mozilla-release",
}

VERBOSE = True


@pytest.fixture
def records():
    return [
        {
            "_source": {
                "download": {"date": "2019-01-28T23:49:22.717388+00:00"},
                "source": {"revision": "abc", "tree": "releases/mozilla-release"},
                "target": {"version": "1"},
            }
        },
        {
            "_source": {
                "download": {"date": "2019-01-29T23:49:22Z"},
                "source": {"revision": "def", "tree": "releases/mozilla-release"},
                "target": {"version": "2"},
            }
        },
    ]


@pytest.mark.web_dependency
def test_nightly_count():
    channel, min_version, max_version = "nightly", 62, 62

    bh = Buildhub()
    releases = bh.get_revision_dates(
        channel, min_version, max_version=max_version, verbose=VERBOSE
    )
    assert len(releases) == 97


@pytest.mark.web_dependency
def test_pagination():
    channel, min_version, max_version = "nightly", 62, 62

    bh = Buildhub()
    releases = bh.get_revision_dates(
        channel, min_version, max_version=max_version, verbose=VERBOSE, window=10
    )
    assert len(releases) == 97


@pytest.mark.web_dependency
def test_duplicate_revisions():
    channel, min_version, max_version = "nightly", 67, 67

    bh = Buildhub()
    releases = bh.get_revision_dates(
        channel, min_version, max_version=max_version, verbose=VERBOSE
    )
    assert len({r["revision"] for r in releases}) == len(releases)


@pytest.mark.web_dependency
def test_release():
    channel, min_version, max_version = "release", 62, 62

    bh = Buildhub()
    releases = bh.get_revision_dates(
        channel, min_version, max_version=max_version, verbose=VERBOSE
    )

    assert FX_RELEASE_62_0_3 in releases


@pytest.mark.web_dependency
def test_min_release():
    channel, min_version, max_version = "release", 63, 63

    bh = Buildhub()
    releases = bh.get_revision_dates(
        channel, min_version, max_version=max_version, verbose=VERBOSE
    )

    assert FX_RELEASE_62_0_3 not in releases


@pytest.mark.web_dependency
def test_no_min_max_version_overlap():
    channel, min_version, max_version = "release", 63, 62
    bh = Buildhub()

    with pytest.raises(NoDataFoundException):
        bh.get_revision_dates(
            channel, min_version, max_version=max_version, verbose=VERBOSE
        )


@pytest.mark.web_dependency
def test_no_released_version():
    channel, min_version = "release", 99
    bh = Buildhub()

    with pytest.raises(NoDataFoundException):
        bh.get_revision_dates(channel, min_version, verbose=VERBOSE)


def test_version_100():
    channel, min_version = "release", 100
    bh = Buildhub()

    with pytest.raises(AssertionError):
        bh.get_revision_dates(channel, min_version, verbose=VERBOSE)


def test_cleaned_dates(records):
    bh = Buildhub()

    expected = [
        {
            "revision": "abc",
            "date": datetime(2019, 1, 28, 23, 49, 22, 717388),
            "version": "1",
            "tree": "releases/mozilla-release",
        },
        {
            "revision": "def",
            "date": datetime(2019, 1, 29, 23, 49, 22),
            "version": "2",
            "tree": "releases/mozilla-release",
        },
    ]

    assert bh._distinct_and_clean(records) == expected


# Test unique and sorted values
def test_unique_sorted(records):
    bh = Buildhub()

    records[1]["_source"]["source"]["revision"] = "abc"
    records[1]["_source"]["download"]["date"] = "2019-01-22T23:49:22Z"

    expected = [
        {
            "revision": "abc",
            "date": datetime(2019, 1, 22, 23, 49, 22),
            "version": "2",
            "tree": "releases/mozilla-release",
        },
    ]

    assert bh._distinct_and_clean(records) == expected
