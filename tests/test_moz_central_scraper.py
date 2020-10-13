import os
from datetime import datetime

import pytest

from probe_scraper.scrapers import moz_central_scraper


def test_extract_major_version():
    assert moz_central_scraper.extract_major_version("62.0a1") == 62
    assert moz_central_scraper.extract_major_version("63.0.2") == 63
    with pytest.raises(Exception):
        moz_central_scraper.extract_major_version("helloworld")


def path_is_in_version(path, version):
    return moz_central_scraper.relative_path_is_in_version(path, version)


@pytest.mark.web_dependency
def test_channel_revisions():
    tmp_dir = "./.test-files"
    min_fx_version = 62
    max_fx_version = 62
    channel = "release"
    revision = "c9ed11ae5c79df3dcb69075e1c9da0317d1ecb1b"

    res = moz_central_scraper.scrape_channel_revisions(
        tmp_dir, min_fx_version, max_fx_version=max_fx_version, channels=[channel]
    )

    registries = {
        probe_type: [
            os.path.join(tmp_dir, "hg", revision, path)
            for path in paths
            if path_is_in_version(path, 62)
        ]
        for probe_type, paths in moz_central_scraper.REGISTRY_FILES.items()
    }

    record = {
        "date": datetime(2018, 10, 1, 18, 40, 35),
        "version": 62,
        "registries": registries,
    }

    assert res[channel][revision] == record
