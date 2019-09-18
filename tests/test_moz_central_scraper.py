from probe_scraper.scrapers import moz_central_scraper
from datetime import datetime
import pytest
import os


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

    res = moz_central_scraper.scrape_channel_revisions(tmp_dir, min_fx_version,
                                                       max_fx_version=max_fx_version,
                                                       channels=[channel])

    registries = {
        probe_type: [
            os.path.join(tmp_dir, "hg", revision, path)
            for path in paths if path_is_in_version(path, 62)
        ] for probe_type, paths in moz_central_scraper.REGISTRY_FILES.items()
    }

    record = {
        "date": datetime(2018, 10, 1, 18, 40, 35),
        "version": 62,
        "registries": registries
    }

    assert res[channel][revision] == record


@pytest.mark.web_dependency
def test_scrape():
    tmp_dir = "./.test-files"
    min_fx_version = 62
    max_fx_version = 62

    res = moz_central_scraper.scrape(tmp_dir, min_fx_version, max_fx_version=max_fx_version)

    channel = "release"
    revision = "84219fbf133cacfc6e31c9471ad20ee7162a02af"

    registries = {
        probe_type: [
            os.path.join(tmp_dir, "hg", revision, path)
            for path in paths if path_is_in_version(path, 62)
        ] for probe_type, paths in moz_central_scraper.REGISTRY_FILES.items()
    }

    record = {
        "channel": channel,
        "version": 62,
        "registries": registries
    }

    assert res[channel][revision] == record


@pytest.mark.web_dependency
def test_artificial_tag():
    tmp_dir = "./.test-files"
    min_fx_version = 71
    max_fx_version = 71

    channel = "nightly"

    res = moz_central_scraper.scrape(tmp_dir, min_fx_version,
                                     max_fx_version=max_fx_version,
                                     channels=[channel])

    revision = "fd2934cca1ae7b492f29a4d240915aa9ec5b4977"

    registries = {
        probe_type: [
            os.path.join(tmp_dir, "hg", revision, path)
            for path in paths if path_is_in_version(path, 71)
        ] for probe_type, paths in moz_central_scraper.REGISTRY_FILES.items()
    }

    record = {
        "channel": channel,
        "version": 71,
        "registries": registries
    }

    assert res[channel][revision] == record
