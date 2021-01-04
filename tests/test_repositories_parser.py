import os
import tempfile

import jsonschema
import pytest
import yaml

from probe_scraper.parsers.repositories import RepositoriesParser


def write_to_temp_file(data):
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, "w") as tmp:
        tmp.write(yaml.dump(data))
    return path


@pytest.fixture
def parser():
    return RepositoriesParser()


@pytest.fixture
def correct_repos_file():
    data = {
        "libraries": [],
        "application_families": [
            {
                "app_name": "mobile_metrics_example",
                "description": "foo",
                "url": "www.github.com/fbertsch/mobile-metrics-example",
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "apps": [
                    {
                        "v1_name": "test-repo",
                        "app_id": "mobile_metrics_example",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    return write_to_temp_file(data)


@pytest.fixture
def incorrect_repos_file():
    data = {
        "libraries": [],
        "application_families": [
            {
                "app_name": "mobile-metrics-example",
                "description": "foo",
                "url": "www.github.com/fbertsch/mobile-metrics-example",
                # "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "apps": [
                    {
                        "v1_name": "test-repo",
                        "app_id": "mobile_metrics_example",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    return write_to_temp_file(data)


@pytest.fixture
def not_kebab_case_repos_file():
    data = {
        "libraries": [],
        "application_families": [
            {
                "app_name": "mobile_metrics_example",
                "description": "foo",
                "url": "www.github.com/fbertsch/mobile-metrics-example",
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "apps": [
                    {
                        "v1_name": "test_repo",
                        "app_id": "mobile_metrics_example",
                        "app_channel": "release",
                    }
                ],
            }
        ],
    }

    return write_to_temp_file(data)


@pytest.fixture
def invalid_release_channel_file():
    data = {
        "libraries": [],
        "application_families": [
            {
                "app_name": "mobile_metrics_example",
                "description": "foo",
                "url": "www.github.com/fbertsch/mobile-metrics-example",
                "notification_emails": ["frank@mozilla.com"],
                "metrics_files": ["metrics.yaml"],
                "apps": [
                    {
                        "v1_name": "test-repo",
                        "app_id": "mobile_metrics_example",
                        "app_channel": "semiquarterly",
                    }
                ],
            }
        ],
    }

    return write_to_temp_file(data)


def test_repositories(parser):
    parser.validate()


def test_repositories_parser_incorrect(parser, incorrect_repos_file):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        parser.validate(incorrect_repos_file)


def test_repositories_parser_invalid_channel(parser, invalid_release_channel_file):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        parser.validate(invalid_release_channel_file)


def test_repositories_parser_not_kebab_case(parser, not_kebab_case_repos_file):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        parser.validate(not_kebab_case_repos_file)


def test_repositories_class(parser, correct_repos_file):
    repos = parser.parse(correct_repos_file)

    assert len(repos) == 1
    assert set(repos[0].get_metrics_file_paths()) == {"metrics.yaml"}
    assert repos[0].to_dict() == {
        "app_id": "mobile-metrics-example",
        "branch": "master",
        "channel": "release",
        "dependencies": [],
        "deprecated": False,
        "description": "foo",
        "metrics_file_paths": ["metrics.yaml"],
        "name": "test-repo",
        "notification_emails": ["frank@mozilla.com"],
        "ping_file_paths": [],
        "prototype": False,
        "url": "www.github.com/fbertsch/mobile-metrics-example",
    }
