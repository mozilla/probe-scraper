from probe_scraper.parsers.repositories import RepositoriesParser
import pytest
import jsonschema
import yaml
import tempfile
import os


def write_to_temp_file(data):
    fd, path = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as tmp:
        tmp.write(yaml.dump(data))
    return path


@pytest.fixture
def parser():
    return RepositoriesParser()


@pytest.fixture
def incorrect_repos_file():
    data = {
        "some_repo": {
            # missing `notification_emails`
            "url": "www.github.com/fbertsch/mobile-metrics-example"
        }
    }

    return write_to_temp_file(data)


@pytest.fixture
def correct_repos_file():
    data = {
        "test-repo": {
            "url": "www.github.com/fbertsch/mobile-metrics-example",
            "notification_emails": ["frank@mozilla.com"],
            "histogram_file_paths": ["Histograms.json", "other/Histograms.json"]
        }
    }

    return write_to_temp_file(data)


def test_repositories(parser):
    parser.validate()


def test_repositories_parser_correct(parser, incorrect_repos_file):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        parser.validate(incorrect_repos_file)


def test_repositories_class(parser, correct_repos_file):
    repos = parser.parse(correct_repos_file)

    assert len(repos) == 1
    assert set(repos[0].get_probe_paths()) == {
        ("histogram", "Histograms.json"),
        ("histogram", "other/Histograms.json")
    }
