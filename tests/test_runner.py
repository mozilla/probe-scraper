from copy import deepcopy
from datetime import datetime

import pytest

from probe_scraper import runner
from probe_scraper.parsers import repositories


def test_add_first_appeared_dates():
    probes_by_channel = {
        "all": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {"revisions": {"first": "rev-a", "last": "rev-b"}},
                    "release": {"revisions": {"first": "rev-c", "last": "rev-d"}},
                }
            }
        },
        "nightly": {
            "histogram/test_int_histogram": {
                "history": {
                    "nightly": {"revisions": {"first": "rev-a", "last": "rev-b"}}
                }
            }
        },
        "release": {
            "histogram/test_int_histogram": {
                "history": {
                    "release": {"revisions": {"first": "rev-c", "last": "rev-d"}}
                }
            }
        },
    }

    first_appeared_dates = {
        "histogram/test_int_histogram": {
            "release": datetime(2019, 1, 1, 0, 0, 0),
            "nightly": datetime(2018, 12, 1, 0, 0, 0),
        }
    }

    expected = deepcopy(probes_by_channel)
    expected["all"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
        "nightly": "2018-12-01 00:00:00",
    }
    expected["release"]["histogram/test_int_histogram"]["first_added"] = {
        "release": "2019-01-01 00:00:00",
    }
    expected["nightly"]["histogram/test_int_histogram"]["first_added"] = {
        "nightly": "2018-12-01 00:00:00"
    }

    assert (
        runner.add_first_appeared_dates(probes_by_channel, first_appeared_dates)
        == expected
    )


def test_trailing_space(tmp_path):
    """Test cases to check the output of json.dumps has no trailing spaces"""
    test_cases = [
        {
            "test1": 12,
            "test2": 31,
        }
    ]

    DIR_NAME = tmp_path / "output_dir"
    FILE_NAME = "file_name.txt"

    for test_case in test_cases:
        trailing_spaces = 0  # Counts no of trailing spaces
        runner.dump_json(test_case, DIR_NAME, FILE_NAME)
        path = DIR_NAME / FILE_NAME
        with open(path, "r") as file:
            for line in file.readline():
                if line[-1] == " ":
                    trailing_spaces += 1

        assert not trailing_spaces


@pytest.fixture
def repo_with_one_ping():
    return {"repo1": {"ping1": {"name": "ping1", "in-source": True}}}


@pytest.fixture
def repo_with_two_pings():
    return {
        "repo1": {
            "ping1": {"name": "ping1", "in-source": True},
            "ping-2": {"name": "ping-2", "in-source": False},
        }
    }


def test_add_pipeline_metadata_with_default(repo_with_one_ping, repo_with_two_pings):
    repo_config = {
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {
                "delete_after_days": 180,
                "collect_through_date": "2025-12-31",
            },
            "submission_timestamp_granularity": "seconds",
        },
        "app_id": "repo-1",
    }
    repository_list = [repositories.Repository(name="repo1", definition=repo_config)]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_one_ping, repositories=repository_list
    )

    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            }
        }
    }
    assert repo_with_one_ping == result

    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_two_pings, repositories=repository_list
    )
    # Notice that the metadata defaults are present in both pings
    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
            "ping-2": {
                "name": "ping-2",
                "in-source": False,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping_2_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
        }
    }
    assert repo_with_two_pings == result


def test_add_pipeline_metadata_no_default_ping_specific(
    repo_with_one_ping, repo_with_two_pings
):
    repo_config = {
        "moz_pipeline_metadata": {
            "ping1": {
                "jwe-mappings": {
                    "decrypted_field_path": "",
                    "source_field_path": "/payload",
                },
                "override_attributes": [
                    {"name": "geo_city", "value": "a_city"},
                    {"name": "geo_subdivision1", "value": "sub"},
                ],
            }
        },
        "app_id": "repo-1",
    }

    repository_list = [repositories.Repository(name="repo1", definition=repo_config)]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_one_ping, repositories=repository_list
    )

    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "jwe-mappings": {
                        "decrypted_field_path": "",
                        "source_field_path": "/payload",
                    },
                    "override_attributes": [
                        {"name": "geo_city", "value": "a_city"},
                        {"name": "geo_subdivision1", "value": "sub"},
                    ],
                },
            }
        }
    }
    assert repo_with_one_ping == result
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_two_pings, repositories=repository_list
    )
    # Notice that this result only has metadata for ping1, not ping-2
    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "jwe-mappings": {
                        "decrypted_field_path": "",
                        "source_field_path": "/payload",
                    },
                    "override_attributes": [
                        {"name": "geo_city", "value": "a_city"},
                        {"name": "geo_subdivision1", "value": "sub"},
                    ],
                },
            },
            "ping-2": {
                "name": "ping-2",
                "in-source": False,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping_2_v1",
                    "bq_metadata_format": "structured",
                },
            },
        }
    }
    assert repo_with_two_pings == result


def test_add_pipeline_metadata_with_default_with_ping_specfic_additions(
    repo_with_two_pings,
):
    repo_config = {
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {
                "delete_after_days": 180,
                "collect_through_date": "2025-12-31",
            },
            "submission_timestamp_granularity": "seconds",
        },
        "moz_pipeline_metadata": {
            "ping1": {
                "jwe-mappings": {
                    "decrypted_field_path": "",
                    "source_field_path": "/payload",
                },
                "override_attributes": [
                    {"name": "geo_city", "value": "a_city"},
                    {"name": "geo_subdivision1", "value": "sub"},
                ],
            }
        },
        "app_id": "repo-1",
    }
    repository_list = [repositories.Repository(name="repo1", definition=repo_config)]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_two_pings, repositories=repository_list
    )
    # Notice that the ping1 specific metadata is in addition to the default_metadata
    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                    "jwe-mappings": {
                        "decrypted_field_path": "",
                        "source_field_path": "/payload",
                    },
                    "override_attributes": [
                        {"name": "geo_city", "value": "a_city"},
                        {"name": "geo_subdivision1", "value": "sub"},
                    ],
                },
            },
            "ping-2": {
                "name": "ping-2",
                "in-source": False,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo_1",
                    "bq_table": "ping_2_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
        }
    }
    assert repo_with_two_pings == result


def test_add_pipeline_metadata_with_default_with_ping_specific_override(
    repo_with_two_pings,
):
    repo_config = {
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {
                "delete_after_days": 180,
                "collect_through_date": "2025-12-31",
            },
            "submission_timestamp_granularity": "seconds",
        },
        "moz_pipeline_metadata": {
            "ping1": {
                "expiration_policy": {
                    "delete_after_days": 90,
                },
            }
        },
        "app_id": "repo.1",
    }
    repository_list = [repositories.Repository(name="repo1", definition=repo_config)]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_two_pings, repositories=repository_list
    )
    # Notice that the default metadata for collect_through_date is applied to both pings, wth ping1
    # having the ping specific value for delete_after_days
    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo.1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 90,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
            "ping-2": {
                "name": "ping-2",
                "in-source": False,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo.1",
                    "bq_table": "ping_2_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
        }
    }
    assert repo_with_two_pings == result


def test_add_pipeline_metadata_with_default_with_pings_override(repo_with_two_pings):
    repo_config = {
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {
                "delete_after_days": 180,
                "collect_through_date": "2025-12-31",
            },
            "submission_timestamp_granularity": "seconds",
        },
        "moz_pipeline_metadata": {
            "ping1": {
                "expiration_policy": {
                    "delete_after_days": 90,
                },
            },
            "ping-2": {
                "expiration_policy": {"collect_through_date": "2022-12-31"},
                "jwe-mappings": {
                    "decrypted_field_path": "",
                    "source_field_path": "/payload",
                },
            },
        },
        "app_id": "repo.1",
    }
    repository_list = [repositories.Repository(name="repo1", definition=repo_config)]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)
    runner.add_pipeline_metadata(
        pings_by_repo=repo_with_two_pings, repositories=repository_list
    )
    # Notice that the default metadata for collect_through_date is applied to both pings, wth ping1
    # having the ping specific value for delete_after_days and ping 2 adding jwe-mappings and
    # changing collect_through_date
    result = {
        "repo1": {
            "ping1": {
                "name": "ping1",
                "in-source": True,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo.1",
                    "bq_table": "ping1_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 90,
                        "collect_through_date": "2025-12-31",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
            "ping-2": {
                "name": "ping-2",
                "in-source": False,
                "moz_pipeline_metadata": {
                    "bq_dataset_family": "repo.1",
                    "bq_table": "ping_2_v1",
                    "bq_metadata_format": "structured",
                    "expiration_policy": {
                        "delete_after_days": 180,
                        "collect_through_date": "2022-12-31",
                    },
                    "jwe-mappings": {
                        "decrypted_field_path": "",
                        "source_field_path": "/payload",
                    },
                    "submission_timestamp_granularity": "seconds",
                },
            },
        }
    }
    assert repo_with_two_pings == result


def test_create_pipeline_metadata_overrides(repo_with_one_ping):
    """
    create_pipeline_metadata_overrides should create a ping entry for pings with
    moz_pipeline_metadata but are not defined in the app
    """
    repo_config = {
        "moz_pipeline_metadata_defaults": {
            "expiration_policy": {
                "delete_after_days": 180,
            },
            "submission_timestamp_granularity": "seconds",
        },
        "moz_pipeline_metadata": {
            "ping1": {
                "expiration_policy": {
                    "delete_after_days": 10,
                },
            },
            "ping2": {
                "expiration_policy": {
                    "delete_after_days": 90,
                },
            },
            "ping3": {
                "expiration_policy": {
                    "delete_after_days": 120,
                },
            },
        },
        "app_id": "repo-1",
    }
    repository_list = [
        repositories.Repository(name="repo1", definition=repo_config),
        repositories.Repository(name="repo2", definition={"app_id": "repo-2"}),
    ]
    runner.add_pipeline_metadata_defaults(repositories=repository_list)

    second_repo = {"repo2": {}}
    actual = runner.create_pipeline_metadata_overrides(
        pings_by_repo=repo_with_one_ping | second_repo, repositories=repository_list
    )

    expected = {
        "repo1": {
            "moz_pipeline_metadata_overrides": {
                "ping2": {
                    "expiration_policy": {
                        "delete_after_days": 90,
                    },
                },
                "ping3": {
                    "expiration_policy": {
                        "delete_after_days": 120,
                    },
                },
            },
        },
        "repo2": {},
    }
    assert expected == actual
