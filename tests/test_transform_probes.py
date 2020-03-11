import probe_scraper.transform_probes as transform
import pprint

from datetime import datetime

# incoming probe_data is of the form:
#   node_id -> {
#     histogram: {
#       name: ...,
#       ...
#     },
#     scalar: {
#       ...
#     },
#   }
#
# node_data is of the form:
#   node_id -> {
#     version: ...
#     channel: ...
#   }

CHANNELS = ["release", "beta"]

IN_NODE_DATA = {
    channel: {
        "node_id_1": {
            "version": "50",
        },
        "node_id_2": {
            "version": "51",
        },
        "node_id_4": {
            "version": "52",
        },
        "node_id_3": {
            "version": "52",
        },
    }
    for channel in CHANNELS
}

REVISION_DATES = {
    channel: {
        "node_id_1": {
            "version": "50",
            "date": datetime(2018, 1, 1, 10, 11, 12)
        },
        "node_id_2": {
            "version": "51",
            "date": datetime(2018, 2, 2, 10, 11, 12)
        },
        "node_id_3": {
            "version": "52",
            "date": datetime(2018, 3, 3, 10, 11, 12)
        },
        "node_id_4": {
            "version": "52",
            "date": datetime(2018, 1, 1, 1, 1, 1)
        },
    }
    for channel in CHANNELS
}

REPOS = {"test-repo-0", "test-repo-1"}

IN_METRICS_DATA = {
    repo: {
        str(i): {
            "example.duration": {
                "type": "timespan",
                "description": "  The duration of the last foreground session.",
                "time_unit": "second",
                "send_in_pings": ["baseline"],
                "bugs": [1497894, 1519120],
                "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                "notification_emails": ["telemetry-client-dev@mozilla.com"]
            }
        } for i in range(4)
    } for repo in REPOS
}

OUT_METRICS_DATA = {
    repo: {
        "example.duration": {
            "type": "timespan",
            "name": "example.duration",
            "history": [
                {
                    "type": "timespan",
                    "description": "  The duration of the last foreground session.",
                    "time_unit": "second",
                    "send_in_pings": ["baseline"],
                    "bugs": [1497894, 1519120],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"],
                    "git-commits": {
                        "first": "3",
                        "last": "0"
                    },
                    "dates": {
                        "first": "1969-12-31 23:59:57",
                        "last": "1970-01-01 00:00:00"
                    },
                    "reflog-index": {
                        "first": 3,
                        "last": 0
                    }
                }
            ]
        }
    } for repo in REPOS
}

IN_PING_DATA = {
    repo: {
        str(i): {
            "metrics": {
                "description": "Metrics ping",
                "bugs": ["https://bugzilla.mozilla.org/1512938"],
                "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                "notification_emails": ["telemetry-client-dev@mozilla.com"],
                "include_client_id": True,
                "send_if_empty": False,
            }
        } for i in range(4)
    } for repo in REPOS
}

OUT_PING_DATA = {
    repo: {
        "metrics": {
            "name": "metrics",
            "history": [
                {
                    "description": "Metrics ping",
                    "bugs": ["https://bugzilla.mozilla.org/1512938"],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"],
                    "include_client_id": True,
                    "send_if_empty": False,
                    "git-commits": {
                        "first": "3",
                        "last": "0"
                    },
                    "dates": {
                        "first": "1969-12-31 23:59:57",
                        "last": "1970-01-01 00:00:00"
                    },
                    "reflog-index": {
                        "first": 3,
                        "last": 0
                    }
                }
            ]
        }
    } for repo in REPOS
}


def in_probe_data():
    top_levels = CHANNELS
    secondary_level_prefix = "node_id_"

    secondary_level = {
        secondary_level_prefix + "1": {
            "histogram": {
                "TEST_HISTOGRAM_1": {
                    "cpp_guard": None,
                    "description": "A description.",
                    "expiry_version": "53.0",
                    "optout": False,
                    "details": {
                        "low": 1,
                        "high": 10,
                        "keyed": False,
                        "kind": "exponential",
                        "n_buckets": 5,
                        "record_in_processes": ["main", "content"],
                    },
                }
             }
        },
        secondary_level_prefix + "2": {
            "histogram": {
                "TEST_HISTOGRAM_1": {
                    "cpp_guard": None,
                    "description": "A description.",
                    "expiry_version": "53.0",
                    "optout": True,
                    "details": {
                        "low": 1,
                        "high": 10,
                        "keyed": False,
                        "kind": "exponential",
                        "n_buckets": 5,
                        "record_in_processes": ["main", "content"],
                    },
                }
             }
        },
        secondary_level_prefix + "3": {
            "histogram": {
                "TEST_HISTOGRAM_1": {
                    "cpp_guard": None,
                    "description": "A description.",
                    "expiry_version": "53.0",
                    "optout": True,
                    "details": {
                        "low": 1,
                        "high": 10,
                        "keyed": False,
                        "kind": "exponential",
                        "n_buckets": 5,
                        "record_in_processes": ["content"],
                    },
                }
            }
        },
        secondary_level_prefix + "4": {
            "histogram": {
                "TEST_HISTOGRAM_1": {
                    "cpp_guard": None,
                    "description": "A description.",
                    "expiry_version": "53.0",
                    "optout": True,
                    "details": {
                        "low": 1,
                        "high": 10,
                        "keyed": False,
                        "kind": "exponential",
                        "n_buckets": 5,
                        "record_in_processes": ["content"],
                    },
                }
            }
        }
    }

    return {top_level: secondary_level for top_level in top_levels}


def out_probe_data(by_channel=False):

    probes = [
        {
            'cpp_guard': None,
            'description': 'A description.',
            'details': {
                'high': 10,
                'keyed': False,
                'kind': 'exponential',
                'low': 1,
                'n_buckets': 5,
                'record_in_processes': ['content'],
            },
            'expiry_version': '53.0',
            'optout': True,
            'revisions': {
                'first': 'node_id_3',
                'last': 'node_id_4',
            },
            'versions': {
                'first': '52',
                'last': '52'
            }
        }, {
            'cpp_guard': None,
            'description': 'A description.',
            'details': {
                'high': 10,
                'keyed': False,
                'kind': 'exponential',
                'low': 1,
                'n_buckets': 5,
                'record_in_processes': ['main', 'content'],
            },
            'expiry_version': '53.0',
            'optout': True,
            'revisions': {
                'first': 'node_id_2',
                'last': 'node_id_2',
            },
            'versions': {
                'first': '51',
                'last': '51'
            }
        }, {
            'cpp_guard': None,
            'description': 'A description.',
            'details': {
                'high': 10,
                'keyed': False,
                'kind': 'exponential',
                'low': 1,
                'n_buckets': 5,
                'record_in_processes': ['main', 'content'],
            },
            'expiry_version': '53.0',
            'optout': False,
            'revisions': {
                'first': 'node_id_1',
                'last': 'node_id_1',
            },
            'versions': {
                'first': '50',
                'last': '50'
            }
        }
    ]

    allowed_channels = CHANNELS

    if by_channel:
        return {
            channel: {
                'histogram/TEST_HISTOGRAM_1': {
                    'history': {channel: probes},
                    'name': 'TEST_HISTOGRAM_1',
                    'type': 'histogram'
                }
            }
            for channel in allowed_channels
        }
    else:
        return {
            'histogram/TEST_HISTOGRAM_1': {
                'history': {channel: probes for channel in allowed_channels},
                'name': 'TEST_HISTOGRAM_1',
                'type': 'histogram'
            }
        }


def get_differences(a, b, path="", sep=" / "):
    res = []
    if a and not b:
        res.append(("A exists but not B", path))
    if b and not a:
        res.append(("B exists but not A", path))
    if not a and not b:
        return res

    a_dict, b_dict = isinstance(a, dict), isinstance(b, dict)
    a_list, b_list = isinstance(a, list), isinstance(b, list)
    if a_dict and not b_dict:
        res.append(("A dict but not B", path))
    elif b_dict and not a_dict:
        res.append(("B dict but not A", path))
    elif not a_dict and not b_dict:
        if a_list and b_list:
            for i, (ae, be) in enumerate(zip(a, b)):
                res = res + get_differences(ae, be, path + sep + str(i))
        elif a != b:
            res.append(("A={}, B={}".format(a, b), path))
    else:
        a_keys, b_keys = set(a.keys()), set(b.keys())
        a_not_b, b_not_a = a_keys - b_keys, b_keys - a_keys

        for k in a_not_b:
            res.append(("A not B", path + sep + k))
        for k in b_not_a:
            res.append(("B not A", path + sep + k))

        for k in (a_keys & b_keys):
            res = res + get_differences(a[k], b[k], path + sep + k)

    return res


def print_and_test(expected, result):
    pp = pprint.PrettyPrinter(indent=2)

    print("\nresult:")
    pp.pprint(result)

    print("\nExpected:")
    pp.pprint(expected)

    print("\nDifferences:")
    print('\n'.join([' - '.join(v) for v in get_differences(expected, result)]))

    assert(result == expected)


def test_probes_equal():
    DATA = in_probe_data()["release"]
    histogram_node1 = DATA["node_id_1"]["histogram"]["TEST_HISTOGRAM_1"]
    histogram_node2 = DATA["node_id_2"]["histogram"]["TEST_HISTOGRAM_1"]
    histogram_node3 = DATA["node_id_3"]["histogram"]["TEST_HISTOGRAM_1"]
    histogram_node4 = DATA["node_id_4"]["histogram"]["TEST_HISTOGRAM_1"]

    assert(not transform.probes_equal(histogram_node1, histogram_node2))
    assert(not transform.probes_equal(histogram_node2, histogram_node3))
    assert(transform.probes_equal(histogram_node3, histogram_node4))


def test_transform_monolithic():
    result = transform.transform(in_probe_data(), IN_NODE_DATA, False)
    expected = out_probe_data(by_channel=False)

    print_and_test(expected, result)


def test_transform_by_channel():
    result = transform.transform(in_probe_data(), IN_NODE_DATA, True)
    expected = out_probe_data(by_channel=True)

    print_and_test(expected, result)


def test_transform_metrics_by_hash():
    timestamps = {
        repo: {
            str(i): (-i, i) for i in range(4)
        } for repo in REPOS
    }

    result = transform.transform_metrics_by_hash(timestamps, IN_METRICS_DATA)
    expected = OUT_METRICS_DATA

    print_and_test(expected, result)


def test_transform_pings_by_hash():
    timestamps = {
        repo: {
            str(i): (-i, i) for i in range(4)
        } for repo in REPOS
    }

    result = transform.transform_pings_by_hash(timestamps, IN_PING_DATA)
    expected = OUT_PING_DATA

    print_and_test(expected, result)


def test_get_minimum_date():
    expected = {
        "histogram/TEST_HISTOGRAM_1": {
            "release": datetime(2018, 1, 1, 1, 1, 1),
            "beta": datetime(2018, 1, 1, 1, 1, 1)
        }
    }

    result = transform.get_minimum_date(in_probe_data(), IN_NODE_DATA, REVISION_DATES)

    print_and_test(expected, result)


def test_sort_ordering():
    # See https://github.com/mozilla/probe-scraper/issues/108
    probes = {
        "test-repo-0": {
            "0": {
                "example.duration": {
                    "type": "timespan",
                    "description": "  The duration of the last foreground session.",
                    "time_unit": "second",
                    "send_in_pings": ["baseline"],
                    "bugs": [1497894, 1519120],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"]
                },
            },
            "1": {
                "example.duration": {
                    "type": "timespan",
                    "description": "  The duration of the last foreground session.",
                    "time_unit": "second",
                    "send_in_pings": ["custom"],
                    "bugs": [1497894, 1519120],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"]
                }
            },
            "2": {
                "example.duration": {
                    "type": "timespan",
                    "description": "  The duration of the last foreground session.",
                    "time_unit": "second",
                    "send_in_pings": ["all_pings"],
                    "bugs": [1497894, 1519120],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"]
                },
            },
            "3": {
                "example.duration": {
                    "type": "timespan",
                    "description": "  The duration of the last foreground session.",
                    "time_unit": "second",
                    "send_in_pings": ["all_pings"],
                    "bugs": [1497894, 1519120],
                    "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                    "notification_emails": ["telemetry-client-dev@mozilla.com"]
                },
            },
        }
    }

    timestamps = {
        "test-repo-0": {
            "0": (2 * 60 * 60 * 24, 0),
            "1": (60 * 60 * 24, 1),
            "2": (60 * 60 * 24, 2),
            "3": (0, 3),
        }
    }

    expected_out = {
        "test-repo-0": {
            "example.duration": {
                "type": "timespan",
                "name": "example.duration",
                "history": [
                    {
                        "type": "timespan",
                        "description": "  The duration of the last foreground session.",
                        "time_unit": "second",
                        "send_in_pings": ["all_pings"],
                        "bugs": [1497894, 1519120],
                        "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                        "notification_emails": ["telemetry-client-dev@mozilla.com"],
                        "git-commits": {
                            "first": "3",
                            "last": "2"
                        },
                        "dates": {
                            "first": "1970-01-01 00:00:00",
                            "last": "1970-01-02 00:00:00"
                        },
                        "reflog-index": {
                            "first": 3,
                            "last": 2
                        }
                    },
                    {
                        "type": "timespan",
                        "description": "  The duration of the last foreground session.",
                        "time_unit": "second",
                        "send_in_pings": ["custom"],
                        "bugs": [1497894, 1519120],
                        "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                        "notification_emails": ["telemetry-client-dev@mozilla.com"],
                        "git-commits": {
                            "first": "1",
                            "last": "1"
                        },
                        "dates": {
                            "first": "1970-01-02 00:00:00",
                            "last": "1970-01-02 00:00:00"
                        },
                        "reflog-index": {
                            "first": 1,
                            "last": 1
                        }
                    },
                    {
                        "type": "timespan",
                        "description": "  The duration of the last foreground session.",
                        "time_unit": "second",
                        "send_in_pings": ["baseline"],
                        "bugs": [1497894, 1519120],
                        "data_reviews": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1512938#c3"],
                        "notification_emails": ["telemetry-client-dev@mozilla.com"],
                        "git-commits": {
                            "first": "0",
                            "last": "0"
                        },
                        "dates": {
                            "first": "1970-01-03 00:00:00",
                            "last": "1970-01-03 00:00:00"
                        },
                        "reflog-index": {
                            "first": 0,
                            "last": 0
                        }
                    }
                ]
            }
        }
    }

    assert transform.transform_metrics_by_hash(timestamps, probes) == expected_out
