import probe_scraper.transform_probes as transform
import pprint

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

IN_NODE_DATA = {
    "release": {
        "node_id_1": {
            "version": "50",
        },
        "node_id_2": {
            "version": "51",
        },
        "node_id_3": {
            "version": "52",
        },
    }
}

IN_PROBE_DATA = {
    "release": {
        "node_id_1": {
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
                    },
                }
             }

        },
        "node_id_2": {
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
                    },
                }
             }
        },
        "node_id_3": {
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
                    },
                }
            }
        }
    }
}

OUT_PROBE_DATA_MONOLITHIC = {
    'histogram/TEST_HISTOGRAM_1': {
        'history': {
            'release': [
                {
                    'cpp_guard': None,
                    'description': 'A description.',
                    'details': {
                        'high': 10,
                        'keyed': False,
                        'kind': 'exponential',
                        'low': 1,
                        'n_buckets': 5
                    },
                    'expiry_version': '53.0',
                    'optout': True,
                    'revisions': {
                        'first': 'node_id_2',
                        'last': 'node_id_3'
                    },
                    'versions': {
                        'first': '51',
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
                        'n_buckets': 5
                    },
                    'expiry_version': '53.0',
                    'optout': False,
                    'revisions': {
                        'first': 'node_id_1',
                        'last': 'node_id_1'
                    },
                    'versions': {
                        'first': '50',
                        'last': '50'
                    }
                }
            ]
        },
        'name': 'TEST_HISTOGRAM_1',
        'type': 'histogram'
    }
}

OUT_PROBE_DATA_BY_CHANNEL = {
    'release': {
        'histogram/TEST_HISTOGRAM_1': {
            'history': {
                'release': [
                    {
                        'cpp_guard': None,
                        'description': 'A description.',
                        'details': {
                            'high': 10,
                            'keyed': False,
                            'kind': 'exponential',
                            'low': 1,
                            'n_buckets': 5
                        },
                        'expiry_version': '53.0',
                        'optout': True,
                        'revisions': {
                            'first': 'node_id_2',
                            'last': 'node_id_3',
                        },
                        'versions': {
                            'first': '51',
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
                            'n_buckets': 5
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
                ],
            },
            'name': 'TEST_HISTOGRAM_1',
            'type': 'histogram'
        }
    }
}


def test_probes_equal():
    DATA = IN_PROBE_DATA["release"]
    histogram_node1 = DATA["node_id_1"]["histogram"]["TEST_HISTOGRAM_1"]
    histogram_node2 = DATA["node_id_2"]["histogram"]["TEST_HISTOGRAM_1"]
    histogram_node3 = DATA["node_id_3"]["histogram"]["TEST_HISTOGRAM_1"]
    assert(not transform.probes_equal('histogram', histogram_node1, histogram_node2))
    assert(transform.probes_equal('histogram', histogram_node2, histogram_node3))


def test_transform_monolithic():
    result = transform.transform(IN_PROBE_DATA, IN_NODE_DATA, False)

    pp = pprint.PrettyPrinter(indent=2)
    print "\nresult:"
    pp.pprint(result)
    assert(result == OUT_PROBE_DATA_MONOLITHIC)


def test_transform_by_channel():
    result = transform.transform(IN_PROBE_DATA, IN_NODE_DATA, True)

    pp = pprint.PrettyPrinter(indent=2)
    print "\nresult:"
    pp.pprint(result)
    assert(result == OUT_PROBE_DATA_BY_CHANNEL)
