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

CHANNELS = ["release", "beta"]
REPOS = ["mobile_metrics_example_1", "mobile_metrics_example_2"]

IN_NODE_DATA = {
    channel: {
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
    for channel in CHANNELS
}


def in_probe_data(moz_central=True):
    if moz_central:
        top_levels = CHANNELS
        secondary_level_prefix = "node_id_"
    else:
        top_levels = REPOS
        secondary_level_prefix = "abcdef"

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
                    },
                }
            }
        }
    }

    return {top_level: secondary_level for top_level in top_levels}


def out_probe_data(by_channel=False, include_versions=True):

    probes = [
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
        }
    ]

    if include_versions:
        probes[0]['revisions'] = {
            'first': 'node_id_2',
            'last': 'node_id_3'
        }
        probes[0]['versions'] = {
            'first': '51',
            'last': '52'
        }

        probes[1]['revisions'] = {
            'first': 'node_id_1',
            'last': 'node_id_1'
        }
        probes[1]['versions'] = {
            'first': '50',
            'last': '50'
        }

        allowed_channels = CHANNELS
    else:
        probes[0]['git-commits'] = {
            'first': 'abcdef2',
            'last': 'abcdef3'
        }
        probes[1]['git-commits'] = {
            'first': 'abcdef1',
            'last': 'abcdef1'
        }

        allowed_channels = REPOS

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
    assert(not transform.probes_equal(histogram_node1, histogram_node2))
    assert(transform.probes_equal(histogram_node2, histogram_node3))


def test_transform_monolithic():
    result = transform.transform(in_probe_data(), IN_NODE_DATA, False)
    expected = out_probe_data(by_channel=False)

    print_and_test(expected, result)


def test_transform_by_channel():
    result = transform.transform(in_probe_data(), IN_NODE_DATA, True)
    expected = out_probe_data(by_channel=True)

    print_and_test(expected, result)


def test_transform_by_hash():
    timestamps = {
        repo: {
            "abcdef{}".format(i): str(i)
            for i in range(1, 4)
        } for repo in REPOS
    }

    result = transform.transform_by_hash(timestamps, in_probe_data(False))
    expected = out_probe_data(by_channel=True, include_versions=False)

    print_and_test(expected, result)
