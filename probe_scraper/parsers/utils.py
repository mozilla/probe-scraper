# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


def set_in_nested_dict(dictionary, path, value):
    """Set a property in a nested dictionary by specifying a path to it.

    A call like e.g.:
      set_in_nested_dict(d, "a/b/c", 1)
    is equivalent to:
      d["a"]["b"]["c"] = 1
    """
    keys = path.split("/")
    for k in keys[:-1]:
        dictionary = dictionary[k]
    dictionary[keys[-1]] = value


def get_major_version(version):
    """Extracts the major (leftmost) version of a version string.

    :param version: the version string (e.g. "53.1")
    :return: a string containing the leftmost number before the first dot
    """
    return version.split(".")[0]
