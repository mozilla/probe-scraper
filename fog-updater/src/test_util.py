# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/


from fog_update import eval_extract, swap_file_list


REPOSITORIES_YAML = """
---
version: "2"
libraries:
  - library_name: gecko
    description: The browser engine developed by Mozilla
    notification_emails:
      - chutten@mozilla.com
    url: https://github.com/mozilla/gecko-dev
    metrics_files:
      - LIB_METRICS_FILES
    ping_files:
      - LIB_PING_FILES

applications:
  - app_name: firefox_desktop
    metrics_files:
      - METRICS_FILES
    ping_files:
      - PING_FILES
  - app_name: firefox_desktop_background_update
    metrics_files:
      - OTHER_METRICS_FILES
    ping_files:
      - OTHER_PING_FILES
"""

METRICS_INDEX = """
# -*- Mode: python; indent-tabs-mode: nil; tab-width: 40 -*-
# vim: set filetype=python:

first_yamls = ["A", "B"]
second_yamls = ["B", "C"]
metrics_yamls = sorted(list(set(first_yamls + second_yamls)))

pings_yamls = [
    "D",
    "E",
    "F"
]
"""


def test_eval_metrics_index():
    data = eval_extract(METRICS_INDEX)
    assert data["first_yamls"] == ["A", "B"]
    assert data["second_yamls"] == ["B", "C"]
    assert data["metrics_yamls"] == ["A", "B", "C"]
    assert data["pings_yamls"] == ["D", "E", "F"]


def test_swap_repositories_yaml():
    data = eval_extract(METRICS_INDEX)
    metrics_files = data["metrics_yamls"]
    output = swap_file_list(
        REPOSITORIES_YAML, "firefox_desktop", metrics_files, "metrics"
    )

    # New files added.
    assert "- METRICS_FILES" not in output
    assert "- A" in output
    assert "- B" in output
    assert "- C" in output
    # ping files untouched.
    assert "- PING_FILES" in output

    # Other app untouched
    assert "- OTHER_METRICS_FILES" in output
    assert "- OTHER_PING_FILES" in output


def test_swap_ping_files():
    data = eval_extract(METRICS_INDEX)
    metrics_files = data["pings_yamls"]
    output = swap_file_list(
        REPOSITORIES_YAML, "firefox_desktop", metrics_files, "pings"
    )

    # metrics files untouched.
    assert "- METRICS_FILES" in output
    # New files added.
    assert "- PING_FILES" not in output
    assert "- D" in output
    assert "- E" in output
    assert "- F" in output

    # Other app untouched
    assert "- OTHER_METRICS_FILES" in output
    assert "- OTHER_PING_FILES" in output


def test_swap_repositories_yaml_unchanged():
    metrics_files = ["METRICS_FILES"]
    output = swap_file_list(
        REPOSITORIES_YAML, "firefox_desktop", metrics_files, "metrics"
    )

    # New files added.
    assert "- METRICS_FILES" in output
    assert "- A" not in output
    # ping files untouched.
    assert "- PING_FILES" in output

    # Other app untouched
    assert "- OTHER_METRICS_FILES" in output
    assert "- OTHER_PING_FILES" in output


def test_libraries():
    data = eval_extract(METRICS_INDEX)
    metrics_files = data["metrics_yamls"]
    output = swap_file_list(
        REPOSITORIES_YAML, "gecko", metrics_files, "metrics", library=True
    )

    # New files added.
    assert "- LIB_METRICS_FILES" not in output
    assert "- A" in output
    assert "- B" in output
    assert "- C" in output
    # ping files untouched.
    assert "- LIB_PING_FILES" in output

    # Other app untouched
    assert "- METRICS_FILES" in output
    assert "- PING_FILES" in output
    assert "- OTHER_METRICS_FILES" in output
    assert "- OTHER_PING_FILES" in output
