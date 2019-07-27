# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import functools
import re

import requests
from requests_file import FileAdapter

# Add support for file:// URLs to the requests library.
# This is required for testing.
session = requests.Session()
session.mount('file://', FileAdapter())


@functools.lru_cache()
def fetch_dependency_file(url, commit_hash):
    url = url.format(commit_hash=commit_hash)
    return session.get(url)


r"""
A regular expression to parse Gradle dependency lines that look like this:

+--- org.mozilla.components:service-glean:0.53.0-SNAPSHOT (n)
+--- com.google.android.gms:play-services-ads-identifier:16.0.0 (n)
+--- androidx.fragment:fragment-testing:1.1.0-alpha08 (n)
\--- com.github.bumptech.glide:glide:4.9.0 (n)
"""
GRADLE_DEPENDENCY_LINE_REGEX = re.compile(
    r'\s*[+\\]---\s+(?P<org>[^:]+):(?P<lib>[^:]+):(?P<version>\S+).*'
)


def parse_gradle_dependencies(response):
    dependencies = {}
    for line in response.iter_lines():
        line = line.decode('utf8', 'replace')
        match = GRADLE_DEPENDENCY_LINE_REGEX.match(line)
        if match:
            data = match.groupdict()
            dependencies['{org}:{lib}'.format(**data)] = {
                'version': data['version'],
                'type': 'dependency'
            }
    return dependencies


def parse_dependencies(repository, commit_hash):
    """
    Loads and parses the dependencies for the given repository at the given
    commit hash.
    """
    if len(repository.dependencies):
        return dict((x, {"type": "dependency"}) for x in repository.dependencies)

    if repository.dependencies_url is None:
        return {}

    response = fetch_dependency_file(repository.dependencies_url, commit_hash)
    if response.status_code != 200:
        return {}

    if repository.dependencies_format == 'gradle':
        return parse_gradle_dependencies(response)
    else:
        print(f"Unknown dependency format '{repository.dependencies_format}'")
