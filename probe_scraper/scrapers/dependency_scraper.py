# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import io
import os
import re
import subprocess


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
    for line in response.readlines():
        match = GRADLE_DEPENDENCY_LINE_REGEX.match(line)
        if match:
            data = match.groupdict()
            dependencies['{org}:{lib}'.format(**data)] = {
                'version': data['version'],
                'type': 'dependency'
            }
    return dependencies


def get_gradle_dependencies(repo_path, repository):
    process = subprocess.run(
        [
            os.path.join(os.path.abspath(repo_path), 'gradlew'),
            'app:dependencies',
            '--configuration',
            'implementation'
        ],
        capture_output=True,
        cwd=repo_path,
        encoding='utf-8'
    )
    output = io.StringIO(process.stdout)
    return parse_gradle_dependencies(output)


def parse_dependencies(repo_path, repository):
    """
    Checks out the given repository to the given commit_hash, and runs
    the appropriate script to parse the dependencies at that commit.
    """
    if len(repository.dependencies):
        return dict((x, {"type": "dependency"}) for x in repository.dependencies)

    if repository.dependencies_format is None:
        return {}

    if repository.dependencies_format == 'gradle':
        return get_gradle_dependencies(repo_path, repository)
    else:
        print(f"Unknown dependency format '{repository.dependencies_format}'")
