# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import jsonschema
import yaml


REPOSITORIES_FILENAME = "repositories.yaml"
REPOSITORIES_SCHEMA = "schemas/repositories.json"


class Repository(object):
    """
    A class representing a repository, read in from `repositories.yaml`
    """

    def __init__(self, name, definition):
        self.name = name
        self.url = definition.get("url")
        self.notification_emails = definition.get("notification_emails")
        self.app_id = definition.get("app_id")
        self.metrics_file_paths = definition.get("metrics_files", [])
        self.library_names = definition.get("library_names", None)
        self.dependencies_url = definition.get("dependencies_url", None)
        self.dependencies_format = definition.get("dependencies_format", None)
        self.dependencies_files = definition.get("dependencies_files", [])

    def get_metrics_file_paths(self):
        return self.metrics_file_paths

    def get_change_files(self):
        return self.metrics_file_paths + self.dependencies_files

    def to_dict(self):
        # Remove null elements
        # https://google.github.io/styleguide/jsoncstyleguide.xml#Empty/Null_Property_Values
        return {k: v for k, v in list(self.__dict__.items()) if v is not None}


class RepositoriesParser(object):
    """
    A parser for `repositories.yaml` files, which both validates and retrieves Repository objects
    """

    def _get_repos(self, filename=None):
        if filename is None:
            filename = REPOSITORIES_FILENAME

        with open(filename, 'r') as f:
            repos = yaml.load(f)

        return repos

    def validate(self, filename=None):
        repos = self._get_repos(filename)

        with open(REPOSITORIES_SCHEMA, 'r') as f:
            schema = json.load(f)

        jsonschema.validate(repos, schema)

    def parse(self, filename=None):
        self.validate(filename)
        repos = self._get_repos(filename)

        return [
            Repository(name, definition)
            for name, definition
            in list(repos.items())
        ]
