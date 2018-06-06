# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import jsonschema
import yaml


REPOSITORIES_FILENAME = "repositories.yaml"
REPOSITORIES_SCHEMA = "schemas/repositories.json"

HISTOGRAM_KEY = "histogram"
SCALAR_KEY = "scalar"
EVENT_KEY = "event"


class Repository(object):
    """
    A class representing a repository, read in from `repositories.yaml`
    """

    def __init__(self, name, definition):
        self.name = name
        self.url = definition.get("url")
        self.notification_emails = definition.get("notification_emails")
        self.app_name = definition.get("app_name")
        self.os = definition.get("os")
        self.histogram_file_paths = definition.get("histogram_file_paths", [])
        self.scalar_file_paths = definition.get("scalar_file_paths", [])
        self.event_file_paths = definition.get("event_file_paths", [])

    def get_probe_paths(self):
        return (self.get_histogram_paths() +
                self.get_scalar_paths() +
                self.get_event_paths())

    def get_histogram_paths(self):
        return [(HISTOGRAM_KEY, p) for p in self.histogram_file_paths]

    def get_scalar_paths(self):
        return [(SCALAR_KEY, p) for p in self.scalar_file_paths]

    def get_event_paths(self):
        return [(EVENT_KEY, p) for p in self.event_file_paths]

    def to_dict(self):
        # Remove null elements
        # https://google.github.io/styleguide/jsoncstyleguide.xml#Empty/Null_Property_Values
        return {k: v for k, v in self.__dict__.items() if v is not None}


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
            in repos.items()
        ]
