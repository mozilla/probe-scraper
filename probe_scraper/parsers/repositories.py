# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import jsonschema
import yaml

REPOSITORIES_FILENAME = "repositories.yaml"

with open("schemas/repositories_v2.yaml", "r") as f:
    REPOSITORIES_SCHEMA = yaml.load(f, Loader=yaml.SafeLoader)

with open("schemas/repository_v1.yaml", "r") as f:
    REPOSITORY_V1_SCHEMA = yaml.load(f, Loader=yaml.SafeLoader)


class Repository(object):
    """
    A class representing a repository, read in from `repositories.yaml`
    """

    default_branch = "master"

    def __init__(self, name, definition):
        self.name = name
        self.url = definition.get("url")
        self.branch = definition.get("branch", Repository.default_branch)
        self.notification_emails = definition.get("notification_emails")
        self.app_id = definition.get("app_id")
        self.description = definition.get("description")
        self.channel = definition.get("channel")
        self.deprecated = definition.get("deprecated", False)
        self.metrics_file_paths = definition.get("metrics_files", [])
        self.ping_file_paths = definition.get("ping_files", [])
        self.library_names = definition.get("library_names", None)
        self.dependencies = definition.get("dependencies", [])
        self.prototype = definition.get("prototype", False)
        self.retention_days = definition.get("retention_days", None)

    @staticmethod
    def from_v2_repo(definition):
        d = definition.copy()
        d["name"] = d["v1_name"]
        d["app_id"] = d["app_id"].lower().replace(".", "-").replace("_", "-")
        channel = d.get("app_channel", None)
        if channel:
            d["channel"] = channel
        jsonschema.validate(d, REPOSITORY_V1_SCHEMA)
        return Repository(definition["v1_name"], d)

    @staticmethod
    def from_v2_library(definition):
        d = definition.copy()
        d["name"] = definition["library_id"]
        d["app_id"] = definition["library_id"]
        jsonschema.validate(d, REPOSITORY_V1_SCHEMA)
        return Repository(definition["library_id"], d)

    def get_branches(self):
        if self.branch == Repository.default_branch:
            return (Repository.default_branch, "main")
        return (self.branch,)

    def get_metrics_file_paths(self):
        return self.metrics_file_paths

    def get_ping_file_paths(self):
        return self.ping_file_paths

    def get_change_files(self):
        return self.metrics_file_paths + self.ping_file_paths

    def get_dependencies(self):
        return self.dependencies

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

        with open(filename, "r") as f:
            repos = yaml.load(f, Loader=yaml.SafeLoader)

        return repos

    def validate(self, filename=None):
        repos = self._get_repos(filename)

        jsonschema.validate(repos, REPOSITORIES_SCHEMA)

    def filter_repos(self, repos, glean_repo):
        if glean_repo is None:
            return repos

        return [r for r in repos if r.name == glean_repo]

    def parse(self, filename=None, glean_repo=None):
        self.validate(filename)
        repos = self._get_repos(filename)

        v2_apps = []
        for family in repos["application_families"]:
            apps = family.pop("apps")
            for app in apps:
                dependencies = family.get("dependencies", []) + app.get("dependencies", [])
                app = {**family, **app}
                app["dependencies"] = dependencies
                v2_apps.append(app)
        repos = [
            Repository.from_v2_library(definition) for definition in repos["libraries"]
        ] + [
            Repository.from_v2_repo(app) for app in v2_apps
        ]
        return self.filter_repos(repos, glean_repo)
