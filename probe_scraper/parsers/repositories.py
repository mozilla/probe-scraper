# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import Counter

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

    def __init__(self, definition):
        self.v1_name = definition.get("v1_name", None)
        self.app_name = definition.get("app_name", None)
        self.canonical_app_name = definition.get("canonical_app_name", None)
        self.url = definition.get("url")
        self.branch = definition.get("branch", Repository.default_branch)
        self.notification_emails = definition.get("notification_emails")
        self.app_id = definition.get("app_id")
        self.description = definition.get("description")
        self.app_channel = definition.get("app_channel")
        self.deprecated = definition.get("deprecated", False)
        self.metrics_file_paths = definition.get("metrics_files", [])
        self.ping_file_paths = definition.get("ping_files", [])
        self.library_names = definition.get("library_names", None)
        self.dependencies = definition.get("dependencies", [])
        self.prototype = definition.get("prototype", False)
        self.retention_days = definition.get("retention_days", None)

    @property
    def name(self):
        """Legacy "name" field kept for compatibility with the v1 API."""
        return self.v1_name

    @property
    def document_namespace(self):
        if self.app_id:
            return self.app_id.lower().replace(".", "-").replace("_", "-")

    @property
    def bq_dataset_family(self):
        if self.app_id:
            return self.app_id.lower().replace(".", "_").replace("-", "_")

    @staticmethod
    def from_v2_app(definition):
        d = definition.copy()
        d["name"] = d["v1_name"]
        d["app_id"] = d["app_id"].lower().replace(".", "-").replace("_", "-")
        channel = d.get("app_channel", None)
        if channel:
            d["channel"] = channel
        jsonschema.validate(d, REPOSITORY_V1_SCHEMA)
        return Repository(definition)

    @staticmethod
    def from_v2_library(definition):
        d = definition.copy()
        d["name"] = definition["v1_name"]
        d["app_id"] = definition["v1_name"]
        jsonschema.validate(d, REPOSITORY_V1_SCHEMA)
        return Repository(definition)

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
        """Dict representation of the repo for the V1 API."""
        # Remove null elements
        # https://google.github.io/styleguide/jsoncstyleguide.xml#Empty/Null_Property_Values
        d = {k: v for k, v in list(self.__dict__.items()) if v is not None}
        d["name"] = self.name
        d.pop("v1_name", None)
        d.pop("app_name", None)
        d.pop("canonical_app_name", None)
        if self.document_namespace:
            d["app_id"] = self.document_namespace
        channel = d.pop("app_channel", None)
        if channel:
            d["channel"] = channel
        return d

    def to_v2_dict(self):
        """Dict representation of the repo for the V2 API."""
        # Remove null elements
        # https://google.github.io/styleguide/jsoncstyleguide.xml#Empty/Null_Property_Values
        d = {k: v for k, v in list(self.__dict__.items()) if v is not None}
        if self.document_namespace:
            d["document_namespace"] = self.document_namespace
        if self.bq_dataset_family:
            d["bq_dataset_family"] = self.bq_dataset_family
        return d


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
                dependencies = family.get("dependencies", []) + app.get(
                    "dependencies", []
                )
                app = {**family, **app}
                app["dependencies"] = dependencies
                v2_apps.append(app)
        repos = [
            Repository.from_v2_library(definition) for definition in repos["libraries"]
        ] + [Repository.from_v2_app(app) for app in v2_apps]

        repo_name_counts = Counter([r.name for r in repos])
        duplicated_names = [k for k, v in repo_name_counts.items() if v > 1]
        assert (
            len(duplicated_names) == 0
        ), f"Found duplicate identifiers: {duplicated_names}"

        return self.filter_repos(repos, glean_repo)
