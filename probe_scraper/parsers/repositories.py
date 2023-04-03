# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import copy

import yaml

from probe_scraper import model_validation

REPOSITORIES_FILENAME = "repositories.yaml"


def remove_none(obj):
    """
    Recursively traverses a dict or list, removing all dict items where the value
    is None. This helps us meet the existing probeinfo API contract and sidesteps
    an awkward incompatibility between JSON schemas and OpenAPI schemas, which use
    incompatible constructs for marking fields as nullable.

    Implementation from https://stackoverflow.com/a/20558778
    """
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)(
            (remove_none(k), remove_none(v))
            for k, v in obj.items()
            if k is not None and v is not None
        )
    else:
        return obj


class Repository(object):
    """
    A class representing a repository, read in from `repositories.yaml`
    """

    def __init__(self, name, definition):
        self.name = name
        self.url = definition.get("url")
        self.branch = definition.get("branch", None)
        self.notification_emails = definition.get("notification_emails")
        self.app_id = definition.get("app_id")
        self.description = definition.get("description")
        self.channel = definition.get("channel")
        self.deprecated = definition.get("deprecated", False)
        self.metrics_file_paths = definition.get("metrics_files", [])
        self.ping_file_paths = definition.get("ping_files", [])
        self.tag_file_paths = definition.get("tag_files", [])
        self.library_names = definition.get("library_names", None)
        self.dependencies = definition.get("dependencies", [])
        self.prototype = definition.get("prototype", False)
        self.retention_days = definition.get("retention_days", None)
        self.encryption = definition.get("encryption", None)
        self.skip_documentation = definition.get("skip_documentation", False)
        self.moz_pipeline_metadata_defaults = definition.get(
            "moz_pipeline_metadata_defaults", {}
        )
        self.moz_pipeline_metadata = definition.get("moz_pipeline_metadata", {})

    def get_metrics_file_paths(self):
        return self.metrics_file_paths

    def get_ping_file_paths(self):
        return self.ping_file_paths

    def get_change_files(self):
        return self.metrics_file_paths + self.ping_file_paths + self.tag_file_paths

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

        version = repos.get("version", "1")
        if version == "1":
            return repos
        else:
            return self._v2_to_v1(filename)

    def validate(self, filename=None):
        data = self._get_repos(filename)
        model_validation.validate_as(data, "RepositoriesYamlV1")

    def parse(self, filename=None):
        """
        Parse the given filename as a set of repository definitions for v1 endpoints.

        The passed file can either be in the old RepositoriesYamlV1 format
        or the current RepositoriesYamlV2 format, in which case it will be
        "downgraded" to v1 format. This is to maintain existing code and output for
        the v1 probeinfo endpoints.

        New endpoints should be built with the data format returned from parse_v2
        rather than this function.
        """
        self.validate(filename)
        repos = self._get_repos(filename)

        repos = [
            Repository(name, definition) for name, definition in list(repos.items())
        ]

        return repos

    def parse_v2(self, filename=None) -> dict:
        """
        Parse the given filename as a set of repository definitions.

        The passed file must be in the current RepositoriesYamlV2 format.
        """
        with open(filename or REPOSITORIES_FILENAME, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
        model_validation.apply_defaults_and_validate(data, "RepositoriesYamlV2")
        repos = data

        app_listings = []
        for app in repos["applications"]:
            channels = app.pop("channels")
            for channel in channels:
                dependencies = app.get("dependencies", []) + channel.pop(
                    "additional_dependencies", []
                )
                listing = {**app, **channel}
                listing["dependencies"] = dependencies
                app_id = listing["app_id"]
                listing["document_namespace"] = (
                    app_id.lower().replace("_", "-").replace(".", "-")
                )
                listing["bq_dataset_family"] = (
                    app_id.lower().replace("-", "_").replace(".", "_")
                )
                # Need a deepcopy to ensure the dictionary values remain distinct.
                listing = copy.deepcopy(listing)
                model_validation.validate_as(listing, "AppListing")
                app_listings.append(listing)

        library_variants = []
        for lib in repos["libraries"]:
            variants = lib.pop("variants")
            for variant in variants:
                listing = {**lib, **variant}
                model_validation.validate_as(listing, "LibraryVariant")
                library_variants.append(listing)

        return {
            "library-variants": library_variants,
            "app-listings": app_listings,
        }

    def _v2_to_v1(self, filename):
        repos_v2 = self.parse_v2(filename)
        repos = {}
        for lib in repos_v2["library-variants"]:
            v1_name = lib["v1_name"]
            lib["library_names"] = [lib["dependency_name"]]
            lib["app_id"] = v1_name
            del lib["library_name"]
            del lib["dependency_name"]
            del lib["v1_name"]
            repos[v1_name] = lib
        for app in repos_v2["app-listings"]:
            app_channel = app.pop("app_channel", None)
            if app_channel is not None:
                app["channel"] = app_channel
            v1_name = app.pop("v1_name")
            app.pop("app_name")
            app.pop("canonical_app_name", None)
            app.pop("bq_dataset_family")
            app_description = app.pop("app_description", None)
            app["description"] = app.get("description", app_description)
            namespace = app.pop("document_namespace")
            app["app_id"] = namespace
            repos[v1_name] = app
        return repos
