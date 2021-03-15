import os
from pathlib import Path

import requests as reqs
import yaml
from glean_parser.lint import lint_yaml_files

GITHUB_RAW_URL = "https://raw.githubusercontent.com"
REPOSITORIES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "repositories.yaml"
)
validation_errors = []
with open(REPOSITORIES) as data:
    repos_data = yaml.load(data, Loader=yaml.SafeLoader)
    repos = repos_data["libraries"] + repos_data["applications"]
    for repo in repos:
        repo_url = repo["url"]
        branch = "master"
        if "branch" in repo:
            branch = repo["branch"]
        metrics_files = []
        if "metrics_files" in repo:
            metrics_files = repo["metrics_files"]
        temp_errors = []
        for metric_file in metrics_files:
            temp_url = (
                GITHUB_RAW_URL
                + repo_url.replace("https://github.com", "")
                + "/"
                + branch
                + "/"
                + metric_file
            )
            response = reqs.get(temp_url)
            if response.status_code != 200:
                temp_errors += ["Metrics file was not found at " + temp_url]
            else:
                with open("temp-metrics.yaml", "w") as filehandle:
                    filehandle.write(response.text)
                yaml_lint_errors = open("yaml-lint-errors.txt", "w")
                temp_errors += lint_yaml_files(
                    [Path("./temp-metrics.yaml")], yaml_lint_errors, {}
                )
        if temp_errors:
            if not repo.get("prototype", None):
                validation_errors.append({"repo": repo, "errors": temp_errors})
    os.remove("temp-metrics.yaml")
    os.remove("yaml-lint-errors.txt")
    if validation_errors:
        print("\nSummary of validation errors \n====================================\n")
        print(f"{len(validation_errors)} repositories had problems\n")
        for error in validation_errors:
            print(
                f"\nErrors found in {error['repo']} \n====================================\n"
            )
            for line_errors in error["errors"]:
                print(line_errors)
        exit(1)
