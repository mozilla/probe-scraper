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
    repos = yaml.load(data, Loader=yaml.FullLoader)
    for repo in repos:
        repo_url = repos[repo]["url"]
        branch = "master"
        if "branch" in repos[repo]:
            branch = repos[repo]["branch"]
        metrics_files = []
        if "metrics_files" in repos[repo]:
            metrics_files = repos[repo]["metrics_files"]
        if metrics_files:
            temp_url = (
                GITHUB_RAW_URL
                + repo_url.replace("https://github.com", "")
                + "/"
                + branch
                + "/"
                + metrics_files[0]
            )
            print(f"Downloading {repo} metrics_file from {temp_url}...")
            response = reqs.get(temp_url)
            assert response.status_code == 200
            print(f"Checking {repo} metrics_file {metrics_files[0]}...\n")
            with open("temp-metrics.yaml", "w") as filehandle:
                filehandle.write(response.text)
            yaml_lint_errors = open("yaml-lint-errors.txt", "w")
            temp_erros = lint_yaml_files(
                [Path("./temp-metrics.yaml")], yaml_lint_errors, {}
            )
            if temp_erros:
                print(f"Errors found in {repo} metrics file : {print(temp_erros)}")
                if "prototype" in repos[repo] and repos[repo]["prototype"]:
                    validation_errors.append(temp_erros)
    os.remove("temp-metrics.yaml")
    os.remove("yaml-lint-errors.txt")
    if validation_errors:
        exit(1)
