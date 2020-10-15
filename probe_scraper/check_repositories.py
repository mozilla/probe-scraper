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
    repos = yaml.load(data, Loader=yaml.SafeLoader)
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
            response = reqs.get(temp_url)
            assert response.status_code == 200
            with open("temp-metrics.yaml", "w") as filehandle:
                filehandle.write(response.text)
            yaml_lint_errors = open("yaml-lint-errors.txt", "w")
            temp_erros = lint_yaml_files(
                [Path("./temp-metrics.yaml")], yaml_lint_errors, {}
            )
            if temp_erros:
                if "prototype" in repos[repo] and not repos[repo]["prototype"]:
                    validation_errors.append({"repo": repo, "errors": temp_erros})
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
