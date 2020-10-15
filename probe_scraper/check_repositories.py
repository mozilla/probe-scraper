import requests as reqs
import yaml

GITHUB_RAW_URL = "https://raw.githubusercontent.com"
REPOSITORIES = "../repositories.yaml"

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
            print("Checking {} metrics_file at {} ...".format(repo, temp_url))
        response = reqs.get(temp_url)
        assert response.status_code == 200
