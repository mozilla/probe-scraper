#!/usr/bin/env python3

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/

from github import Github, GithubException, InputGitAuthor, enable_console_debug_logging
import datetime
import difflib
import io
import os
import requests
import sys

DEFAULT_ORGANIZATION = "mozilla"
DEFAULT_AUTHOR_NAME = "data-updater"
DEFAULT_AUTHOR_EMAIL = "telemetry-alerts@mozilla.com"
USAGE = "usage: fog-update"
INDEX_URL = "https://raw.githubusercontent.com/mozilla/gecko-dev/master/toolkit/components/glean/metrics_index.py"  # noqa
BODY_TEMPLATE = f"""This (automated) patch updates the list from metrics_index.py.

For reviewers:

* Canonical source for the index: <{INDEX_URL}>
* Please double-check that the changes here are valid and that the referenced files exist.
    * If the referenced files do not exist, schema deploys will fail
* Please double-check that none of the files are _deleted_ from the list.
    * Deleted files result in incompatible schema changes that might not be immediately caught.
* Delete this branch after merging or closing the PR.

---

The source code of this automation bot lives in <https://github.com/mozilla/probe-scraper/tree/main/fog-updater>.
"""  # noqa


class UnmodifiedException(Exception):
    pass


def ts():
    return str(datetime.datetime.now())


def eval_extract(code):
    """
    Eval `code` and return a map of variables and their values.

    `code` should be valid Python code.
    Only the builtins `list` and `set` are provided.

    Note: this executes arbitrary Python code.
    Because of the limited builtins list this should be reasonably safe.
    Still only use this with known valid code!
    """

    # Allow `list` and `set`, so `list(set(a+b+c))` works.
    globs = {"__builtins__": {"list": list, "set": set, "sorted": sorted}}
    exec(code, globs)
    globs.pop("__builtins__")
    return globs


def swap_file_list(content, app, files, metrics_or_pings, library=False):
    """
    Replace the list of `metrics_files` or `ping_files` in `content` with `files`
    for the given app or library..
    Returns the changed content.

    All other content is left untouched.
    YAML syntax is assumed.
    File entries are correctly indented.
    """
    output = io.StringIO()
    state = None
    if library:
        app = f"- library_name: {app}"
    else:
        app = f"- app_name: {app}"
    indent = 0

    lines = content.split("\n")

    # Remove trailing newlines.
    while not lines[-1]:
        lines.pop()

    for line in lines:
        if state is None and line.strip() == app:
            state = "app"
        elif (
            state == "app"
            and metrics_or_pings == "metrics"
            and "metrics_files:" in line
        ):
            state = "files"
        elif state == "app" and metrics_or_pings == "pings" and "ping_files:" in line:
            state = "files"
        elif state == "files":
            if line.strip().startswith("-"):
                indent = line.find("-")
                continue
            else:
                for file in files:
                    print(" " * indent, file=output, end="")
                    print(f"- {file}\n", file=output, end="")
                state = None

        print(line, file=output)

    return output.getvalue()


def get_latest_metrics_index():
    r = requests.get(INDEX_URL)
    r.raise_for_status()
    return r.text


def _rewrite_repositories_yaml(repo, branch, data, debug=False):
    contents = repo.get_contents("repositories.yaml", ref=branch)
    content = contents.decoded_content.decode("utf-8")

    new_content = content
    for item in data:
        name, metrics_or_pings, library, files = item
        new_content = swap_file_list(
            new_content, name, files, metrics_or_pings, library
        )

    if content == new_content:
        raise UnmodifiedException(
            "Update to repositories.yaml resulted in no changes: maybe the file was already up to date?"  # noqa
        )

    if debug:
        diff = difflib.unified_diff(
            content.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile="old/repositories.yaml",
            tofile="new/repositories.yaml",
        )
        sys.stdout.writelines(diff)

    return new_content


def _commit_repositories_yaml(repo, branch, author, new_content):
    contents = repo.get_contents("repositories.yaml", ref=branch)

    repo.update_file(
        contents.path,
        "Update repositories.yaml with new FOG metrics_yamls list",
        new_content,
        contents.sha,
        branch=branch,
        author=author,
    )

    return True


def main(argv, repo, author, debug=False, dry_run=False):
    if len(argv) < 1:
        print(USAGE)
        sys.exit(1)

    release_branch_name = "main"
    short_version = "main"

    metrics_index = get_latest_metrics_index()
    data = eval_extract(metrics_index)
    gecko_metrics = sorted(data["gecko_metrics"])
    gecko_pings = sorted(data["gecko_pings"])
    firefox_desktop_metrics = sorted(data["firefox_desktop_metrics"])
    firefox_desktop_pings = sorted(data["firefox_desktop_pings"])
    background_update_metrics = sorted(data["background_update_metrics"])
    background_update_pings = sorted(data["background_update_pings"])
    background_tasks_metrics = sorted(data["background_tasks_metrics"])
    background_tasks_pings = sorted(data["background_tasks_pings"])

    data = [
        # Name, metrics/pings, library?, files
        ["gecko", "metrics", True, gecko_metrics],
        ["gecko", "pings", True, gecko_pings],
        ["firefox_desktop", "metrics", False, firefox_desktop_metrics],
        ["firefox_desktop", "pings", False, firefox_desktop_pings],
        [
            "firefox_desktop_background_update",
            "metrics",
            False,
            background_update_metrics,
        ],
        ["firefox_desktop_background_update", "pings", False, background_update_pings],
        [
            "firefox_desktop_background_tasks",
            "metrics",
            False,
            background_tasks_metrics,
        ],
        ["firefox_desktop_background_tasks", "pings", False, background_tasks_pings],
    ]

    print(f"{ts()} Updating repositories.yaml")
    try:
        new_content = _rewrite_repositories_yaml(
            repo, release_branch_name, data, debug=dry_run or debug
        )
    except UnmodifiedException as e:
        print(f"{ts()} {e}")
        return
    except Exception as e:
        print(f"{ts()} {e}")
        raise

    if dry_run:
        print(f"{ts()} Dry-run so not continuing.")
        return

    # Create a non unique PR branch name for work on this ac release branch.
    pr_branch_name = f"fog-update/update-metrics-index-{short_version}"

    try:
        pr_branch = repo.get_branch(pr_branch_name)
        if pr_branch:
            print(f"{ts()} The PR branch {pr_branch_name} already exists. Exiting.")
            return
    except GithubException:
        # TODO Only ignore a 404 here, fail on others
        pass

    release_branch = repo.get_branch(release_branch_name)
    print(f"{ts()} Last commit on {release_branch_name} is {release_branch.commit.sha}")

    print(f"{ts()} Creating branch {pr_branch_name} on {release_branch.commit.sha}")
    repo.create_git_ref(
        ref=f"refs/heads/{pr_branch_name}", sha=release_branch.commit.sha
    )
    print(f"{ts()} Created branch {pr_branch_name} on {release_branch.commit.sha}")

    _commit_repositories_yaml(repo, pr_branch_name, author, new_content)

    print(f"{ts()} Creating pull request")
    pr = repo.create_pull(
        title=f"Update to latest metrics_index list on {release_branch_name}",
        body=BODY_TEMPLATE,
        head=pr_branch_name,
        base=release_branch_name,
    )
    print(f"{ts()} Pull request at {pr.html_url}")


if __name__ == "__main__":
    debug = os.getenv("DEBUG") is not None
    if debug:
        enable_console_debug_logging()

    github_access_token = os.getenv("GITHUB_TOKEN")
    if not github_access_token:
        print("No GITHUB_TOKEN set. Exiting.")
        sys.exit(1)

    github = Github(github_access_token)
    if github.get_user() is None:
        print("Could not get authenticated user. Exiting.")
        sys.exit(1)

    dry_run = os.getenv("DRY_RUN") == "True"

    organization = os.getenv("GITHUB_REPOSITORY_OWNER") or DEFAULT_ORGANIZATION

    repo = github.get_repo(f"{organization}/probe-scraper")

    author_name = os.getenv("AUTHOR_NAME") or DEFAULT_AUTHOR_NAME
    author_email = os.getenv("AUTHOR_EMAIL") or DEFAULT_AUTHOR_EMAIL
    author = InputGitAuthor(author_name, author_email)

    print(
        f"{ts()} This is fog-update working on https://github.com/{organization} as {author_email} / {author_name}"  # noqa
    )

    main(sys.argv, repo, author, debug, dry_run)
