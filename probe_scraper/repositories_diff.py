"""
Diff the Glean output of two probe-scraper runs and render it as a markdown PR
comment.

The outputs are expected to come from
`python -m probe_scraper.runner --glean --update --dry-run`, which doesn't scrape
git history, so this only covers output derived from repositories.yaml itself
(dependencies, repositories, app-listings, etc).
"""

import argparse
import difflib
import re
from pathlib import Path
from typing import List

COMMENT_MARKER = "<!-- probe-scraper-repositories-diff -->"
# GitHub rejects comments longer than 65536 characters, so leave room for the text around the diff
MAX_DIFF_LENGTH = 60000
# general files only contain the time of the run
IGNORED_FILE_NAMES = {"general"}


def diff_dirs(base_dir: Path, head_dir: Path) -> List[str]:
    def output_files(root: Path):
        return {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() and path.name not in IGNORED_FILE_NAMES
        }

    def read_lines(path: Path):
        return path.read_text().splitlines(keepends=True) if path.exists() else []

    base_files = output_files(base_dir)
    head_files = output_files(head_dir)
    # Update runs never delete per-app files (glean/<app>/*) from the bucket, so
    # an app removed from repositories.yaml keeps its old files.
    kept_files = {
        name
        for name in base_files - head_files
        if name.startswith("glean/") and name.count("/") == 2
    }

    lines = []
    for name in sorted((base_files | head_files) - kept_files):
        file_diff = list(
            difflib.unified_diff(
                read_lines(base_dir / name),
                read_lines(head_dir / name),
                fromfile=f"a/{name}",
                tofile=f"b/{name}",
            )
        )
        # make sure every line ends in a newline, including the last line of a file
        lines += [line if line.endswith("\n") else line + "\n" for line in file_diff]
    return lines


def render_comment(diff_lines: List[str]) -> str:
    header = (
        f"{COMMENT_MARKER}\n"
        "### probe-scraper output diff\n\n"
        "Changes to the Glean metadata probe-scraper would publish with this PR, "
        "compared to the base branch. Metrics, pings and tags are not scraped, "
        "so the per-app `metrics`, `pings` and `tags` files are empty here. For "
        "example, a new `metrics_files` entry or ping `moz_pipeline_metadata` "
        "shows up in `glean/repositories`, but not its effect on those files.\n\n"
    )
    if not diff_lines:
        return header + "No changes to probe-scraper output.\n"

    changed_files = sum(1 for line in diff_lines if line.startswith("+++ "))
    shown = []
    length = 0
    for line in diff_lines:
        if length + len(line) > MAX_DIFF_LENGTH:
            break
        shown.append(line)
        length += len(line)
    diff = "".join(shown)

    # use a fence longer than any run of backticks in the diff so it can't be closed early
    longest_backticks = max((len(m) for m in re.findall(r"`+", diff)), default=0)
    fence = "`" * max(3, longest_backticks + 1)

    comment = (
        header
        + f"<details>\n<summary>{changed_files} file(s) changed</summary>\n\n"
        + f"{fence}diff\n{diff}{fence}\n\n</details>\n"
    )
    if len(shown) < len(diff_lines):
        comment += (
            "\nThe diff was truncated. The full diff is in the `repositories-diff` "
            "artifact of the workflow run.\n"
        )
    return comment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-dir",
        help="Output of the base branch.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--head-dir",
        help="Output of the PR.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to write comment.md and full.diff to.",
        type=Path,
        required=True,
    )
    args = parser.parse_args()

    diff_lines = diff_dirs(args.base_dir, args.head_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "full.diff").write_text("".join(diff_lines))
    (args.output_dir / "comment.md").write_text(render_comment(diff_lines))


if __name__ == "__main__":
    main()
