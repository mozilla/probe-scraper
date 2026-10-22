"""
Diff the Glean output that probe-scraper would publish for two versions of
repositories.yaml, and render it as a markdown PR comment.

No git history is scraped, so this only covers output derived from
repositories.yaml itself (dependencies, repositories, app-listings, etc).
"""

import argparse
import difflib
import re
import tempfile
from pathlib import Path
from typing import List

from .runner import load_glean_metrics

COMMENT_MARKER = "<!-- probe-scraper-repositories-diff -->"
# GitHub rejects comments longer than 65536 characters, so leave room for the text around the diff
MAX_DIFF_LENGTH = 60000
# These only contain the time of the run
IGNORED_FILE_NAMES = {"general", "index.html"}


def generate_output(repositories_file: Path, out_dir: Path):
    # update=True without any repo filter skips scraping and only writes the
    # output that comes from repositories.yaml
    load_glean_metrics(
        cache_dir=out_dir / "cache",
        out_dir=out_dir / "output",
        repositories_file=repositories_file,
        dry_run=True,
        glean_repos=None,
        bugzilla_api_key=None,
        update=True,
    )
    return out_dir / "output"


def diff_dirs(base_dir: Path, head_dir: Path) -> List[str]:
    def output_files(root: Path):
        return {
            path.relative_to(root).as_posix()
            for path in root.rglob("*")
            if path.is_file() and path.name not in IGNORED_FILE_NAMES
        }

    def read_lines(path: Path):
        return path.read_text().splitlines(keepends=True) if path.exists() else []

    lines = []
    for name in sorted(output_files(base_dir) | output_files(head_dir)):
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
        "Changes to the Glean metadata probe-scraper would publish with this "
        "version of `repositories.yaml`. Metrics, pings and tags are not scraped, "
        "so changes that depend on them (e.g. new `metrics_files` or ping "
        "`moz_pipeline_metadata`) don't show up here.\n\n"
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
        "--base-file",
        help="repositories.yaml to compare against.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--head-file",
        help="Changed repositories.yaml.",
        type=Path,
        default="repositories.yaml",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to write comment.md and full.diff to.",
        type=Path,
        required=True,
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        base_dir = generate_output(args.base_file, tmp_dir / "base")
        head_dir = generate_output(args.head_file, tmp_dir / "head")
        diff_lines = diff_dirs(base_dir, head_dir)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "full.diff").write_text("".join(diff_lines))
    (args.output_dir / "comment.md").write_text(render_comment(diff_lines))


if __name__ == "__main__":
    main()
