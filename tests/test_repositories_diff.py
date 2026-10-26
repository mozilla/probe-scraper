import copy
import subprocess
import sys
from pathlib import Path

import yaml

from probe_scraper import repositories_diff

ROOT = Path(__file__).parent.parent
REPOSITORIES = {
    "version": "2",
    "libraries": [
        {
            "library_name": name,
            "description": name,
            "notification_emails": ["nobody@example.com"],
            "url": f"https://example.com/{name}",
            "variants": [{"v1_name": name, "dependency_name": name}],
        }
        for name in ("glean-core", "example-lib")
    ],
    "applications": [
        {
            "app_name": "example",
            "canonical_app_name": "Example",
            "app_description": "Example app",
            "notification_emails": ["nobody@example.com"],
            "url": "https://example.com/example",
            "dependencies": ["glean-core", "example-lib"],
            "channels": [
                {
                    "v1_name": f"example-{channel}",
                    "app_id": f"org.example.{channel}",
                    "app_channel": channel,
                }
                for channel in ("release", "beta")
            ],
        }
    ],
}


def generate_output(repositories_file: Path, out_dir: Path) -> Path:
    # same command the repositories-diff workflow runs
    subprocess.run(
        [
            sys.executable,
            "-m",
            "probe_scraper.runner",
            "--glean",
            "--update",
            "--dry-run",
            "--repositories-file",
            repositories_file,
            "--out-dir",
            out_dir,
        ],
        cwd=ROOT,
        check=True,
    )
    return out_dir


def test_diff_dirs(tmp_path):
    base = tmp_path / "base"
    head = tmp_path / "head"
    for d in (base, head):
        (d / "glean" / "app").mkdir(parents=True)
        (d / "glean" / "app" / "unchanged").write_text("{}\n")
    (base / "glean" / "app" / "general").write_text("old time\n")
    (head / "glean" / "app" / "general").write_text("new time\n")
    (base / "glean" / "app" / "dependencies").write_text("a\nb\n")
    (head / "glean" / "app" / "dependencies").write_text("a\nc")
    (head / "glean" / "app" / "added").write_text("new\n")
    (base / "glean" / "removed").mkdir()
    (base / "glean" / "removed" / "dependencies").write_text("{}\n")

    diff = "".join(repositories_diff.diff_dirs(base, head))

    assert "general" not in diff
    assert "unchanged" not in diff
    assert "removed" not in diff
    assert "+++ b/glean/app/added\n@@ -0,0 +1 @@\n+new\n" in diff
    assert "-b\n+c\n" in diff


def test_render_comment_no_changes():
    comment = repositories_diff.render_comment([])
    assert "No changes" in comment


def test_render_comment_fence():
    comment = repositories_diff.render_comment(["--- a/x\n", "+++ b/x\n", "+````\n"])
    assert "1 file(s) changed" in comment
    assert "`````diff\n" in comment


def test_render_comment_truncated(monkeypatch):
    monkeypatch.setattr(repositories_diff, "MAX_DIFF_LENGTH", 25)
    comment = repositories_diff.render_comment(
        ["--- a/x\n", "+++ b/x\n", "+line\n", "+another line\n"]
    )
    assert "+line\n" in comment
    assert "another line" not in comment
    assert "truncated" in comment


def test_dependency_change(tmp_path):
    base_file = tmp_path / "base.yaml"
    base_file.write_text(yaml.dump(REPOSITORIES))
    head_repos = copy.deepcopy(REPOSITORIES)
    head_repos["applications"][0]["dependencies"].remove("example-lib")
    head_file = tmp_path / "head.yaml"
    head_file.write_text(yaml.dump(head_repos))

    base_dir = generate_output(base_file, tmp_path / "base")
    head_dir = generate_output(head_file, tmp_path / "head")
    diff = "".join(repositories_diff.diff_dirs(base_dir, head_dir))

    for name in ("example-release", "example-beta"):
        assert f"+++ b/glean/{name}/dependencies" in diff
    assert "+++ b/v2/glean/app-listings" in diff
    assert '-    "name": "example-lib",' in diff
