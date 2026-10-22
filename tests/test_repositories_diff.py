from pathlib import Path

import yaml

from probe_scraper import repositories_diff

REPOSITORIES = Path(__file__).parent.parent / "repositories.yaml"


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

    diff = "".join(repositories_diff.diff_dirs(base, head))

    assert "general" not in diff
    assert "unchanged" not in diff
    assert "+++ b/glean/app/added\n@@ -0,0 +1 @@\n+new\n" in diff
    assert "-b\n+c\n" in diff


def test_render_comment_no_changes():
    comment = repositories_diff.render_comment([])
    assert comment.startswith(repositories_diff.COMMENT_MARKER)
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
    repos = yaml.safe_load(REPOSITORIES.read_text())
    app = next(a for a in repos["applications"] if a["app_name"] == "firefox_ios")
    removed = app["dependencies"].pop()
    head_file = tmp_path / "repositories.yaml"
    head_file.write_text(yaml.dump(repos))

    base_dir = repositories_diff.generate_output(REPOSITORIES, tmp_path / "base")
    head_dir = repositories_diff.generate_output(head_file, tmp_path / "head")
    diff = "".join(repositories_diff.diff_dirs(base_dir, head_dir))

    for name in ("firefox-ios-release", "firefox-ios-beta", "firefox-ios-dev"):
        assert f"+++ b/glean/{name}/dependencies" in diff
    assert "+++ b/v2/glean/app-listings" in diff
    assert f'-    "name": "{removed}",' in diff
