import json
import os
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (REPOSITORY_ROOT / ".github/pull_request_template.md").read_text()


@pytest.mark.parametrize(
    ("change", "valid"),
    [
        ("Closes #123", True),
        ("Refs langgenius/graphon#123", True),
        ("Fixes https://github.com/langgenius/graphon/issues/123", True),
        ("no issue", False),
        ("unchecked", True),
        ("extended template", True),
        ("missing extension", False),
        *(
            ("omit " + line, False)
            for line in TEMPLATE.splitlines()
            if line.startswith(("## ", "- [ ] "))
        ),
        ("empty", False),
    ],
)
def test_pull_request_body(tmp_path: Path, change: str, valid: bool) -> None:
    template = TEMPLATE
    if change in {"extended template", "missing extension"}:
        template += "\n## Compatibility\n- [ ] Describe compatibility\n"
    template_path = tmp_path / ".github/pull_request_template.md"
    template_path.parent.mkdir()
    template_path.write_text(template)
    body = template.replace("Closes #", "Closes #123")
    if change.startswith("omit "):
        body = body.replace(change.removeprefix("omit "), "")
    elif change == "missing extension":
        body = body.replace("- [ ] Describe compatibility", "")
    elif change == "empty":
        body = None
    elif change == "no issue":
        body = body.replace("Closes #123", "Closes #")
    elif change not in {"unchecked", "extended template"}:
        body = body.replace("Closes #123", change)
    if body is not None and change != "unchecked":
        body = body.replace("[ ]", "[x]")
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps({"pull_request": {"body": body}}))

    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [sys.executable, str(REPOSITORY_ROOT / ".github/scripts/check_pr.py")],
        cwd=tmp_path,
        env={**os.environ, "GITHUB_EVENT_PATH": str(event_path)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert (result.returncode == 0) is valid, result.stdout + result.stderr
    if not valid:
        assert "PR body" in result.stderr


@pytest.mark.parametrize(
    ("change", "valid"),
    [
        ("unchanged", True),
        ("new fixture", True),
        ("modified", False),
        ("engine modified", False),
        ("deleted", False),
        ("credentials", False),
    ],
)
def test_preserved_repository_files(tmp_path: Path, change: str, valid: bool) -> None:
    def run_git(*args: str) -> str:
        return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [str(shutil.which("git")), *args],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=True,
            env={
                **os.environ,
                "GIT_AUTHOR_NAME": "Test",
                "GIT_AUTHOR_EMAIL": "test@example.com",
                "GIT_COMMITTER_NAME": "Test",
                "GIT_COMMITTER_EMAIL": "test@example.com",
            },
        ).stdout.strip()

    run_git("init")
    fixture_directory = "engine" if change == "engine modified" else "runtime"
    fixture = tmp_path / f"tests/{fixture_directory}/fixtures/historical.json"
    fixture.parent.mkdir(parents=True)
    fixture.write_text("original snapshot")
    run_git("add", ".")
    run_git("-c", "core.hooksPath=/dev/null", "commit", "-m", "base")
    base_sha = run_git("rev-parse", "HEAD")
    if change == "new fixture":
        fixture.with_name("new.json").write_text("new snapshot")
    elif change == "engine modified":
        fixture.write_text("original snapshot ")
    elif change == "modified":
        fixture.write_text("replacement snapshot")
    elif change == "deleted":
        fixture.unlink()
    elif change == "credentials":
        credentials = tmp_path / "examples/slim_llm/credentials.json"
        credentials.parent.mkdir(parents=True)
        credentials.write_text("{}")
    run_git("add", ".")
    run_git("-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "change")
    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github/workflows/pr.yml").read_text()
    )
    step = next(
        step
        for step in workflow["jobs"]["check"]["steps"]
        if step.get("name") == "Check preserved repository files"
    )

    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [str(shutil.which("bash")), "-e", "-c", step["run"]],
        cwd=tmp_path,
        env={**os.environ, "PR_BASE_SHA": base_sha},
        capture_output=True,
        text=True,
        check=False,
    )

    assert (result.returncode == 0) is valid, result.stdout + result.stderr
