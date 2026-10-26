import os
import re
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BASE_TIMESTAMP = "2026-09-01T00:00:00Z"
REVIEW_TIMESTAMP = "2026-09-02T00:00:00Z"
GUIDE = "README.md"
PLAN = "docs/plans/done.md"
ARCHIVED_PLAN = "docs/archive/plans/old.md"
COMPONENT_GUIDE = "src/component/README.md"
LAST_CHECKED_LINE = re.compile(
    r'^last_checked: "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)"$',
    re.MULTILINE,
)


class GitRepository:
    """A temporary repository whose `feature` branch is a pull request to `main`."""

    def __init__(self, root: Path, python_directory: Path) -> None:
        self.root = root
        self.commit_count = 0
        self.environment = {
            **{
                key: value
                for key, value in os.environ.items()
                if not key.startswith("GIT_")
            },
            "PATH": f"{python_directory}{os.pathsep}{os.environ['PATH']}",
            # A zone other than UTC exposes timestamps written in local time.
            "TZ": "CST-8",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "Test",
            "GIT_AUTHOR_EMAIL": "test@example.com",
            "GIT_COMMITTER_NAME": "Test",
            "GIT_COMMITTER_EMAIL": "test@example.com",
        }

    def run_git(self, *args: str) -> str:
        # Each commit gets a later date, as it would on GitHub.
        date = f"2026-09-01T00:{self.commit_count:02d}:00+00:00"
        return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [str(shutil.which("git")), *args],
            cwd=self.root,
            capture_output=True,
            text=True,
            check=True,
            env={
                **self.environment,
                "GIT_AUTHOR_DATE": date,
                "GIT_COMMITTER_DATE": date,
            },
        ).stdout

    def write(
        self,
        path: str,
        content: str,
        metadata: str | None = f'last_checked: "{BASE_TIMESTAMP}"',
    ) -> None:
        header = f"<!-- knowledge\n{metadata}\n-->\n" if metadata else ""
        document = self.root / path
        document.parent.mkdir(parents=True, exist_ok=True)
        document.write_text(header + content, encoding="utf-8")

    def replace(self, path: str, old: str, new: str) -> None:
        document = self.root / path
        text = document.read_text(encoding="utf-8")
        assert old in text
        document.write_text(text.replace(old, new), encoding="utf-8")

    def commit(self, message: str) -> None:
        self.commit_count += 1
        self.run_git("add", ".")
        self.run_git("commit", "-m", message)

    def merge(self, branch: str) -> None:
        self.commit_count += 1
        self.run_git("merge", "--no-ff", "--no-edit", branch)

    def find_changed_paths(self) -> list[str]:
        status = self.run_git("status", "--porcelain", "--untracked-files=all")
        return sorted(line[3:] for line in status.splitlines())

    def update_timestamps(self, base_sha: str) -> None:
        workflow = yaml.safe_load(
            (REPOSITORY_ROOT / ".github/workflows/autofix.yml").read_text()
        )
        step = next(
            step
            for step in workflow["jobs"]["autofix"]["steps"]
            if step.get("name") == "Update document timestamps"
        )
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            [str(shutil.which("bash")), "-e", "-c", step["run"]],
            cwd=self.root,
            env={**self.environment, "PR_BASE_SHA": base_sha},
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture
def repository(tmp_path: Path) -> GitRepository:
    # The workflow runs the runner's python3, which has no project dependencies.
    python = tmp_path / "bin/python3"
    python.parent.mkdir()
    python.write_text(f'#!/bin/sh\nexec "{sys.executable}" -S "$@"\n')
    python.chmod(0o755)
    repository = GitRepository(tmp_path / "repository", python.parent)
    repository.root.mkdir()
    repository.run_git("init", "--initial-branch=main")
    shutil.copytree(
        REPOSITORY_ROOT / ".github/scripts",
        repository.root / ".github/scripts",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    shutil.copy(REPOSITORY_ROOT / ".gitignore", repository.root)
    repository.write(
        GUIDE,
        "# Guide\n\nIntroduction.\n\n"
        '```markdown\nlast_checked: "2026-01-01T00:00:00Z"\n```\n\nDetails.\n',
        metadata=f'last_checked: "{BASE_TIMESTAMP}"\nowner: maintainers',
    )
    repository.write(PLAN, "# Done\n\nResult.\n")
    repository.write(ARCHIVED_PLAN, "# Old\n\nResult.\n")
    repository.write("CLA.md", "# Agreement\n", metadata=None)
    repository.write("module.py", "VALUE = 1\n", metadata=None)
    repository.commit("base")
    repository.run_git("switch", "--create", "feature")
    return repository


def edit_page(repository: GitRepository) -> None:
    repository.replace(GUIDE, "Details.", "Updated details.")
    repository.commit("edit guide")


def add_page(repository: GitRepository) -> None:
    repository.write(COMPONENT_GUIDE, "# Component\n")
    repository.commit("add component guide")


def update_timestamp_only(repository: GitRepository) -> None:
    repository.replace(GUIDE, BASE_TIMESTAMP, REVIEW_TIMESTAMP)
    repository.commit("review guide")


def edit_page_after_timestamp_update(repository: GitRepository) -> None:
    update_timestamp_only(repository)
    edit_page(repository)


def edit_page_with_timestamp_update(repository: GitRepository) -> None:
    repository.replace(GUIDE, "Details.", "Updated details.")
    repository.replace(GUIDE, BASE_TIMESTAMP, REVIEW_TIMESTAMP)
    repository.commit("edit and review guide")


def update_timestamps_after_editing_pages(repository: GitRepository) -> None:
    repository.replace(GUIDE, "Details.", "Updated details.")
    repository.write(COMPONENT_GUIDE, "# Component\n")
    repository.commit("edit guide and add component guide")
    repository.replace(GUIDE, BASE_TIMESTAMP, REVIEW_TIMESTAMP)
    repository.replace(COMPONENT_GUIDE, BASE_TIMESTAMP, REVIEW_TIMESTAMP)
    repository.commit("review guides")


def edit_page_and_review_other_page(repository: GitRepository) -> None:
    repository.replace(PLAN, "Result.", "Updated result.")
    repository.replace(GUIDE, BASE_TIMESTAMP, REVIEW_TIMESTAMP)
    repository.commit("edit plan and review guide")
    repository.replace("module.py", "1", "2")
    repository.commit("change code")


def edit_page_before_merging_base_branch(repository: GitRepository) -> None:
    edit_page(repository)
    repository.run_git("switch", "main")
    update_timestamp_only(repository)
    repository.run_git("switch", "feature")
    repository.merge("main")


def archive_plan(repository: GitRepository) -> None:
    archived_plan = "docs/archive/plans/done.md"
    (repository.root / PLAN).rename(repository.root / archived_plan)
    repository.replace(archived_plan, "Result.", "Updated result.")
    repository.replace(ARCHIVED_PLAN, "Result.", "Updated result.")
    repository.commit("archive plan")


def delete_page(repository: GitRepository) -> None:
    edit_page(repository)
    (repository.root / GUIDE).unlink()
    repository.commit("delete guide")


def edit_file_without_metadata(repository: GitRepository) -> None:
    repository.replace("CLA.md", "Agreement", "Updated agreement")
    repository.commit("edit agreement")


def edit_base_branch(repository: GitRepository) -> None:
    repository.run_git("switch", "main")
    edit_page(repository)
    repository.run_git("switch", "feature")
    repository.replace("module.py", "1", "2")
    repository.commit("change code")


def merge_base_branch(repository: GitRepository) -> None:
    edit_base_branch(repository)
    repository.merge("main")


def merge_base_branch_after_page_review(repository: GitRepository) -> None:
    edit_page_with_timestamp_update(repository)
    repository.run_git("switch", "main")
    repository.replace(GUIDE, "Introduction.", "Updated introduction.")
    repository.commit("edit guide introduction")
    repository.run_git("switch", "feature")
    repository.merge("main")


@pytest.mark.parametrize(
    ("change", "updated_paths"),
    [
        (edit_page, [GUIDE]),
        (add_page, [COMPONENT_GUIDE]),
        (edit_page_after_timestamp_update, [GUIDE]),
        (edit_page_and_review_other_page, [PLAN]),
        (edit_page_before_merging_base_branch, [GUIDE]),
        (edit_page_with_timestamp_update, []),
        (update_timestamp_only, []),
        (update_timestamps_after_editing_pages, []),
        (archive_plan, []),
        (delete_page, []),
        (edit_file_without_metadata, []),
        (edit_base_branch, []),
        (merge_base_branch, []),
        (merge_base_branch_after_page_review, []),
    ],
    ids=lambda value: value.__name__ if callable(value) else "-".join(value) or "none",
)
def test_pull_request_document_timestamps(
    repository: GitRepository,
    change: Callable[[GitRepository], None],
    updated_paths: list[str],
) -> None:
    change(repository)
    # CI checks out the pull request head commit, without local branches.
    base_sha = repository.run_git("rev-parse", "main").strip()
    repository.run_git("switch", "--detach")
    repository.run_git("branch", "--delete", "--force", "main", "feature")

    started_at = datetime.now(UTC).replace(microsecond=0)
    repository.update_timestamps(base_sha)
    finished_at = datetime.now(UTC)

    assert repository.find_changed_paths() == updated_paths
    for path in updated_paths:
        document = (repository.root / path).read_text()
        last_checked_line = LAST_CHECKED_LINE.search(document)
        assert last_checked_line, document
        assert started_at <= datetime.fromisoformat(last_checked_line[1]) <= finished_at
        committed_document = repository.run_git("show", f"HEAD:{path}")
        assert document == re.sub(
            r"^last_checked: .*$",
            last_checked_line[0],
            committed_document,
            count=1,
            flags=re.MULTILINE,
        )

    if updated_paths:
        # autofix.ci commits the update, which runs the workflow again. Rerun in a
        # later second so that a repeated update would change the timestamp.
        repository.commit("update document timestamps")
        rerun_at = finished_at.replace(microsecond=0) + timedelta(seconds=1)
        time.sleep(max(0.0, (rerun_at - datetime.now(UTC)).total_seconds()))
        repository.update_timestamps(base_sha)
        assert repository.find_changed_paths() == []
