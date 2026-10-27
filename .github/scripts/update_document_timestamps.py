# ruff: file-ignore[implicit-namespace-package]
"""Update review timestamps of knowledge documents a pull request changes.

A document's `last_checked` becomes the current UTC time when the pull request's
own commits changed its content after they last changed its metadata; adding a
document counts as a content change. Merges from the base branch do not count,
archived plans keep their timestamps, and a valid timestamp within a day of the
current time is kept.
"""

import re
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

METADATA_BLOCK = re.compile(r"\A<!-- knowledge\n(.*?)\n-->(?:\n|$)", re.DOTALL)
LAST_CHECKED_LINE = re.compile(r"^last_checked:.*$", re.MULTILINE)
LAST_CHECKED_TIMESTAMP = re.compile(
    r'^last_checked: "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)"$',
    re.MULTILINE,
)
ARCHIVE_DIRECTORY = "docs/archive/"


def run_git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [str(shutil.which("git")), *args],
        capture_output=True,
        encoding="utf-8",
        check=check,
    )


def read_document(revision: str, path: str) -> str | None:
    result = run_git("cat-file", "blob", f"{revision}:{path}", check=False)
    return result.stdout if result.returncode == 0 else None


def split_metadata(document: str) -> tuple[str, str]:
    """Return a document's metadata block and the content after it."""
    metadata = METADATA_BLOCK.match(document)
    if metadata is None:
        return "", document
    return metadata[0], document[metadata.end() :]


def is_checked_within_a_day(metadata: str, now: datetime) -> bool:
    """Return whether `last_checked` is a valid time within a day of `now`."""
    timestamp = LAST_CHECKED_TIMESTAMP.search(metadata)
    if timestamp is None:
        return False
    try:
        last_checked = datetime.fromisoformat(timestamp[1])
    except ValueError:
        return False
    return abs(now - last_checked) <= timedelta(days=1)


def needs_new_timestamp(path: str, merge_base: str) -> bool:
    """Return whether the PR changed content after it last changed metadata."""
    commits = run_git(
        "rev-list",
        "--first-parent",
        "--no-merges",
        "--topo-order",
        f"{merge_base}..HEAD",
        "--",
        path,
    ).stdout.split()
    for commit in commits:
        new_document = read_document(commit, path)
        old_document = read_document(f"{commit}^", path)
        if new_document is None:
            continue
        if old_document is None:
            return True
        new_metadata, new_content = split_metadata(new_document)
        old_metadata, old_content = split_metadata(old_document)
        if new_metadata != old_metadata:
            return False
        if new_content != old_content:
            return True
    return False


def find_documents_to_update(base: str, now: datetime) -> list[str]:
    merge_base = run_git("merge-base", base, "HEAD").stdout.strip()
    changed_paths = run_git(
        "diff-tree",
        "-r",
        "-z",
        "--name-only",
        "--no-renames",
        "--diff-filter=AM",
        merge_base,
        "HEAD",
    ).stdout.split("\0")
    paths_to_update = []
    for path in changed_paths:
        if not path.endswith(".md") or path.startswith(ARCHIVE_DIRECTORY):
            continue
        metadata, _ = split_metadata(Path(path).read_text(encoding="utf-8"))
        if (
            LAST_CHECKED_LINE.search(metadata)
            and not is_checked_within_a_day(metadata, now)
            and needs_new_timestamp(path, merge_base)
        ):
            paths_to_update.append(path)
    return paths_to_update


def update_last_checked(path: Path, timestamp: str) -> None:
    metadata, content = split_metadata(path.read_text(encoding="utf-8"))
    metadata = LAST_CHECKED_LINE.sub(f'last_checked: "{timestamp}"', metadata, count=1)
    path.write_text(metadata + content, encoding="utf-8")


if __name__ == "__main__":
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    for path in find_documents_to_update(sys.argv[1], now):
        update_last_checked(Path(path), timestamp)
