# ruff: file-ignore[implicit-namespace-package]
"""Update review timestamps of knowledge documents a pull request changes.

A document's `last_checked` becomes the current UTC time when the pull request's
own commits changed its content after they last changed its metadata; adding a
document counts as a content change. Merges from the base branch do not count,
and archived plans keep their timestamps.
"""

import re
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from datetime import UTC, datetime
from pathlib import Path

METADATA_BLOCK = re.compile(r"\A<!-- knowledge\n(.*?)\n-->(?:\n|$)", re.DOTALL)
LAST_CHECKED_LINE = re.compile(r"^last_checked:.*$", re.MULTILINE)
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


def find_documents_to_update(base: str) -> list[str]:
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
    return [
        path
        for path in changed_paths
        if path.endswith(".md")
        and not path.startswith(ARCHIVE_DIRECTORY)
        and LAST_CHECKED_LINE.search(
            split_metadata(Path(path).read_text(encoding="utf-8"))[0]
        )
        and needs_new_timestamp(path, merge_base)
    ]


def update_last_checked(path: Path, timestamp: str) -> None:
    metadata, content = split_metadata(path.read_text(encoding="utf-8"))
    metadata = LAST_CHECKED_LINE.sub(f'last_checked: "{timestamp}"', metadata, count=1)
    path.write_text(metadata + content, encoding="utf-8")


if __name__ == "__main__":
    timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for path in find_documents_to_update(sys.argv[1]):
        update_last_checked(Path(path), timestamp)
