"""Check knowledge links, discoverability, and recorded review age.

Fenced code, external URLs, and fragments are ignored. Reference links, HTML,
link titles, and paths containing spaces or parentheses are outside this check;
it does not verify heading anchors or the semantic freshness of documentation.
"""

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest
import yaml


def find_repository_documents(root: Path) -> set[Path]:
    return (
        set(root.glob("*.md"))
        | set(root.glob("docs/**/*.md"))
        | set(root.glob("src/**/*.md"))
        | set(root.glob("examples/**/*.md"))
    )


def check_document_reviews(root: Path, now: datetime) -> None:
    """Require valid metadata and no review older than seven days."""
    documents = find_repository_documents(root) - {
        root / "CLA.md",
        root / "CHANGELOG.md",
    }
    reviews: list[tuple[datetime, Path]] = []
    for document in sorted(documents):
        path = document.relative_to(root)
        header = re.match(
            r"\A<!-- knowledge\n(.*?)\n-->(?:\n|$)",
            document.read_text(encoding="utf-8"),
            re.DOTALL,
        )
        assert header, f"{path}: missing leading knowledge metadata with last_checked"
        error = (
            f"{path}: last_checked must be a UTC timestamp string: "
            '"YYYY-MM-DDTHH:MM:SSZ"'
        )
        try:
            metadata = yaml.safe_load(header[1])
        except yaml.YAMLError as exc:
            raise AssertionError(error) from exc
        assert isinstance(metadata, dict), error
        timestamp = metadata.get("last_checked")
        assert isinstance(timestamp, str), error
        assert re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", timestamp
        ), error
        try:
            checked_at = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise AssertionError(error) from exc
        assert checked_at <= now, f"{path}: last_checked {timestamp} is in the future"
        reviews.append((checked_at, path))

    assert reviews, "No knowledge documents found"
    checked_at, oldest_document = min(reviews)
    assert now - checked_at <= timedelta(days=7), (
        f"{oldest_document}: last_checked {checked_at.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        "is more than seven days old; review the document and update its metadata"
    )


def test_repository_documentation_reviews() -> None:
    check_document_reviews(Path(__file__).resolve().parents[1], datetime.now(UTC))


@pytest.mark.parametrize(
    "age",
    [timedelta(0), timedelta(days=7), timedelta(days=7, seconds=1), timedelta(days=8)],
)
def test_document_review_deadline(tmp_path: Path, age: timedelta) -> None:
    now = datetime(2026, 9, 9, 12, tzinfo=UTC)
    (tmp_path / "README.md").write_text(
        '<!-- knowledge\nlast_checked: "2026-09-09T12:00:00Z"\n-->\n# Index\n',
        encoding="utf-8",
    )
    oldest_document = tmp_path / "docs" / "guide.md"
    oldest_document.parent.mkdir()
    timestamp = (now - age).strftime("%Y-%m-%dT%H:%M:%SZ")
    oldest_document.write_text(
        f'<!-- knowledge\nlast_checked: "{timestamp}"\n-->\n# Guide\n',
        encoding="utf-8",
    )
    if age == timedelta(days=8):
        (oldest_document.parent / "a-newer.md").write_text(
            '<!-- knowledge\nlast_checked: "2026-09-02T11:59:59Z"\n-->\n# Newer\n',
            encoding="utf-8",
        )

    if age > timedelta(days=7):
        with pytest.raises(
            AssertionError, match=rf"docs/guide.md.*{re.escape(timestamp)}"
        ):
            check_document_reviews(tmp_path, now)
    else:
        check_document_reviews(tmp_path, now)


@pytest.mark.parametrize(
    "path",
    [
        "GUIDE.md",
        "docs/nested/guide.md",
        "docs/plans/done.md",
        "docs/CHANGELOG.md",
        "src/component/guide.md",
        "examples/demo/README.md",
        "examples/demo/guide.md",
    ],
)
def test_every_knowledge_document_requires_metadata(tmp_path: Path, path: str) -> None:
    document = tmp_path / path
    document.parent.mkdir(parents=True, exist_ok=True)
    document.write_text("# Guide\n", encoding="utf-8")
    with pytest.raises(AssertionError, match=re.escape(path)):
        check_document_reviews(tmp_path, datetime(2026, 9, 9, tzinfo=UTC))


@pytest.mark.parametrize(
    "metadata",
    [
        "last_checked: [",
        "[]",
        "title: Guide",
        "last_checked: null",
        "last_checked: 2026-09-08T00:00:00Z",
        'last_checked: "2026-09-08"',
        'last_checked: "2026-09-08T00:00:00"',
        'last_checked: "2026-09-08T08:00:00+08:00"',
        'last_checked: "2026-02-30T00:00:00Z"',
        'last_checked: "2026-09-09T00:00:01Z"',
    ],
)
def test_invalid_document_review_metadata(tmp_path: Path, metadata: str) -> None:
    (tmp_path / "README.md").write_text(
        f"<!-- knowledge\n{metadata}\n-->\n# Guide\n", encoding="utf-8"
    )
    with pytest.raises(AssertionError, match=r"README.md.*last_checked"):
        check_document_reviews(tmp_path, datetime(2026, 9, 9, tzinfo=UTC))


def test_review_metadata_must_start_document(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text(
        '# Guide\n<!-- knowledge\nlast_checked: "2026-09-09T00:00:00Z"\n-->\n',
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match=r"README.md"):
        check_document_reviews(tmp_path, datetime(2026, 9, 9, tzinfo=UTC))


def test_legal_release_and_template_files_do_not_require_reviews(
    tmp_path: Path,
) -> None:
    (tmp_path / "README.md").write_text(
        '<!-- knowledge\nlast_checked: "2026-09-09T00:00:00Z"\n-->\n# Index\n',
        encoding="utf-8",
    )
    for path in ("CLA.md", "CHANGELOG.md", ".github/pull_request_template.md"):
        document = tmp_path / path
        document.parent.mkdir(parents=True, exist_ok=True)
        document.write_text("# Historical or template content\n", encoding="utf-8")
    check_document_reviews(tmp_path, datetime(2026, 9, 9, tzinfo=UTC))


def test_repository_documentation_links() -> None:
    root = Path(__file__).resolve().parents[1]
    knowledge = set(root.glob("docs/**/*.md")) | {root / "ARCHITECTURE.md"}
    documents = find_repository_documents(root)
    links: dict[Path, set[Path]] = {document: set() for document in documents}

    for document in sorted(documents):
        fence = ""
        lines = document.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines, 1):
            marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
            if marker:
                delimiter = marker[1]
                if not fence:
                    fence = delimiter
                elif delimiter[0] == fence[0] and len(delimiter) >= len(fence):
                    fence = ""
                continue
            if fence:
                continue
            for destination in re.findall(r"\[[^\]\n]+\]\(([^()\s]+)\)", line):
                url = urlsplit(destination)
                if url.scheme or url.netloc or not url.path:
                    continue
                target = (document.parent / unquote(url.path)).resolve()
                assert target.exists(), (
                    f"Broken link: {document.relative_to(root)}:{number}: {destination}"
                )
                links[document].add(target)

    reached: set[Path] = set()
    pending = {root / "AGENTS.md"}
    while pending:
        document = pending.pop()
        reached.add(document)
        pending.update(links.get(document, set()) - reached)

    orphans = sorted(str(path.relative_to(root)) for path in knowledge - reached)
    assert not orphans, "Link from AGENTS.md or a reachable document:\n" + "\n".join(
        orphans
    )
