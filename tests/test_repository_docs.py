"""Check active knowledge links, discoverability, and recorded review age.

Historical documents under docs/archive are excluded.

Inline local links and ATX heading fragments (including duplicate headings) are
checked. Fenced code and external URLs are ignored. Reference links, HTML anchors,
Setext headings, link titles, and paths containing spaces or parentheses are
outside this check. Prose accuracy still requires human review.
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
    ) - set(root.glob("docs/archive/**/*.md"))


def check_document_reviews(root: Path, now: datetime) -> None:
    """Require valid metadata and no review older than seven days."""
    documents = find_repository_documents(root) - {
        root / "CLA.md",
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


def test_legal_and_template_files_do_not_require_reviews(
    tmp_path: Path,
) -> None:
    (tmp_path / "README.md").write_text(
        '<!-- knowledge\nlast_checked: "2026-09-09T00:00:00Z"\n-->\n# Index\n',
        encoding="utf-8",
    )
    for path in ("CLA.md", ".github/pull_request_template.md"):
        document = tmp_path / path
        document.parent.mkdir(parents=True, exist_ok=True)
        document.write_text("# Historical or template content\n", encoding="utf-8")
    check_document_reviews(tmp_path, datetime(2026, 9, 9, tzinfo=UTC))


def read_document_lines(document: Path) -> list[tuple[int, str]]:
    """Return source lines outside fenced code with their original line numbers."""
    lines: list[tuple[int, str]] = []
    fence = ""
    for number, line in enumerate(document.read_text(encoding="utf-8").splitlines(), 1):
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if marker:
            delimiter = marker[1]
            if not fence:
                fence = delimiter
            elif delimiter[0] == fence[0] and len(delimiter) >= len(fence):
                fence = ""
        elif not fence:
            lines.append((number, line))
    return lines


def find_heading_anchors(lines: list[tuple[int, str]]) -> set[str]:
    anchors: set[str] = set()
    for _, line in lines:
        heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)(?:\s+#+)?$", line)
        if heading:
            slug = re.sub(r"[^\w\- ]", "", heading[1].lower()).replace(" ", "-")
            anchor = slug
            suffix = 0
            while anchor in anchors:
                suffix += 1
                anchor = f"{slug}-{suffix}"
            anchors.add(anchor)
    return anchors


def check_document_links(root: Path) -> None:
    documents = find_repository_documents(root)
    knowledge = {
        document for document in documents if document.is_relative_to(root / "docs")
    } | {root / "ARCHITECTURE.md"}
    links: dict[Path, set[Path]] = {document: set() for document in documents}

    lines_by_document = {
        document: read_document_lines(document) for document in documents
    }
    anchors_by_document = {
        document: find_heading_anchors(lines)
        for document, lines in lines_by_document.items()
    }

    for document, lines in lines_by_document.items():
        for number, line in lines:
            for destination in re.findall(r"\[[^\]\n]+\]\(([^()\s]+)\)", line):
                url = urlsplit(destination)
                if url.scheme or url.netloc:
                    continue
                target = (
                    (document.parent / unquote(url.path)).resolve()
                    if url.path
                    else document
                )
                assert target.exists(), (
                    f"Broken link: {document.relative_to(root)}:{number}: {destination}"
                )
                if url.fragment and target in anchors_by_document:
                    assert unquote(url.fragment) in anchors_by_document[target], (
                        f"Broken heading: {document.relative_to(root)}:{number}: "
                        f"{destination}"
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


def test_repository_documentation_links() -> None:
    check_document_links(Path(__file__).resolve().parents[1])


@pytest.mark.parametrize("archived", [False, True])
def test_archived_plans_are_excluded_from_knowledge_checks(
    tmp_path: Path, archived: bool
) -> None:
    metadata = '<!-- knowledge\nlast_checked: "2026-09-09T00:00:00Z"\n-->\n'
    (tmp_path / "AGENTS.md").write_text(
        metadata + "# Index\n[Architecture](ARCHITECTURE.md)\n", encoding="utf-8"
    )
    (tmp_path / "ARCHITECTURE.md").write_text(
        metadata + "# Architecture\n", encoding="utf-8"
    )
    directory = tmp_path / ("docs/archive/plans" if archived else "docs/plans")
    directory.mkdir(parents=True)
    (directory / "completed.md").write_text(
        "# Completed plan\n[Old source](missing.py)\n", encoding="utf-8"
    )
    now = datetime(2026, 9, 9, tzinfo=UTC)

    if archived:
        check_document_reviews(tmp_path, now)
        check_document_links(tmp_path)
    else:
        with pytest.raises(
            AssertionError, match=r"docs/plans/completed.md.*last_checked"
        ):
            check_document_reviews(tmp_path, now)
        with pytest.raises(
            AssertionError, match=r"Broken link: docs/plans/completed.md"
        ):
            check_document_links(tmp_path)


@pytest.mark.parametrize("destination", ["#missing", "guide.md#missing"])
def test_document_links_reject_missing_headings(
    tmp_path: Path, destination: str
) -> None:
    (tmp_path / "AGENTS.md").write_text(
        f"# Index\n[Architecture](ARCHITECTURE.md)\n[Guide]({destination})\n"
    )
    (tmp_path / "ARCHITECTURE.md").write_text("# Architecture\n")
    (tmp_path / "guide.md").write_text("# Guide\n```markdown\n# Missing\n```\n")
    with pytest.raises(AssertionError, match=r"Broken heading: AGENTS.md"):
        check_document_links(tmp_path)


def test_document_links_accept_heading_fragments(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_text(
        "# Index\n[Self](#index)\n[Architecture](ARCHITECTURE.md#state--ownership)\n"
        "[Repeated](ARCHITECTURE.md#state--ownership-1)\n"
        "[Unicode](ARCHITECTURE.md#%E7%8A%B6%E6%80%81)\n"
        "[Archived](docs/archive/plans/old.md#obsolete)\n"
    )
    (tmp_path / "ARCHITECTURE.md").write_text(
        "# Architecture\n## State & `Ownership`\n## State & `Ownership`\n## 状态\n"
    )
    archived = tmp_path / "docs/archive/plans/old.md"
    archived.parent.mkdir(parents=True)
    archived.write_text("Historical content\n")
    check_document_links(tmp_path)
