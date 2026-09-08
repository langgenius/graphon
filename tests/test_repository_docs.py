"""Check simple inline [label](relative/path) links and knowledge discoverability.

Fenced code, external URLs, and fragments are ignored. Reference links, HTML,
link titles, and paths containing spaces or parentheses are outside this check;
it does not verify heading anchors or the semantic freshness of documentation.
"""

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit


def test_repository_documentation_links() -> None:
    root = Path(__file__).resolve().parents[1]
    knowledge = set(root.glob("docs/**/*.md")) | {root / "ARCHITECTURE.md"}
    documents = set(root.glob("*.md")) | set(root.glob("docs/**/*.md"))
    documents |= set(root.glob("src/**/README.md"))
    documents |= set(root.glob("examples/**/README.md"))
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
