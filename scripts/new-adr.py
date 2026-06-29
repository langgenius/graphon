#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

ADR_DIR = Path("docs/adr")
ADR_README = ADR_DIR / "README.md"
ADR_PATTERN = re.compile(r"^(?P<id>\d{4,})-(?P<slug>[a-z0-9-]+)\.md$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a new ADR scaffold.")
    parser.add_argument("--id", type=int, help="Explicit ADR id to use.")
    parser.add_argument("--title", required=True, help="ADR title.")
    return parser.parse_args()


def slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    if not slug:
        msg = "title must contain at least one alphanumeric character"
        raise ValueError(msg)
    return slug


def next_adr_id(adr_dir: Path) -> int:
    max_id = 0
    for path in adr_dir.iterdir():
        match = ADR_PATTERN.match(path.name)
        if match is None:
            continue
        max_id = max(max_id, int(match.group("id")))
    return max_id + 1


def format_adr_id(raw_id: int) -> str:
    return f"{raw_id:04d}"


def render_adr(title: str, adr_id: str) -> str:
    return f"""# ADR {adr_id}: {title}

- Status: Proposed
- Date: {datetime.now(tz=UTC).date().isoformat()}
- Related PRs: N/A
- Supersedes: N/A
- Superseded by: N/A

## Context

Describe the pressure, problem, or ambiguity that forced this decision.

## Decision

Describe the decision in concrete terms. Prefer stable boundaries and contracts
over implementation trivia.

## Consequences

List the direct consequences of the decision, including migration cost,
limitations, and follow-up work.

## Alternatives Considered

Record the reasonable alternatives that were considered and rejected, and why
they were rejected. Do not try to enumerate every possible option.

## Rollout Notes

Record migration steps, compatibility notes, or deferred work if they matter to
future maintainers.
"""


def update_readme(readme_path: Path, filename: str, title: str) -> None:
    readme = readme_path.read_text(encoding="utf-8")
    marker = "## Historical Backfill"
    if marker not in readme:
        msg = f"could not find '{marker}' in {readme_path}"
        raise ValueError(msg)

    entry = f"- [{filename}]({filename}): {title}\n\n"
    if entry.strip() in readme:
        return

    readme = readme.replace(marker, entry + marker, 1)
    readme_path.write_text(readme, encoding="utf-8")


def main() -> int:
    args = parse_args()
    if not ADR_DIR.is_dir():
        msg = f"ADR directory not found: {ADR_DIR}"
        raise SystemExit(msg)
    if not ADR_README.is_file():
        msg = f"ADR README not found: {ADR_README}"
        raise SystemExit(msg)

    slug = slugify(args.title)
    raw_id = args.id if args.id is not None else next_adr_id(ADR_DIR)
    adr_id = format_adr_id(raw_id)
    filename = f"{adr_id}-{slug}.md"
    output_path = ADR_DIR / filename

    if output_path.exists():
        msg = f"ADR already exists: {output_path}"
        raise SystemExit(msg)

    output_path.write_text(render_adr(args.title, adr_id), encoding="utf-8")
    update_readme(ADR_README, filename, args.title)
    sys.stdout.write(f"{output_path}\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        sys.stderr.write(f"{exc}\n")
        raise SystemExit(1) from exc
