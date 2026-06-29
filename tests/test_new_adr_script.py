from __future__ import annotations

import subprocess  # noqa: S404
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "new-adr.py"


def write_adr_readme(tmp_path: Path) -> None:
    adr_dir = tmp_path / "docs" / "adr"
    adr_dir.mkdir(parents=True)
    (adr_dir / "README.md").write_text(
        """# Architecture Decision Records

## Current ADRs

- [0001-existing.md](0001-existing.md): existing decision

## Historical Backfill

- [backlog.md](backlog.md): grouped historical pull requests
""",
        encoding="utf-8",
    )
    (adr_dir / "0001-existing.md").write_text(
        "# ADR 0001: Existing\n",
        encoding="utf-8",
    )


def run_script(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [sys.executable, str(SCRIPT_PATH), *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def test_requires_title_argument(tmp_path: Path) -> None:
    write_adr_readme(tmp_path)

    result = run_script(tmp_path)

    assert result.returncode != 0
    assert "--title" in result.stderr


def test_assigns_next_id_and_updates_readme(tmp_path: Path) -> None:
    write_adr_readme(tmp_path)

    result = run_script(tmp_path, "--title", "Add polling support")

    assert result.returncode == 0, result.stderr
    new_adr_path = tmp_path / "docs" / "adr" / "0002-add-polling-support.md"
    assert new_adr_path.exists()
    new_adr = new_adr_path.read_text(encoding="utf-8")
    assert "# ADR 0002: Add polling support" in new_adr
    assert (
        "Record the reasonable alternatives that were considered and rejected,"
        in new_adr
    )

    readme = (tmp_path / "docs" / "adr" / "README.md").read_text(encoding="utf-8")
    assert (
        "- [0002-add-polling-support.md](0002-add-polling-support.md): "
        "Add polling support"
    ) in readme


def test_uses_explicit_id_when_provided(tmp_path: Path) -> None:
    write_adr_readme(tmp_path)

    result = run_script(tmp_path, "--id", "1234", "--title", "Extract HITL interface")

    assert result.returncode == 0, result.stderr
    new_adr_path = tmp_path / "docs" / "adr" / "1234-extract-hitl-interface.md"
    assert new_adr_path.exists()
    assert "# ADR 1234: Extract HITL interface" in new_adr_path.read_text(
        encoding="utf-8"
    )
