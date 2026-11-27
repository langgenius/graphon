import os
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def run_import_linter(source_directory: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [
            sys.executable,
            "-c",
            "from importlinter.cli import lint_imports_command; lint_imports_command()",
            "--config",
            str(REPOSITORY_ROOT / "pyproject.toml"),
            "--no-cache",
            "--no-logo",
        ],
        cwd=source_directory.parent,
        env={**os.environ, "PYTHONPATH": str(source_directory)},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def source_directory(tmp_path: Path) -> Path:
    source_directory = tmp_path / "src"
    shutil.copytree(
        REPOSITORY_ROOT / "src",
        source_directory,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    result = run_import_linter(source_directory)
    assert result.returncode == 0, result.stdout + result.stderr
    return source_directory


@pytest.mark.parametrize(
    ("importer", "imported"),
    [
        ("file", "runtime"),
        ("runtime", "engine"),
        ("graph", "graph_builder"),
        ("dsl", "graph_builder"),
        ("graph_builder", "dsl"),
    ],
)
def test_layers_reject_disallowed_type_checking_imports(
    source_directory: Path, importer: str, imported: str
) -> None:
    module_path = source_directory / "graphon" / importer / "disallowed_import.py"
    module_path.write_text(
        f"from typing import TYPE_CHECKING\n"
        f"if TYPE_CHECKING:\n    import graphon.{imported}\n"
    )

    result = run_import_linter(source_directory)

    assert result.returncode == 1, result.stdout + result.stderr
    assert f"graphon.{importer}.disallowed_import" in result.stdout
    assert f"graphon.{imported}" in result.stdout


def test_new_top_level_modules_require_a_layer(source_directory: Path) -> None:
    (source_directory / "graphon" / "unclassified_module.py").write_text("")

    result = run_import_linter(source_directory)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "unclassified_module" in result.stdout
