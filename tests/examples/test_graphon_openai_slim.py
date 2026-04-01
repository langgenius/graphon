from __future__ import annotations

import os
from pathlib import Path

import pytest

from examples.graphon_openai_slim.workflow import ALLOWED_ENV_VARS, load_env_file


def test_load_env_file_sets_missing_values(monkeypatch, tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "OPENAI_API_KEY=secret\n"
        'SLIM_BINARY_PATH="../bin/dify-plugin-daemon-slim"\n'
        "export SLIM_PROVIDER=openai\n",
        encoding="utf-8",
    )

    with monkeypatch.context() as context:
        context.delenv("OPENAI_API_KEY", raising=False)
        context.delenv("SLIM_BINARY_PATH", raising=False)
        context.delenv("SLIM_PROVIDER", raising=False)

        load_env_file(env_file)

        assert env_file.is_file()
        assert os.environ["OPENAI_API_KEY"] == "secret"
        assert os.environ["SLIM_BINARY_PATH"] == str(
            (tmp_path / ".." / "bin" / "dify-plugin-daemon-slim").resolve()
        )
        assert os.environ["SLIM_PROVIDER"] == "openai"


def test_load_env_file_does_not_override_existing_values(
    monkeypatch,
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("OPENAI_API_KEY=from-file\n", encoding="utf-8")
    with monkeypatch.context() as context:
        context.setenv("OPENAI_API_KEY", "from-env")

        load_env_file(env_file)

        assert os.environ["OPENAI_API_KEY"] == "from-env"


def test_load_env_file_rejects_invalid_line(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("NOT_VALID\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"Invalid \.env line 1"):
        load_env_file(env_file)


def test_load_env_file_rejects_unknown_key(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("UNKNOWN_KEY=value\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"Unsupported \.env key 'UNKNOWN_KEY'"):
        load_env_file(env_file)


def test_env_example_matches_allowed_env_vars() -> None:
    env_example = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / "graphon_openai_slim"
        / ".env.example"
    )
    keys = {
        line.split("=", 1)[0].removeprefix("export ").strip()
        for line in env_example.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert keys == set(ALLOWED_ENV_VARS)
