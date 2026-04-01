from __future__ import annotations

import os
from io import StringIO
from pathlib import Path

import pytest

from examples.graphon_openai_slim.workflow import (
    ALLOWED_ENV_VARS,
    load_env_file,
    write_stream_chunk,
)
from graphon.enums import BuiltinNodeTypes
from graphon.graph_events.node import NodeRunStreamChunkEvent


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


def test_write_stream_chunk_writes_llm_text_chunks() -> None:
    output = StringIO()
    event = NodeRunStreamChunkEvent(
        id="event-1",
        node_id="llm",
        node_type=BuiltinNodeTypes.LLM,
        selector=["llm", "text"],
        chunk="hello",
        is_final=False,
    )

    assert write_stream_chunk(event, stream_output=output) is True
    assert output.getvalue() == "hello"


def test_write_stream_chunk_ignores_non_llm_text_chunks() -> None:
    output = StringIO()
    event = NodeRunStreamChunkEvent(
        id="event-2",
        node_id="output",
        node_type=BuiltinNodeTypes.ANSWER,
        selector=["output", "answer"],
        chunk="hello",
        is_final=False,
    )

    assert write_stream_chunk(event, stream_output=output) is False
    assert output.getvalue() == ""
