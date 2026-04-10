from __future__ import annotations

import os
from io import StringIO
from pathlib import Path

import pytest

from examples.openai_slim_minimal.workflow import write_stream_chunk
from examples.openai_slim_support import ALLOWED_ENV_VARS, load_env_file
from graphon.enums import BuiltinNodeTypes
from graphon.graph_events.node import NodeRunStreamChunkEvent


def test_load_env_file_sets_missing_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
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
            (tmp_path / ".." / "bin" / "dify-plugin-daemon-slim").resolve(),
        )
        assert os.environ["SLIM_PROVIDER"] == "openai"


def test_load_env_file_does_not_override_existing_values(
    monkeypatch: pytest.MonkeyPatch,
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


@pytest.mark.parametrize(
    "example_dir_name",
    [
        "openai_slim_minimal",
        "openai_slim_parallel_translation",
    ],
)
def test_env_example_matches_allowed_env_vars(example_dir_name: str) -> None:
    env_example = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / example_dir_name
        / ".env.example"
    )
    keys = {
        line.split("=", 1)[0].removeprefix("export ").strip()
        for line in env_example.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert keys == set(ALLOWED_ENV_VARS)


@pytest.mark.parametrize(
    ("example_dir_name", "expected_binary_path", "expected_plugin_folder"),
    [
        (
            "openai_slim_minimal",
            "~/.local/bin/dify-plugin-daemon-slim",
            "~/.local/share/graphon/slim/plugins",
        ),
        (
            "openai_slim_parallel_translation",
            "~/.local/bin/dify-plugin-daemon-slim",
            "~/.local/share/graphon/slim/plugins",
        ),
    ],
)
def test_env_example_sets_recommended_unix_defaults(
    example_dir_name: str,
    expected_binary_path: str,
    expected_plugin_folder: str,
) -> None:
    env_example = (
        Path(__file__).resolve().parents[2]
        / "examples"
        / example_dir_name
        / ".env.example"
    )
    values = {
        key.strip(): value.strip()
        for raw_line in env_example.read_text(encoding="utf-8").splitlines()
        if (line := raw_line.strip()) and not line.startswith("#") and "=" in line
        for key, value in [line.removeprefix("export ").split("=", 1)]
    }

    assert values["SLIM_BINARY_PATH"] == expected_binary_path
    assert values["SLIM_PLUGIN_FOLDER"] == expected_plugin_folder


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
    assert not output.getvalue()
