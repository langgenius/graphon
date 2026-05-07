from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from examples.slim_llm import settings


def test_load_credentials_adds_default_slim_settings(tmp_path: Path) -> None:
    credentials_file = tmp_path / "credentials.json"
    credentials_file.write_text(
        json.dumps(
            {
                "model_credentials": [
                    {
                        "vendor": "openai",
                        "values": {"openai_api_key": "key"},
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    credentials = settings.load_credentials(credentials_file)

    assert credentials["model_credentials"]
    assert credentials["slim"] == {
        "mode": "local",
        "plugin_folder": str((tmp_path / ".slim/plugins").resolve()),
    }


def test_load_credentials_keeps_remote_settings(tmp_path: Path) -> None:
    credentials_file = tmp_path / "credentials.json"
    credentials_file.write_text(
        json.dumps(
            {
                "slim": {
                    "mode": "remote",
                    "plugin_folder": "cache/plugins",
                    "daemon_addr": "http://daemon.test",
                    "daemon_key": "secret",
                },
            },
        ),
        encoding="utf-8",
    )

    credentials = settings.load_credentials(credentials_file)

    assert credentials["slim"] == {
        "mode": "remote",
        "plugin_folder": str((tmp_path / "cache/plugins").resolve()),
        "daemon_addr": "http://daemon.test",
        "daemon_key": "secret",
    }


def test_slim_client_config_uses_slim_settings(tmp_path: Path) -> None:
    config = settings.slim_client_config(
        {
            "slim": {
                "mode": "remote",
                "plugin_folder": str(tmp_path / "plugins"),
                "daemon_addr": "http://daemon.test",
                "daemon_key": "secret",
            },
        },
    )

    assert config.folder == (tmp_path / "plugins").resolve()
    assert config.mode == "remote"
    assert config.daemon_addr == "http://daemon.test"
    assert config.daemon_key == "secret"


def test_openai_credentials_reads_vendor_values() -> None:
    assert settings.openai_credentials(
        {
            "model_credentials": [
                {
                    "vendor": "openai",
                    "values": {"openai_api_key": "key"},
                },
            ],
        },
    ) == {"openai_api_key": "key"}


def test_use_local_slim_binary_keeps_existing_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as context:
        context.setenv("SLIM_BINARY_PATH", "/custom/slim")

        settings.use_local_slim_binary()

        assert os.environ["SLIM_BINARY_PATH"] == "/custom/slim"


def test_use_local_slim_binary_uses_example_binary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    slim_binary = tmp_path / "slim"
    slim_binary.write_text("#!/bin/sh\n", encoding="utf-8")

    with monkeypatch.context() as context:
        context.delenv("SLIM_BINARY_PATH", raising=False)
        context.setattr(settings, "LOCAL_SLIM_BINARY", slim_binary)

        settings.use_local_slim_binary()

        assert os.environ["SLIM_BINARY_PATH"] == str(slim_binary)
