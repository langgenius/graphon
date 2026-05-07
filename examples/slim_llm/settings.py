from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from graphon.dsl.slim import SlimClientConfig

HERE = Path(__file__).resolve().parent
GRAPH_FILE = HERE / "graph.yml"
CREDENTIALS_FILE = HERE / "credentials.json"
CREDENTIALS_EXAMPLE_FILE = HERE / "credentials.example.json"
LOCAL_SLIM_BINARY = HERE / "slim"

OPENAI_PLUGIN_ID = (
    "langgenius/openai:0.3.8@"
    "592c8252795b5f75807de2d609a03196ed02596b409f7642b4a07548c7ff57ef"
)
OPENAI_PROVIDER = "openai"
OPENAI_MODEL = "gpt-5.4"
DEFAULT_QUERY = "Reply with only the word Graphon."


def load_credentials(path: Path = CREDENTIALS_FILE) -> dict[str, Any]:
    if not path.is_file():
        msg = (
            f"Missing {path.name}. Copy {CREDENTIALS_EXAMPLE_FILE.name} "
            f"to {path.name} and fill it in."
        )
        raise FileNotFoundError(msg)

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"{path.name} must contain a JSON object."
        raise TypeError(msg)

    credentials = dict(raw)
    credentials["slim"] = slim_settings(credentials.get("slim"), base_dir=path.parent)
    return credentials


def slim_settings(raw_settings: object, *, base_dir: Path) -> dict[str, Any]:
    if raw_settings is None:
        settings: dict[str, Any] = {}
    elif isinstance(raw_settings, dict):
        settings = dict(raw_settings)
    else:
        msg = "credentials.json field 'slim' must be an object."
        raise TypeError(msg)

    settings.setdefault("mode", "local")
    settings.setdefault("plugin_folder", ".slim/plugins")
    for key in ("plugin_folder", "plugin_root"):
        if key in settings:
            settings[key] = resolve_path(settings[key], base_dir=base_dir)
    return settings


def resolve_path(value: object, *, base_dir: Path) -> object:
    if not isinstance(value, str) or not value:
        return value

    path = Path(value).expanduser()
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


def use_local_slim_binary() -> None:
    if os.environ.get("SLIM_BINARY_PATH"):
        return
    if LOCAL_SLIM_BINARY.is_file():
        os.environ["SLIM_BINARY_PATH"] = str(LOCAL_SLIM_BINARY)


def slim_client_config(credentials: dict[str, Any]) -> SlimClientConfig:
    settings = credentials["slim"]
    if not isinstance(settings, dict):
        msg = "credentials['slim'] must be an object."
        raise TypeError(msg)

    return SlimClientConfig(
        folder=Path(str(settings["plugin_folder"])),
        mode=str(settings.get("mode") or "local"),
        daemon_addr=str(settings.get("daemon_addr") or ""),
        daemon_key=str(settings.get("daemon_key") or ""),
    )


def openai_credentials(credentials: dict[str, Any]) -> dict[str, Any]:
    for item in credentials.get("model_credentials", []):
        if not isinstance(item, dict) or item.get("vendor") != OPENAI_PROVIDER:
            continue
        values = item.get("values")
        if isinstance(values, dict):
            return dict(values)

    msg = "credentials.json must include OpenAI model credentials."
    raise ValueError(msg)
