from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess  # noqa: S404
import sys
import tempfile
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from graphon.model_runtime.entities.model_entities import AIModelEntity
from graphon.model_runtime.utils.encoders import jsonable_encoder

logger = logging.getLogger(__name__)

_SLIM_BINARY_NAME = "dify-plugin-daemon-slim"
_SLIM_BINARY_PATH_ENV = "SLIM_BINARY_PATH"


class SlimClientError(RuntimeError):
    """Raised when the DSL slim client cannot run or parse Slim output."""


@dataclass(frozen=True, slots=True)
class SlimMessageEvent:
    stage: str
    message: str
    data: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SlimChunkEvent:
    data: Any


@dataclass(frozen=True, slots=True)
class SlimDoneEvent:
    pass


type SlimEvent = SlimMessageEvent | SlimChunkEvent | SlimDoneEvent


@dataclass(slots=True, frozen=True)
class SlimClientConfig:
    folder: Path
    python_path: str = "python3"
    uv_path: str = ""
    python_env_init_timeout: int = 120
    max_execution_timeout: int = 600
    pip_mirror_url: str = ""
    pip_extra_args: str = ""
    marketplace_url: str = "https://marketplace.dify.ai"

    def __post_init__(self) -> None:
        python_path = self.python_path
        if python_path == "python3":
            python_path = sys.executable

        object.__setattr__(self, "folder", self.folder.expanduser().resolve())
        object.__setattr__(self, "python_path", python_path)
        object.__setattr__(self, "uv_path", self.uv_path or (shutil.which("uv") or ""))

    def build_env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["SLIM_MODE"] = "local"
        env["SLIM_FOLDER"] = str(self.folder)
        env["SLIM_PYTHON_PATH"] = self.python_path
        env["SLIM_PYTHON_ENV_INIT_TIMEOUT"] = str(self.python_env_init_timeout)
        env["SLIM_MAX_EXECUTION_TIMEOUT"] = str(self.max_execution_timeout)
        env["SLIM_MARKETPLACE_URL"] = self.marketplace_url
        if self.uv_path:
            env["SLIM_UV_PATH"] = self.uv_path
        if self.pip_mirror_url:
            env["SLIM_PIP_MIRROR_URL"] = self.pip_mirror_url
        if self.pip_extra_args:
            env["SLIM_PIP_EXTRA_ARGS"] = self.pip_extra_args

        return env


@dataclass(slots=True)
class SlimClient:
    config: SlimClientConfig
    binary_path: str | None = None
    _binary_path: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._binary_path = self.binary_path or resolve_slim_binary_path()

    def cached_plugin_root(self, plugin_id: str) -> Path | None:
        return cached_slim_plugin_root(config=self.config, plugin_id=plugin_id)

    def invoke_events(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[SlimEvent, None, None]:
        with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr_file:
            process = subprocess.Popen(  # noqa: S603
                [
                    self._binary_path,
                    "-id",
                    plugin_id,
                    "-action",
                    action,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                text=True,
                encoding="utf-8",
                env=self.config.build_env(),
            )
            request_payload = {"data": jsonable_encoder(dict(data))}
            if process.stdin is None:
                msg = "Slim subprocess did not expose stdin."
                raise SlimClientError(msg)
            process.stdin.write(json.dumps(request_payload))
            process.stdin.close()

            if process.stdout is None:
                msg = "Slim subprocess did not expose stdout."
                raise SlimClientError(msg)

            pending_error: Exception | None = None
            try:
                yield from _iter_slim_events(process.stdout)
            except Exception as error:
                pending_error = error
                raise
            finally:
                try:
                    _check_slim_process_exit(process=process, stderr_file=stderr_file)
                except SlimClientError:
                    if pending_error is None:
                        raise

    def invoke_chunks(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[Any, None, None]:
        for event in self.invoke_events(plugin_id=plugin_id, action=action, data=data):
            match event:
                case SlimMessageEvent():
                    logger.debug(
                        "slim[%s] %s: %s",
                        action,
                        event.stage,
                        event.message,
                    )
                case SlimChunkEvent():
                    yield event.data
                case SlimDoneEvent():
                    return

    def invoke_unary(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        chunks = list(
            self.invoke_chunks(plugin_id=plugin_id, action=action, data=data),
        )
        if not chunks:
            return {}
        payload = chunks[-1]
        if not isinstance(payload, Mapping):
            msg = f"Expected dict payload for Slim action {action}, got {type(payload)}"
            raise SlimClientError(msg)
        return payload

    def get_ai_model_schema(
        self,
        *,
        plugin_id: str,
        provider: str,
        model_type: str,
        model: str,
        credentials: Mapping[str, Any],
    ) -> AIModelEntity | None:
        result = self.invoke_unary(
            plugin_id=plugin_id,
            action="get_ai_model_schemas",
            data={
                "provider": provider,
                "model_type": model_type,
                "model": model,
                "credentials": dict(credentials),
            },
        )
        raw_schema = result.get("model_schema")
        if raw_schema is None:
            return None
        if not isinstance(raw_schema, Mapping):
            msg = f"Unexpected model schema payload: {raw_schema!r}"
            raise SlimClientError(msg)
        return AIModelEntity.model_validate(raw_schema)

    def extract(
        self,
        *,
        plugin_id: str | None = None,
        path: str | Path | None = None,
        action: str | None = None,
    ) -> Mapping[str, Any]:
        args = [self._binary_path, "extract", "-output", "json"]
        if plugin_id is not None:
            args.extend(("-id", plugin_id))
        if path is not None:
            args.extend(("-path", str(path)))
        if action is not None:
            args.extend(("-action", action))

        process = subprocess.run(  # noqa: S603
            args,
            text=True,
            encoding="utf-8",
            capture_output=True,
            env=self.config.build_env(),
            check=False,
        )
        if process.returncode != 0:
            raise SlimClientError(
                _slim_error_message(process.stderr, process.returncode)
            )

        try:
            payload = json.loads(process.stdout)
        except json.JSONDecodeError as error:
            msg = f"Slim extract returned invalid JSON: {error}"
            raise SlimClientError(msg) from error
        if not isinstance(payload, Mapping):
            msg = f"Slim extract returned {type(payload).__name__}, expected object."
            raise SlimClientError(msg)
        return payload


def resolve_slim_binary_path() -> str:
    configured_path = os.environ.get(_SLIM_BINARY_PATH_ENV, "").strip()
    if configured_path:
        binary_path = Path(configured_path).expanduser().resolve()
        if not binary_path.is_file():
            msg = f"{_SLIM_BINARY_PATH_ENV} points to a missing file: {binary_path}"
            raise SlimClientError(msg)
        if not os.access(binary_path, os.X_OK):
            msg = (
                f"{_SLIM_BINARY_PATH_ENV} points to a non-executable file: "
                f"{binary_path}"
            )
            raise SlimClientError(msg)
        return str(binary_path)

    binary_path = shutil.which(_SLIM_BINARY_NAME)
    if binary_path is None:
        msg = (
            f"{_SLIM_BINARY_NAME} is not available in PATH. "
            f"Set {_SLIM_BINARY_PATH_ENV} to override it."
        )
        raise SlimClientError(msg)
    return binary_path


def slim_plugin_cache_path(*, folder: Path, plugin_id: str) -> Path:
    return folder / plugin_id.replace(":", "-")


def cached_slim_plugin_root(
    *,
    config: SlimClientConfig,
    plugin_id: str,
) -> Path | None:
    candidate = slim_plugin_cache_path(folder=config.folder, plugin_id=plugin_id)
    return candidate if candidate.exists() else None


def _iter_slim_events(stdout: Iterable[str]) -> Generator[SlimEvent, None, None]:
    for line in stdout:
        if not line.strip():
            continue
        event = _parse_slim_event(line)
        yield event
        if isinstance(event, SlimDoneEvent):
            return


def _parse_slim_event(line: str) -> SlimEvent:
    try:
        event = json.loads(line)
    except json.JSONDecodeError as error:
        msg = f"Slim emitted invalid JSON event: {line.strip()}"
        raise SlimClientError(msg) from error
    if not isinstance(event, Mapping):
        msg = f"Slim emitted {type(event).__name__}, expected event object."
        raise SlimClientError(msg)

    event_type = event.get("event")
    match event_type:
        case "message":
            return _parse_message_event(event)
        case "chunk":
            return SlimChunkEvent(data=event.get("data"))
        case "done":
            return SlimDoneEvent()
        case "error":
            error = event.get("data") or {}
            if isinstance(error, Mapping):
                message = str(error.get("message") or "Slim error.")
            else:
                message = str(error or "Slim error.")
            raise SlimClientError(message)
        case _:
            msg = f"Unknown Slim event type: {event_type}"
            raise SlimClientError(msg)


def _parse_message_event(event: Mapping[str, Any]) -> SlimMessageEvent:
    message = event.get("data") or {}
    if not isinstance(message, Mapping):
        msg = f"Unexpected Slim message payload: {message!r}"
        raise SlimClientError(msg)
    return SlimMessageEvent(
        stage=str(message.get("stage") or ""),
        message=str(message.get("message") or ""),
        data=message,
    )


def _check_slim_process_exit(
    *,
    process: subprocess.Popen[str],
    stderr_file: IO[str],
) -> None:
    return_code = process.wait()
    stderr_file.seek(0)
    stderr_text = stderr_file.read().strip()
    if return_code == 0:
        return
    raise SlimClientError(_slim_error_message(stderr_text, return_code))


def _slim_error_message(stderr_text: str, return_code: int) -> str:
    if not stderr_text:
        return f"Slim process exited with code {return_code}"
    try:
        stderr_payload = json.loads(stderr_text.splitlines()[-1])
    except json.JSONDecodeError:
        return f"Slim process exited with code {return_code}: {stderr_text}"
    if isinstance(stderr_payload, Mapping):
        return str(
            stderr_payload.get("message")
            or f"Slim process exited with code {return_code}",
        )
    return f"Slim process exited with code {return_code}: {stderr_text}"
