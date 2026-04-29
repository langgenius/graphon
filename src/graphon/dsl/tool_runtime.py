from __future__ import annotations

import logging
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import yaml

from graphon.file.models import File
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.nodes.runtime import ToolNodeRuntimeProtocol
from graphon.nodes.tool.entities import ToolNodeData
from graphon.nodes.tool.exc import ToolNodeError
from graphon.nodes.tool_runtime_entities import (
    ToolRuntimeHandle,
    ToolRuntimeMessage,
    ToolRuntimeParameter,
)
from graphon.runtime.variable_pool import VariablePool

from .entities import DslDependency, DslToolCredential
from .slim.client import (
    SlimClient,
    SlimClientConfig,
    SlimClientError,
    cached_slim_plugin_root,
)

logger = logging.getLogger(__name__)

_PLUGIN_PROVIDER_PARTS = 2
_SLIM_ACTION_INVOKE_TOOL = "invoke_tool"

type SlimActionInvoker = Callable[
    [str, str, Mapping[str, Any]],
    Iterable[Mapping[str, Any]],
]


@dataclass(frozen=True, slots=True)
class _SlimToolBinding:
    plugin_id: str
    provider: str
    provider_id: str
    tool_name: str
    credentials: Mapping[str, Any]
    credential_type: str
    configuration_parameters: Mapping[str, Any]
    runtime_parameters: Sequence[ToolRuntimeParameter]


@dataclass(slots=True)
class SlimToolNodeRuntime(ToolNodeRuntimeProtocol):
    """Slim-backed runtime for simple plugin tool nodes."""

    config: SlimClientConfig
    tool_credential: DslToolCredential
    dependencies: Sequence[DslDependency]
    action_invoker: SlimActionInvoker | None = None
    _client: SlimClient | None = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.action_invoker is None:
            self._client = SlimClient(config=self.config)
        else:
            self._client = None

    def get_runtime(
        self,
        *,
        node_id: str,
        node_data: ToolNodeData,
        variable_pool: VariablePool | None,
        node_execution_id: str | None = None,
    ) -> ToolRuntimeHandle:
        _ = node_id, node_execution_id
        provider_id = node_data.provider_id
        provider = _tool_provider_name(
            provider_id=provider_id,
            provider_name=node_data.provider_name,
        )
        plugin_id = _tool_plugin_unique_identifier(
            node_data=node_data,
            dependencies=self.dependencies,
        )
        if plugin_id is None:
            msg = (
                "Slim tool runtime requires a plugin dependency; builtin and "
                "non-plugin tools are not supported by the default DSL runtime."
            )
            raise ToolNodeError(msg)

        plugin_root = cached_slim_plugin_root(config=self.config, plugin_id=plugin_id)
        configuration_parameters = _resolve_tool_input_values(
            node_data.tool_configurations,
            variable_pool=variable_pool,
        )
        runtime_parameters = _tool_runtime_parameters(
            node_data=node_data,
            plugin_root=plugin_root,
            provider=provider,
        )
        binding = _SlimToolBinding(
            plugin_id=plugin_id,
            provider=provider,
            provider_id=provider_id,
            tool_name=node_data.tool_name,
            credentials=self.tool_credential.values,
            credential_type=self.tool_credential.credential_type,
            configuration_parameters=configuration_parameters,
            runtime_parameters=runtime_parameters,
        )
        return ToolRuntimeHandle(raw=binding)

    def get_runtime_parameters(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
    ) -> Sequence[ToolRuntimeParameter]:
        return self._binding(tool_runtime).runtime_parameters

    def invoke(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
        tool_parameters: Mapping[str, Any],
        workflow_call_depth: int,
        provider_name: str,
    ) -> Generator[ToolRuntimeMessage, None, None]:
        _ = workflow_call_depth, provider_name
        binding = self._binding(tool_runtime)
        merged_parameters = dict(tool_parameters)
        merged_parameters.update(binding.configuration_parameters)
        data = {
            "provider": binding.provider,
            "tool": binding.tool_name,
            "credentials": dict(binding.credentials),
            "credential_type": binding.credential_type,
            "tool_parameters": merged_parameters,
        }
        for payload in self._invoke_action(
            plugin_id=binding.plugin_id,
            action=_SLIM_ACTION_INVOKE_TOOL,
            data=data,
        ):
            yield _tool_runtime_message_from_payload(payload)

    def get_usage(
        self,
        *,
        tool_runtime: ToolRuntimeHandle,
    ) -> LLMUsage:
        _ = tool_runtime
        return LLMUsage.empty_usage()

    def build_file_reference(self, *, mapping: Mapping[str, Any]) -> File:
        _ = mapping
        msg = "Slim DSL tool runtime does not support file tool messages yet."
        raise ToolNodeError(msg)

    def _invoke_action(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Iterable[Mapping[str, Any]]:
        if self.action_invoker is not None:
            return self.action_invoker(plugin_id, action, data)
        if self._client is None:
            msg = "Slim client was not initialized."
            raise ToolNodeError(msg)
        return self._invoke_client(plugin_id=plugin_id, action=action, data=data)

    def _invoke_client(
        self,
        *,
        plugin_id: str,
        action: str,
        data: Mapping[str, Any],
    ) -> Generator[Mapping[str, Any], None, None]:
        if self._client is None:
            msg = "Slim client was not initialized."
            raise ToolNodeError(msg)
        try:
            for chunk in self._client.invoke_chunks(
                plugin_id=plugin_id,
                action=action,
                data=data,
            ):
                if not isinstance(chunk, Mapping):
                    msg = f"Unexpected Slim tool chunk payload: {chunk!r}"
                    raise ToolNodeError(msg)
                yield chunk
        except SlimClientError as error:
            raise ToolNodeError(str(error)) from error

    @staticmethod
    def _binding(tool_runtime: ToolRuntimeHandle) -> _SlimToolBinding:
        if isinstance(tool_runtime.raw, _SlimToolBinding):
            return tool_runtime.raw
        msg = "Tool runtime handle was not created by SlimToolNodeRuntime."
        raise ToolNodeError(msg)


def _tool_provider_name(*, provider_id: str, provider_name: str) -> str:
    for candidate in (provider_id, provider_name):
        parts = [part for part in candidate.split("/") if part]
        if parts:
            return parts[-1]
    return provider_id or provider_name


def _tool_plugin_unique_identifier(
    *,
    node_data: ToolNodeData,
    dependencies: Sequence[DslDependency],
) -> str | None:
    if node_data.plugin_unique_identifier:
        return node_data.plugin_unique_identifier
    provider_candidates = [node_data.provider_id, node_data.provider_name]
    for provider in provider_candidates:
        prefix = _plugin_prefix(provider)
        provider_name = _tool_provider_name(
            provider_id=provider,
            provider_name=provider,
        )
        for dependency in dependencies:
            plugin_unique_identifier = dependency.plugin_unique_identifier
            if not plugin_unique_identifier:
                continue
            if prefix and plugin_unique_identifier.startswith(f"{prefix}:"):
                return plugin_unique_identifier
            if f"/{provider_name}:" in plugin_unique_identifier:
                return plugin_unique_identifier
    return None


def _plugin_prefix(provider: str | None) -> str | None:
    if not provider:
        return None
    parts = [part for part in provider.split("/") if part]
    if len(parts) >= _PLUGIN_PROVIDER_PARTS:
        return "/".join(parts[:_PLUGIN_PROVIDER_PARTS])
    return None


def resolve_dsl_tool_credential(
    *,
    credentials: Sequence[DslToolCredential],
    plugin_id: str,
    provider_id: str,
    provider: str,
    tool_name: str,
) -> DslToolCredential:
    best_credential: DslToolCredential | None = None
    best_score = -1
    for credential in credentials:
        score = _tool_credential_match_score(
            credential=credential,
            plugin_id=plugin_id,
            provider_id=provider_id,
            provider=provider,
            tool_name=tool_name,
        )
        if score is not None and score > best_score:
            best_credential = credential
            best_score = score

    return best_credential or DslToolCredential()


def _tool_credential_match_score(
    *,
    credential: DslToolCredential,
    plugin_id: str,
    provider_id: str,
    provider: str,
    tool_name: str,
) -> int | None:
    score = 0
    selectors = (
        (credential.plugin_unique_identifier, plugin_id, 16),
        (credential.provider_id, provider_id, 8),
        (credential.provider, provider, 4),
        (credential.tool_name, tool_name, 2),
    )
    for actual, expected, weight in selectors:
        if not actual:
            continue
        if actual != expected:
            return None
        score += weight
    return score


def _resolve_tool_input_values(
    tool_inputs: Mapping[str, Any],
    *,
    variable_pool: VariablePool | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, raw_tool_input in tool_inputs.items():
        if not isinstance(raw_tool_input, Mapping):
            result[name] = raw_tool_input
            continue

        input_type = raw_tool_input.get("type")
        value = raw_tool_input.get("value")
        match input_type:
            case "constant":
                result[name] = value
            case "mixed":
                result[name] = (
                    variable_pool.convert_template(str(value)).text
                    if variable_pool is not None
                    else str(value)
                )
            case "variable":
                if variable_pool is None:
                    continue
                if not isinstance(value, list) or not all(
                    isinstance(part, str) for part in value
                ):
                    msg = "Variable tool configuration value must be a list of strings."
                    raise ToolNodeError(msg)
                variable = variable_pool.get(value)
                if variable is not None:
                    result[name] = variable.value
            case _:
                if value is not None:
                    result[name] = value
    return result


def _tool_runtime_parameters(
    *,
    node_data: ToolNodeData,
    plugin_root: Path | None,
    provider: str,
) -> Sequence[ToolRuntimeParameter]:
    parameters = _declared_tool_parameters(
        plugin_root=plugin_root,
        provider=provider,
        tool_name=node_data.tool_name,
    )
    if parameters:
        return parameters

    names = [*node_data.tool_parameters.keys(), *node_data.tool_configurations.keys()]
    return [
        ToolRuntimeParameter(name=name, required=False)
        for index, name in enumerate(dict.fromkeys(names))
    ]


def _declared_tool_parameters(
    *,
    plugin_root: Path | None,
    provider: str,
    tool_name: str,
) -> list[ToolRuntimeParameter]:
    if plugin_root is None:
        return []
    manifest_path = plugin_root / "manifest.yaml"
    if not manifest_path.is_file():
        return []

    try:
        manifest = _load_yaml(manifest_path)
        tool_provider_paths = manifest.get("plugins", {}).get("tools", []) or []
        for provider_path in tool_provider_paths:
            provider_declaration = _load_yaml(plugin_root / str(provider_path))
            identity = provider_declaration.get("identity") or {}
            if identity.get("name") != provider:
                continue
            for tool_path in provider_declaration.get("tools", []) or []:
                tool_declaration = _load_yaml(plugin_root / str(tool_path))
                tool_identity = tool_declaration.get("identity") or {}
                if tool_identity.get("name") != tool_name:
                    continue
                return [
                    ToolRuntimeParameter(
                        name=str(parameter["name"]),
                        required=bool(parameter.get("required", False)),
                    )
                    for parameter in tool_declaration.get("parameters", []) or []
                    if isinstance(parameter, Mapping) and parameter.get("name")
                ]
    except (KeyError, OSError, TypeError, ValueError, yaml.YAMLError) as error:
        logger.debug("failed to read slim tool declaration: %s", error)
    return []


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def _tool_runtime_message_from_payload(
    payload: Mapping[str, Any],
) -> ToolRuntimeMessage:
    message_type = str(payload.get("type") or "text")
    message_payload = payload.get("message")
    meta = payload.get("meta")
    graph_meta = dict(meta) if isinstance(meta, Mapping) else None

    match message_type:
        case "text" | "link":
            message = _text_message(message_payload)
        case "json":
            message = _json_message(message_payload)
        case "variable":
            message = ToolRuntimeMessage.VariableMessage.model_validate(message_payload)
        case "log":
            message = ToolRuntimeMessage.LogMessage.model_validate(message_payload)
        case "retriever_resources":
            message = ToolRuntimeMessage.RetrieverResourceMessage.model_validate(
                message_payload,
            )
        case _:
            msg = (
                "Slim DSL tool runtime does not support tool message type "
                f"{message_type!r}."
            )
            raise ToolNodeError(msg)

    return ToolRuntimeMessage(
        type=ToolRuntimeMessage.MessageType(message_type),
        message=message,
        meta=graph_meta,
    )


def _text_message(payload: object) -> ToolRuntimeMessage.TextMessage:
    if isinstance(payload, Mapping):
        message_payload = cast(Mapping[str, Any], payload)
        return ToolRuntimeMessage.TextMessage(
            text=str(message_payload.get("text") or ""),
        )
    return ToolRuntimeMessage.TextMessage(text=str(payload or ""))


def _json_message(payload: object) -> ToolRuntimeMessage.JsonMessage:
    if not isinstance(payload, Mapping):
        msg = "Slim JSON tool message must be a mapping."
        raise ToolNodeError(msg)
    message_payload = cast(Mapping[str, Any], payload)
    json_object = message_payload.get("json_object")
    if not isinstance(json_object, (dict, list)):
        msg = "Slim JSON tool message must contain object or array json_object."
        raise ToolNodeError(msg)
    return ToolRuntimeMessage.JsonMessage(
        json_object=json_object,
        suppress_output=bool(message_payload.get("suppress_output", False)),
    )
