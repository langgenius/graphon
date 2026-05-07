from __future__ import annotations

import importlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from graphon.entities.base_node_data import BaseNodeData
from graphon.entities.graph_config import NodeConfigDict
from graphon.enums import BuiltinNodeTypes
from graphon.file.enums import FileType
from graphon.file.models import File
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.entities.message_entities import PromptMessage
from graphon.nodes.answer.answer_node import AnswerNode
from graphon.nodes.base.node import Node
from graphon.nodes.end.end_node import EndNode
from graphon.nodes.if_else.if_else_node import IfElseNode
from graphon.nodes.llm import LLMNode, LLMNodeData
from graphon.nodes.start import StartNode
from graphon.nodes.template_transform.entities import TemplateTransformNodeData
from graphon.nodes.template_transform.template_transform_node import (
    TemplateTransformNode,
)
from graphon.nodes.tool.entities import ToolNodeData
from graphon.nodes.tool.tool_node import ToolNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.template_rendering import Jinja2TemplateRenderer, TemplateRenderError

from .entities import (
    DslCredentials,
    DslDependency,
    DslModelCredential,
)
from .errors import DslError
from .slim import DslSlimPreparedLLM, SlimClientConfig
from .tool_runtime import SlimToolNodeRuntime, resolve_dsl_tool_credential

_OPENAI_CREDENTIAL_ALIASES = ("openai_api_key", "OPENAI_API_KEY", "api_key")
_OPENAI_OPTIONAL_CREDENTIAL_ALIASES = {
    "openai_api_base": (
        "openai_api_base",
        "OPENAI_API_BASE",
        "OPENAI_BASE_URL",
        "api_base",
        "base_url",
    ),
    "openai_organization": (
        "openai_organization",
        "OPENAI_ORGANIZATION",
        "organization",
    ),
    "validate_model": ("validate_model", "OPENAI_VALIDATE_MODEL"),
    "api_protocol": ("api_protocol", "OPENAI_API_PROTOCOL"),
}
_DEFAULT_SLIM_PLUGIN_FOLDER = ".slim/plugins"
_PLUGIN_PROVIDER_PARTS = 2
_SIMPLE_JINJA_VARIABLE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


def _dsl_error(
    message: str,
    *,
    code: str,
    path: str | None = None,
    kind: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> DslError:
    return DslError(
        message,
        code=code,
        path=path,
        kind=kind,
        details=details,
    )


class _PassthroughPromptMessageSerializer:
    def serialize(
        self,
        *,
        model_mode: LLMMode,
        prompt_messages: Sequence[PromptMessage],
    ) -> object:
        _ = model_mode
        return list(prompt_messages)


class _TextOnlyFileSaver:
    def save_binary_string(
        self,
        data: bytes,
        mime_type: str,
        file_type: FileType,
        extension_override: str | None = None,
    ) -> File:
        _ = data, mime_type, file_type, extension_override
        msg = "DSL import default LLM file saver only supports text responses."
        raise RuntimeError(msg)

    def save_remote_url(self, url: str, file_type: FileType) -> File:
        _ = url, file_type
        msg = "DSL import default LLM file saver only supports text responses."
        raise RuntimeError(msg)


class _UnsupportedToolFileManager:
    def create_file_by_raw(
        self,
        *,
        file_binary: bytes,
        mimetype: str,
        filename: str | None = None,
    ) -> object:
        _ = file_binary, mimetype, filename
        msg = "DSL import default Slim tool runtime does not support tool files yet."
        raise RuntimeError(msg)

    def get_file_generator_by_tool_file_id(
        self,
        tool_file_id: str,
    ) -> tuple[None, None]:
        _ = tool_file_id
        return None, None


class _DefaultJinja2TemplateRenderer(Jinja2TemplateRenderer):
    def render_template(self, template: str, variables: Mapping[str, Any]) -> str:
        try:
            jinja2 = importlib.import_module("jinja2")
        except ImportError:
            return _render_simple_jinja_template(template, variables)

        try:
            environment = jinja2.Environment(
                autoescape=False,
                undefined=jinja2.StrictUndefined,
            )
            return str(environment.from_string(template).render(**dict(variables)))
        except jinja2.TemplateError as error:
            raise TemplateRenderError(str(error)) from error


def _render_simple_jinja_template(
    template: str,
    variables: Mapping[str, Any],
) -> str:
    def replace(match: re.Match[str]) -> str:
        return str(variables.get(match.group(1), ""))

    return _SIMPLE_JINJA_VARIABLE.sub(replace, template)


def _canonical_vendor(provider: str | None) -> str | None:
    if not provider:
        return None
    parts = [part for part in provider.split("/") if part]
    return parts[-1] if parts else provider


def _plugin_prefix(provider: str | None) -> str | None:
    if not provider:
        return None
    parts = [part for part in provider.split("/") if part]
    if len(parts) >= _PLUGIN_PROVIDER_PARTS:
        return "/".join(parts[:_PLUGIN_PROVIDER_PARTS])
    return None


def _find_plugin_id(
    *,
    provider: str | None,
    dependencies: Sequence[DslDependency],
) -> str | None:
    prefix = _plugin_prefix(provider)
    for dependency in dependencies:
        plugin_unique_identifier = dependency.plugin_unique_identifier
        if not plugin_unique_identifier:
            continue
        if prefix is not None and plugin_unique_identifier.startswith(f"{prefix}:"):
            return plugin_unique_identifier
        if provider and f"/{provider}:" in plugin_unique_identifier:
            return plugin_unique_identifier
    return None


def _find_tool_plugin_id(
    *,
    node_data: ToolNodeData,
    dependencies: Sequence[DslDependency],
) -> str | None:
    plugin_unique_identifier = node_data.plugin_unique_identifier
    if plugin_unique_identifier:
        for dependency in dependencies:
            if dependency.plugin_unique_identifier == plugin_unique_identifier:
                return plugin_unique_identifier

    for provider in (node_data.provider_id, node_data.provider_name):
        plugin_id = _find_plugin_id(provider=provider, dependencies=dependencies)
        if plugin_id is not None:
            return plugin_id
    return None


def _normalize_openai_credentials(values: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for alias in _OPENAI_CREDENTIAL_ALIASES:
        value = values.get(alias)
        if isinstance(value, str) and value:
            result["openai_api_key"] = value
            break
    if "openai_api_key" not in result:
        return {}

    for target, aliases in _OPENAI_OPTIONAL_CREDENTIAL_ALIASES.items():
        for alias in aliases:
            value = values.get(alias)
            if isinstance(value, str) and value:
                result[target] = value
                break
    return result


def _resolve_credentials(
    *,
    credentials: DslCredentials,
    provider: str,
    vendor: str,
    plugin_id: str | None,
) -> dict[str, Any]:
    model_credential = _resolve_model_credential(
        credentials=credentials.model_credentials,
        provider=provider,
        vendor=vendor,
        plugin_id=plugin_id,
    )
    values = dict(model_credential.values) if model_credential is not None else {}

    if vendor == "openai":
        normalized = _normalize_openai_credentials(values)
        if normalized:
            return normalized
        msg = "Missing OpenAI credential for LLM node."
        raise _dsl_error(
            msg,
            code="credential.missing_required_key",
            details={"required_keys": ["openai_api_key"], "vendor": vendor},
        )

    return values


def _resolve_model_credential(
    *,
    credentials: Sequence[DslModelCredential],
    provider: str,
    vendor: str,
    plugin_id: str | None,
) -> DslModelCredential | None:
    best_credential: DslModelCredential | None = None
    best_score = -1
    for credential in credentials:
        score = _model_credential_match_score(
            credential=credential,
            provider=provider,
            vendor=vendor,
            plugin_id=plugin_id,
        )
        if score is not None and score > best_score:
            best_credential = credential
            best_score = score
    return best_credential


def _model_credential_match_score(
    *,
    credential: DslModelCredential,
    provider: str,
    vendor: str,
    plugin_id: str | None,
) -> int | None:
    score = 0
    if credential.plugin_unique_identifier:
        if credential.plugin_unique_identifier != plugin_id:
            return None
        score += 8
    if credential.provider:
        if credential.provider != provider:
            return None
        score += 4
    if credential.vendor:
        if credential.vendor != vendor:
            return None
        score += 2
    return score


def _resolve_plugin_folder(credentials: DslCredentials) -> Path:
    if credentials.slim.plugin_folder:
        return Path(credentials.slim.plugin_folder).expanduser()

    env_folder = os.environ.get("SLIM_PLUGIN_FOLDER", "").strip()
    if env_folder:
        return Path(env_folder).expanduser()

    return Path(_DEFAULT_SLIM_PLUGIN_FOLDER)


def _node_data_payload(data: BaseNodeData | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(data, BaseNodeData):
        return data.model_dump(mode="python")
    return dict(data)


@dataclass(slots=True)
class SlimDslNodeFactory:
    graph_config: Mapping[str, Any]
    graph_init_params: Any
    graph_runtime_state: GraphRuntimeState
    credentials: DslCredentials
    dependencies: list[DslDependency]
    slim_client_config: SlimClientConfig = field(init=False)

    def __post_init__(self) -> None:
        slim = self.credentials.slim
        self.slim_client_config = SlimClientConfig(
            folder=_resolve_plugin_folder(self.credentials),
            mode=slim.mode,
            daemon_addr=slim.daemon_addr,
            daemon_key=slim.daemon_key,
            python_path=slim.python_path,
            uv_path=slim.uv_path,
            python_env_init_timeout=slim.python_env_init_timeout,
            max_execution_timeout=slim.max_execution_timeout,
            pip_mirror_url=slim.pip_mirror_url,
            pip_extra_args=slim.pip_extra_args,
            marketplace_url=slim.marketplace_url,
            ignore_uv_lock=slim.ignore_uv_lock,
        )

    def create_node(self, node_config: NodeConfigDict) -> Node:  # noqa: PLR0911
        node_id = str(node_config["id"])
        data = node_config["data"]
        data_payload = _node_data_payload(data)
        node_type = data_payload["type"]

        match node_type:
            case BuiltinNodeTypes.START:
                return StartNode(
                    node_id=node_id,
                    data=StartNode.validate_node_data(data),
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                )
            case BuiltinNodeTypes.END:
                return EndNode(
                    node_id=node_id,
                    data=EndNode.validate_node_data(data),
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                )
            case BuiltinNodeTypes.ANSWER:
                return AnswerNode(
                    node_id=node_id,
                    data=AnswerNode.validate_node_data(data),
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                )
            case BuiltinNodeTypes.IF_ELSE:
                return IfElseNode(
                    node_id=node_id,
                    data=IfElseNode.validate_node_data(data),
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                )
            case BuiltinNodeTypes.TEMPLATE_TRANSFORM:
                return TemplateTransformNode(
                    node_id=node_id,
                    data=TemplateTransformNodeData.model_validate(data_payload),
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                    jinja2_template_renderer=_DefaultJinja2TemplateRenderer(),
                )
            case BuiltinNodeTypes.LLM:
                return self._create_llm_node(node_id=node_id, data=data_payload)
            case BuiltinNodeTypes.TOOL:
                tool_data = ToolNodeData.model_validate(data_payload)
                try:
                    runtime = self._create_tool_runtime(tool_data)
                except DslError:
                    raise
                except Exception as error:
                    raise _dsl_error(
                        str(error),
                        code="runtime.slim_unavailable",
                        path=f"/nodes/{node_id}/data",
                        details={"node_id": node_id, "node_type": node_type},
                    ) from error
                return ToolNode(
                    node_id=node_id,
                    data=tool_data,
                    graph_init_params=self.graph_init_params,
                    graph_runtime_state=self.graph_runtime_state,
                    tool_file_manager_factory=_UnsupportedToolFileManager(),
                    runtime=runtime,
                )
            case _:
                msg = f"Unsupported DSL node type: {node_type}"
                raise _dsl_error(
                    msg,
                    code="node.unsupported_type",
                    path=f"/nodes/{node_id}",
                    details={"node_id": node_id, "node_type": node_type},
                )

    def _create_llm_node(self, *, node_id: str, data: Mapping[str, Any]) -> LLMNode:
        normalized_data = dict(data)
        model = dict(normalized_data.get("model") or {})
        raw_provider = str(model.get("provider") or "")
        vendor = _canonical_vendor(raw_provider)
        if not vendor:
            msg = "LLM node is missing model provider."
            raise _dsl_error(
                msg,
                code="node.llm_missing_provider",
                path=f"/nodes/{node_id}/data/model/provider",
                details={"node_id": node_id},
            )
        model["provider"] = vendor
        normalized_data["model"] = model

        plugin_id = _find_plugin_id(
            provider=raw_provider,
            dependencies=self.dependencies,
        )
        if plugin_id is None:
            msg = "LLM node dependency could not be resolved."
            raise _dsl_error(
                msg,
                code="dependency.missing_plugin",
                path=f"/nodes/{node_id}/data/model/provider",
                details={"node_id": node_id, "provider": raw_provider},
            )

        credentials = _resolve_credentials(
            credentials=self.credentials,
            provider=raw_provider,
            vendor=vendor,
            plugin_id=plugin_id,
        )
        try:
            prepared_llm = DslSlimPreparedLLM(
                config=self.slim_client_config,
                plugin_id=plugin_id,
                provider=vendor,
                model_name=str(model.get("name") or ""),
                credentials=credentials,
                parameters=model.get("completion_params") or {},
            )
        except Exception as error:
            raise _dsl_error(
                str(error),
                code="runtime.slim_unavailable",
                path=f"/nodes/{node_id}/data/model",
                details={"node_id": node_id, "vendor": vendor},
            ) from error
        return LLMNode(
            node_id=node_id,
            data=LLMNodeData.model_validate(normalized_data),
            graph_init_params=self.graph_init_params,
            graph_runtime_state=self.graph_runtime_state,
            model_instance=prepared_llm,
            llm_file_saver=_TextOnlyFileSaver(),
            prompt_message_serializer=_PassthroughPromptMessageSerializer(),
            default_query_selector=("sys", "query"),
        )

    def _create_tool_runtime(self, node_data: ToolNodeData) -> SlimToolNodeRuntime:
        plugin_dependencies = [
            dependency
            for dependency in self.dependencies
            if dependency.plugin_unique_identifier
        ]
        plugin_id = _find_tool_plugin_id(
            node_data=node_data,
            dependencies=plugin_dependencies,
        )
        if plugin_id is None:
            msg = "Tool node dependency could not be resolved."
            raise _dsl_error(
                msg,
                code="dependency.missing_plugin",
                details={"node_type": BuiltinNodeTypes.TOOL},
            )
        provider = _canonical_vendor(node_data.provider_id) or _canonical_vendor(
            node_data.provider_name,
        )
        if provider is None:
            msg = "Tool node is missing provider."
            raise _dsl_error(
                msg,
                code="node.tool_missing_provider",
                details={"node_type": BuiltinNodeTypes.TOOL},
            )
        dependency = next(
            dependency
            for dependency in plugin_dependencies
            if dependency.plugin_unique_identifier == plugin_id
        )

        return SlimToolNodeRuntime(
            config=self.slim_client_config,
            tool_credential=resolve_dsl_tool_credential(
                credentials=self.credentials.tool_credentials,
                plugin_id=plugin_id,
                provider_id=node_data.provider_id,
                provider=provider,
                tool_name=node_data.tool_name,
            ),
            dependencies=[dependency],
        )
