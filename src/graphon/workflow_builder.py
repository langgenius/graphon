from __future__ import annotations

import importlib
import inspect
import pkgutil
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, cast, final

import graphon.nodes
from graphon.entities.base_node_data import BaseNodeData
from graphon.entities.graph_config import NodeConfigDict
from graphon.entities.graph_init_params import GraphInitParams
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.graph.graph import Graph
from graphon.model_runtime.entities.llm_entities import LLMMode
from graphon.model_runtime.entities.message_entities import (
    PromptMessage,
    PromptMessageRole,
)
from graphon.nodes.base.entities import OutputVariableEntity, OutputVariableType
from graphon.nodes.base.node import Node
from graphon.nodes.llm import (
    LLMNodeChatModelMessage,
    LLMNodeCompletionModelPromptTemplate,
)
from graphon.nodes.llm.file_saver import LLMFileSaver
from graphon.nodes.llm.runtime_protocols import (
    PreparedLLMProtocol,
    PromptMessageSerializerProtocol,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.template_rendering import Jinja2TemplateRenderer
from graphon.variables.input_entities import VariableEntity, VariableEntityType

_NODE_INIT_BASE_PARAMS: Final[frozenset[str]] = frozenset(
    {
        "self",
        "node_id",
        "config",
        "graph_init_params",
        "graph_runtime_state",
    },
)

_BUILTIN_NODE_MODULES_LOADED = False


@dataclass(frozen=True, slots=True)
class WorkflowRuntime:
    workflow_id: str
    graph_runtime_state: GraphRuntimeState
    run_context: Mapping[str, Any] = field(default_factory=dict)
    call_depth: int = 0
    prepared_llm: PreparedLLMProtocol | None = None
    llm_file_saver: LLMFileSaver | None = None
    prompt_message_serializer: PromptMessageSerializerProtocol | None = None
    jinja2_template_renderer: Jinja2TemplateRenderer | None = None
    node_kwargs_factory: NodeKwargsFactory | None = None

    @classmethod
    def from_graph_init_params(
        cls,
        graph_init_params: GraphInitParams,
        *,
        graph_runtime_state: GraphRuntimeState,
        prepared_llm: PreparedLLMProtocol | None = None,
        llm_file_saver: LLMFileSaver | None = None,
        prompt_message_serializer: PromptMessageSerializerProtocol | None = None,
        jinja2_template_renderer: Jinja2TemplateRenderer | None = None,
        node_kwargs_factory: NodeKwargsFactory | None = None,
    ) -> WorkflowRuntime:
        return cls(
            workflow_id=graph_init_params.workflow_id,
            graph_runtime_state=graph_runtime_state,
            run_context=graph_init_params.run_context,
            call_depth=graph_init_params.call_depth,
            prepared_llm=prepared_llm,
            llm_file_saver=llm_file_saver,
            prompt_message_serializer=prompt_message_serializer,
            jinja2_template_renderer=jinja2_template_renderer,
            node_kwargs_factory=node_kwargs_factory,
        )

    def create_graph_init_params(
        self,
        *,
        graph_config: Mapping[str, Any],
    ) -> GraphInitParams:
        return GraphInitParams(
            workflow_id=self.workflow_id,
            graph_config=graph_config,
            run_context=dict(self.run_context),
            call_depth=self.call_depth,
        )


@dataclass(frozen=True, slots=True)
class NodeMaterializationContext[NodeDataT: BaseNodeData]:
    node_id: str
    data: NodeDataT
    runtime: WorkflowRuntime
    graph_init_params: GraphInitParams
    graph_runtime_state: GraphRuntimeState


type NodeKwargsFactory = Callable[
    [NodeMaterializationContext[BaseNodeData], type[Node]],
    Mapping[str, object],
]


@dataclass(frozen=True, slots=True)
class NodeOutputRef:
    node_id: str
    output_name: str

    @property
    def selector(self) -> tuple[str, str]:
        return (self.node_id, self.output_name)

    def as_template(self) -> str:
        return "{{#" + ".".join(self.selector) + "#}}"

    def output(
        self,
        variable: str | None = None,
        *,
        value_type: OutputVariableType = OutputVariableType.ANY,
    ) -> OutputBinding:
        return OutputBinding.from_ref(
            self,
            variable=variable,
            value_type=value_type,
        )

    def __str__(self) -> str:
        return self.as_template()


class OutputBinding(OutputVariableEntity):
    @classmethod
    def from_ref(
        cls,
        ref: NodeOutputRef,
        *,
        variable: str | None = None,
        value_type: OutputVariableType = OutputVariableType.ANY,
    ) -> OutputBinding:
        return cls(
            variable=variable or ref.output_name,
            value_type=value_type,
            value_selector=ref.selector,
        )

    @property
    def selector(self) -> tuple[str, ...]:
        return tuple(self.value_selector)


@dataclass(frozen=True, slots=True)
class WorkflowNodeSpec[NodeDataT: BaseNodeData]:
    node_id: str
    data: NodeDataT

    def as_node_config(self) -> NodeConfigDict:
        return {"id": self.node_id, "data": self.data}


@dataclass(frozen=True, slots=True)
class WorkflowEdgeSpec:
    tail: str
    head: str
    source_handle: str = "source"

    def as_edge_config(self) -> dict[str, str]:
        return {
            "source": self.tail,
            "target": self.head,
            "sourceHandle": self.source_handle,
        }


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    root_node_id: str
    nodes: tuple[WorkflowNodeSpec[BaseNodeData], ...]
    edges: tuple[WorkflowEdgeSpec, ...]

    @property
    def graph_config(self) -> dict[str, list[object]]:
        return {
            "nodes": [node.as_node_config() for node in self.nodes],
            "edges": [edge.as_edge_config() for edge in self.edges],
        }

    def materialize(self, runtime: WorkflowRuntime) -> Graph:
        return _WorkflowMaterializer(runtime=runtime).materialize(self)


@dataclass(frozen=True, slots=True)
class NodeHandle:
    builder: WorkflowBuilder
    node_id: str

    def then(
        self,
        node_id: str,
        data: BaseNodeData,
        *,
        source_handle: str = "source",
    ) -> NodeHandle:
        return self.builder.add_node(
            node_id=node_id,
            data=data,
            from_node_id=self.node_id,
            source_handle=source_handle,
        )

    def connect(
        self,
        target: NodeHandle,
        *,
        source_handle: str = "source",
    ) -> NodeHandle:
        return self.builder.connect(
            tail=self,
            head=target,
            source_handle=source_handle,
        )

    def ref(self, output_name: str) -> NodeOutputRef:
        return NodeOutputRef(node_id=self.node_id, output_name=output_name)


@final
class _PassthroughPromptMessageSerializer:
    def serialize(
        self,
        *,
        model_mode: LLMMode,
        prompt_messages: Sequence[PromptMessage],
    ) -> object:
        _ = model_mode
        return list(prompt_messages)


@final
class _TextOnlyFileSaver:
    def save_binary_string(
        self,
        data: bytes,
        mime_type: str,
        file_type: FileType,
        extension_override: str | None = None,
    ) -> File:
        _ = data, mime_type, file_type, extension_override
        msg = "WorkflowBuilder default saver only supports text outputs."
        raise RuntimeError(msg)

    def save_remote_url(self, url: str, file_type: FileType) -> File:
        _ = url, file_type
        msg = "WorkflowBuilder default saver only supports text outputs."
        raise RuntimeError(msg)


class WorkflowBuilder:
    def __init__(self) -> None:
        self._node_order: list[str] = []
        self._node_specs: dict[str, WorkflowNodeSpec[BaseNodeData]] = {}
        self._edges: list[WorkflowEdgeSpec] = []
        self._handles: dict[str, NodeHandle] = {}
        self._root_node_id: str | None = None

    def root(self, node_id: str, data: BaseNodeData) -> NodeHandle:
        if self._root_node_id is not None:
            msg = f"Root node has already been set to {self._root_node_id!r}."
            raise ValueError(msg)
        self._store_node(node_id=node_id, data=data)
        self._root_node_id = node_id
        return self._remember_handle(node_id)

    def add_node(
        self,
        *,
        node_id: str,
        data: BaseNodeData,
        from_node_id: str,
        source_handle: str = "source",
    ) -> NodeHandle:
        if from_node_id not in self._node_specs:
            msg = f"Predecessor node {from_node_id!r} is not registered."
            raise ValueError(msg)
        self._store_node(node_id=node_id, data=data)
        self._edges.append(
            WorkflowEdgeSpec(
                tail=from_node_id,
                head=node_id,
                source_handle=source_handle,
            ),
        )
        return self._remember_handle(node_id)

    def connect(
        self,
        *,
        tail: NodeHandle,
        head: NodeHandle,
        source_handle: str = "source",
    ) -> NodeHandle:
        self._ensure_owned_handle(tail)
        self._ensure_owned_handle(head)
        self._edges.append(
            WorkflowEdgeSpec(
                tail=tail.node_id,
                head=head.node_id,
                source_handle=source_handle,
            ),
        )
        return head

    def handle(self, node_id: str) -> NodeHandle:
        try:
            return self._handles[node_id]
        except KeyError as error:
            msg = f"Unknown node id {node_id!r}."
            raise KeyError(msg) from error

    def build(self) -> WorkflowSpec:
        if self._root_node_id is None:
            msg = "WorkflowBuilder requires a root node before build()."
            raise ValueError(msg)
        return WorkflowSpec(
            root_node_id=self._root_node_id,
            nodes=tuple(self._node_specs[node_id] for node_id in self._node_order),
            edges=tuple(self._edges),
        )

    def materialize(self, runtime: WorkflowRuntime) -> Graph:
        return self.build().materialize(runtime)

    def _remember_handle(self, node_id: str) -> NodeHandle:
        handle = NodeHandle(builder=self, node_id=node_id)
        self._handles[node_id] = handle
        return handle

    def _store_node(self, *, node_id: str, data: BaseNodeData) -> None:
        if node_id in self._node_specs:
            msg = f"Node id {node_id!r} is already registered."
            raise ValueError(msg)
        self._node_order.append(node_id)
        self._node_specs[node_id] = WorkflowNodeSpec(node_id=node_id, data=data)

    def _ensure_owned_handle(self, handle: NodeHandle) -> None:
        if handle.builder is not self:
            msg = "NodeHandle belongs to a different WorkflowBuilder instance."
            raise ValueError(msg)


@final
class _WorkflowNodeFactory:
    def __init__(
        self,
        *,
        runtime: WorkflowRuntime,
        graph_init_params: GraphInitParams,
    ) -> None:
        self._runtime = runtime
        self._graph_init_params = graph_init_params
        self._llm_file_saver = runtime.llm_file_saver or _TextOnlyFileSaver()
        self._prompt_message_serializer = (
            runtime.prompt_message_serializer or _PassthroughPromptMessageSerializer()
        )

    def create_node(self, node_config: NodeConfigDict) -> Node:
        node_id = node_config["id"]
        node_data = node_config["data"]
        node_cls = _resolve_node_class(node_data)
        typed_node_data = cast(
            "BaseNodeData",
            node_cls.validate_node_data(node_data),
        )
        context = NodeMaterializationContext(
            node_id=node_id,
            data=typed_node_data,
            runtime=self._runtime,
            graph_init_params=self._graph_init_params,
            graph_runtime_state=self._runtime.graph_runtime_state,
        )
        return node_cls(
            **self._base_node_kwargs(context),
            **self._build_extra_node_kwargs(node_cls, context),
        )

    def _build_extra_node_kwargs(
        self,
        node_cls: type[Node],
        context: NodeMaterializationContext[BaseNodeData],
    ) -> dict[str, object]:
        init_parameters = inspect.signature(node_cls.__init__).parameters
        extra_kwargs: dict[str, object] = {}

        if (
            "model_instance" in init_parameters
            and context.runtime.prepared_llm is not None
        ):
            extra_kwargs["model_instance"] = context.runtime.prepared_llm
        if "llm_file_saver" in init_parameters:
            extra_kwargs["llm_file_saver"] = self._llm_file_saver
        if "prompt_message_serializer" in init_parameters:
            extra_kwargs["prompt_message_serializer"] = self._prompt_message_serializer
        if (
            "jinja2_template_renderer" in init_parameters
            and context.runtime.jinja2_template_renderer is not None
        ):
            extra_kwargs["jinja2_template_renderer"] = (
                context.runtime.jinja2_template_renderer
            )
        if (
            "template_renderer" in init_parameters
            and context.runtime.jinja2_template_renderer is not None
        ):
            extra_kwargs["template_renderer"] = context.runtime.jinja2_template_renderer

        if context.runtime.node_kwargs_factory is not None:
            extra_kwargs.update(
                dict(context.runtime.node_kwargs_factory(context, node_cls)),
            )

        missing_kwargs = [
            name
            for name, parameter in init_parameters.items()
            if name not in _NODE_INIT_BASE_PARAMS
            and name not in extra_kwargs
            and parameter.kind
            in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }
            and parameter.default is inspect.Parameter.empty
        ]
        if missing_kwargs:
            missing_args = ", ".join(sorted(missing_kwargs))
            msg = (
                f"{node_cls.__name__} requires additional runtime arguments: "
                f"{missing_args}. "
                "Provide them through WorkflowRuntime or `node_kwargs_factory`."
            )
            raise ValueError(msg)

        return extra_kwargs

    @staticmethod
    def _base_node_kwargs(
        context: NodeMaterializationContext[BaseNodeData],
    ) -> dict[str, object]:
        return {
            "node_id": context.node_id,
            "config": {"id": context.node_id, "data": context.data},
            "graph_init_params": context.graph_init_params,
            "graph_runtime_state": context.graph_runtime_state,
        }


@final
class _WorkflowMaterializer:
    def __init__(self, *, runtime: WorkflowRuntime) -> None:
        self._runtime = runtime

    def materialize(self, workflow: WorkflowSpec) -> Graph:
        graph_config = workflow.graph_config
        graph_init_params = self._runtime.create_graph_init_params(
            graph_config=graph_config,
        )
        return Graph.init(
            graph_config=graph_config,
            node_factory=_WorkflowNodeFactory(
                runtime=self._runtime,
                graph_init_params=graph_init_params,
            ),
            root_node_id=workflow.root_node_id,
        )


def _resolve_node_class(node_data: BaseNodeData) -> type[Node]:
    _bootstrap_builtin_node_registry()
    version_mapping = Node.get_node_type_classes_mapping().get(node_data.type)
    if version_mapping is None:
        msg = (
            f"No node class registered for node type {node_data.type!r}. "
            "Ensure the node module has been imported before materialization."
        )
        raise ValueError(msg)

    node_cls = version_mapping.get(node_data.version)
    if node_cls is None:
        versions = ", ".join(sorted(key for key in version_mapping if key != "latest"))
        msg = (
            f"No node class registered for node type {node_data.type!r} "
            f"with version {node_data.version!r}. "
            f"Available versions: {versions or '<none>'}."
        )
        raise ValueError(msg)

    return node_cls


def _bootstrap_builtin_node_registry() -> None:
    global _BUILTIN_NODE_MODULES_LOADED  # noqa: PLW0603
    if _BUILTIN_NODE_MODULES_LOADED:
        return

    for module_info in pkgutil.walk_packages(
        graphon.nodes.__path__,
        graphon.nodes.__name__ + ".",
    ):
        module_leaf = module_info.name.rsplit(".", maxsplit=1)[-1]
        if module_leaf == "node" or module_leaf.endswith("_node"):
            importlib.import_module(module_info.name)

    _BUILTIN_NODE_MODULES_LOADED = True


def template(*parts: str | NodeOutputRef) -> str:
    return "".join(
        part.as_template() if isinstance(part, NodeOutputRef) else part
        for part in parts
    )


def chat_message(
    role: PromptMessageRole,
    *parts: str | NodeOutputRef,
) -> LLMNodeChatModelMessage:
    return LLMNodeChatModelMessage(role=role, text=template(*parts))


def system(*parts: str | NodeOutputRef) -> LLMNodeChatModelMessage:
    return chat_message(PromptMessageRole.SYSTEM, *parts)


def user(*parts: str | NodeOutputRef) -> LLMNodeChatModelMessage:
    return chat_message(PromptMessageRole.USER, *parts)


def assistant(*parts: str | NodeOutputRef) -> LLMNodeChatModelMessage:
    return chat_message(PromptMessageRole.ASSISTANT, *parts)


def completion_prompt(
    *parts: str | NodeOutputRef,
) -> LLMNodeCompletionModelPromptTemplate:
    return LLMNodeCompletionModelPromptTemplate(text=template(*parts))


def input_variable(
    variable: str,
    *,
    variable_type: VariableEntityType,
    label: str | None = None,
    description: str = "",
    required: bool = False,
    hide: bool = False,
    default: object | None = None,
    max_length: int | None = None,
    options: Sequence[str] = (),
    allowed_file_types: Sequence[FileType] | None = (),
    allowed_file_extensions: Sequence[str] | None = (),
    allowed_file_upload_methods: Sequence[FileTransferMethod] | None = (),
    json_schema: Mapping[str, Any] | None = None,
) -> VariableEntity:
    return VariableEntity(
        variable=variable,
        label=label or variable.replace("_", " ").title(),
        description=description,
        type=variable_type,
        required=required,
        hide=hide,
        default=default,
        max_length=max_length,
        options=list(options),
        allowed_file_types=list(allowed_file_types or []),
        allowed_file_extensions=list(allowed_file_extensions or []),
        allowed_file_upload_methods=list(allowed_file_upload_methods or []),
        json_schema=dict(json_schema) if json_schema is not None else None,
    )


def paragraph_input(
    variable: str,
    *,
    label: str | None = None,
    description: str = "",
    required: bool = False,
    hide: bool = False,
    default: str | None = None,
    max_length: int | None = None,
) -> VariableEntity:
    return input_variable(
        variable,
        variable_type=VariableEntityType.PARAGRAPH,
        label=label,
        description=description,
        required=required,
        hide=hide,
        default=default,
        max_length=max_length,
    )


__all__ = [
    "NodeHandle",
    "NodeMaterializationContext",
    "NodeOutputRef",
    "OutputBinding",
    "WorkflowBuilder",
    "WorkflowEdgeSpec",
    "WorkflowNodeSpec",
    "WorkflowRuntime",
    "WorkflowSpec",
    "assistant",
    "chat_message",
    "completion_prompt",
    "input_variable",
    "paragraph_input",
    "system",
    "template",
    "user",
]
