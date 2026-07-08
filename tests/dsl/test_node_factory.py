from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar, cast

import pytest
import yaml

from graphon.dsl import loads
from graphon.dsl.entities import DslCredentials
from graphon.dsl.errors import DslError
from graphon.dsl.node_factory import (
    SlimDslNodeFactory,
    _TextOnlyFileSaver,
    _UnsupportedHttpFileReferenceFactory,
)
from graphon.entities.graph_config import NodeConfigDict
from graphon.file.enums import FileTransferMethod, FileType
from graphon.file.models import File
from graphon.graph_events.node import (
    NodeRunFailedEvent,
    NodeRunSucceededEvent,
    NodeRunVariableUpdatedEvent,
)
from graphon.http import HttpResponse
from graphon.model_runtime.entities.common_entities import I18nObject
from graphon.model_runtime.entities.llm_entities import LLMResult, LLMUsage
from graphon.model_runtime.entities.message_entities import (
    AssistantPromptMessage,
    PromptMessage,
    PromptMessageTool,
)
from graphon.model_runtime.entities.model_entities import (
    AIModelEntity,
    FetchFrom,
    ModelFeature,
    ModelType,
)
from graphon.nodes.http_request.exc import HttpRequestNodeError
from graphon.nodes.http_request.node import HttpRequestNode
from graphon.nodes.list_operator.node import ListOperatorNode
from graphon.nodes.llm.exc import LLMNodeError
from graphon.nodes.parameter_extractor.parameter_extractor_node import (
    ParameterExtractorNode,
)
from graphon.nodes.question_classifier import QuestionClassifierNode
from graphon.nodes.variable_aggregator.variable_aggregator_node import (
    VariableAggregatorNode,
)
from graphon.nodes.variable_assigner.v1.node import (
    VariableAssignerNode as VariableAssignerNodeV1,
)
from graphon.nodes.variable_assigner.v2.node import (
    VariableAssignerNode as VariableAssignerNodeV2,
)
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from tests.helpers import build_graph_init_params, build_variable_pool

_OPENAI_PLUGIN_ID = "langgenius/openai:0.3.8@test"


class _FakeSlimLLM:
    instances: ClassVar[list[_FakeSlimLLM]] = []
    responses: ClassVar[dict[str, Any]] = {}
    features: ClassVar[list[ModelFeature]] = []
    tool_calls: ClassVar[dict[str, list[AssistantPromptMessage.ToolCall]]] = {}

    def __init__(
        self,
        *,
        config: object,
        plugin_id: str,
        provider: str,
        model_name: str,
        credentials: Mapping[str, Any],
        parameters: Mapping[str, Any] | None = None,
        stop: Sequence[str] | None = None,
    ) -> None:
        self.config = config
        self.plugin_id = plugin_id
        self._provider = provider
        self._model_name = model_name
        self.credentials = dict(credentials)
        self._parameters = dict(parameters or {})
        self._stop = list(stop) if stop is not None else None
        self.invoke_calls: list[dict[str, Any]] = []
        self.instances.append(self)

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def parameters(self) -> Mapping[str, Any]:
        return dict(self._parameters)

    @parameters.setter
    def parameters(self, value: Mapping[str, Any]) -> None:
        self._parameters = dict(value)

    @property
    def stop(self) -> Sequence[str] | None:
        return None if self._stop is None else list(self._stop)

    def get_model_schema(self) -> AIModelEntity:
        return AIModelEntity(
            model=self._model_name,
            label=I18nObject(en_US=self._model_name),
            model_type=ModelType.LLM,
            features=list(self.features),
            fetch_from=FetchFrom.CUSTOMIZABLE_MODEL,
            model_properties={},
            parameter_rules=[],
        )

    def get_llm_num_tokens(self, prompt_messages: Sequence[PromptMessage]) -> int:
        _ = prompt_messages
        return 0

    def invoke_llm(
        self,
        *,
        prompt_messages: Sequence[PromptMessage],
        model_parameters: Mapping[str, Any],
        tools: Sequence[PromptMessageTool] | None,
        stop: Sequence[str] | None,
        stream: bool,
        first_token_timeout: float | None = None,
    ) -> LLMResult:
        self.invoke_calls.append({
            "prompt_messages": list(prompt_messages),
            "model_parameters": dict(model_parameters),
            "tools": list(tools or []),
            "stop": stop,
            "stream": stream,
            "first_token_timeout": first_token_timeout,
        })
        return LLMResult(
            model=self._model_name,
            prompt_messages=list(prompt_messages),
            message=AssistantPromptMessage(
                content=self.responses.get(self._model_name, "{}"),
                tool_calls=list(self.tool_calls.get(self._model_name, [])),
            ),
            usage=LLMUsage.empty_usage(),
        )


def _patch_fake_slim_llm(monkeypatch: pytest.MonkeyPatch) -> type[_FakeSlimLLM]:
    _FakeSlimLLM.instances = []
    _FakeSlimLLM.responses = {}
    _FakeSlimLLM.features = []
    _FakeSlimLLM.tool_calls = {}
    monkeypatch.setattr("graphon.dsl.node_factory.SlimLLM", _FakeSlimLLM)
    return _FakeSlimLLM


def _openai_credentials() -> dict[str, Any]:
    return {
        "model_credentials": [
            {
                "vendor": "openai",
                "values": {"api_key": "secret-key"},
            },
        ],
    }


def _graph_dsl_for_node(node_data: dict[str, Any]) -> str:
    return _graph_dsl_for_nodes(
        nodes=[{"id": "node", "data": node_data}],
        edges=[{"source": "start", "target": "node"}],
    )


def _graph_dsl_for_node_without_dependencies(node_data: dict[str, Any]) -> str:
    return yaml.safe_dump({
        "kind": "graph",
        "graph": {
            "nodes": [
                {"id": "start", "data": {"type": "start", "variables": []}},
                {"id": "node", "data": node_data},
            ],
            "edges": [{"source": "start", "target": "node"}],
        },
    })


def _graph_dsl_for_nodes(
    *,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> str:
    return yaml.safe_dump({
        "kind": "graph",
        "dependencies": [
            {
                "type": "marketplace",
                "value": {
                    "marketplace_plugin_unique_identifier": _OPENAI_PLUGIN_ID,
                },
            },
        ],
        "graph": {
            "nodes": [
                {"id": "start", "data": {"type": "start", "variables": []}},
                *nodes,
            ],
            "edges": edges,
        },
    })


def _exported_app_dsl_with_category_one_two_nodes() -> str:
    return yaml.safe_dump({
        "kind": "app",
        "version": "0.3.0",
        "app": {
            "mode": "workflow",
            "name": "Category one and two export",
        },
        "dependencies": [
            {
                "type": "marketplace",
                "value": {
                    "marketplace_plugin_unique_identifier": _OPENAI_PLUGIN_ID,
                },
            },
        ],
        "workflow": {
            "environment_variables": [
                {"name": "api_key", "value": "env-secret"},
            ],
            "conversation_variables": [
                {"name": "topic", "value": "before"},
            ],
            "graph": {
                "nodes": [
                    {
                        "id": "start",
                        "type": "custom",
                        "position": {"x": 0, "y": 0},
                        "data": {"type": "start", "title": "Start", "variables": []},
                    },
                    {
                        "id": "http",
                        "type": "custom",
                        "position": {"x": 320, "y": 0},
                        "data": {
                            **_http_request_data(),
                            "title": "HTTP Request",
                            "headers": "Authorization: Bearer {{#env.api_key#}}",
                            "params": "q: {{#start.query#}}",
                            "body": {
                                "type": "raw-text",
                                "data": [
                                    {
                                        "type": "text",
                                        "value": "{{#start.query#}}",
                                    },
                                ],
                            },
                            "timeout": {"connect": 5, "read": 10, "write": 5},
                        },
                    },
                    {
                        "id": "aggregate",
                        "type": "custom",
                        "position": {"x": 640, "y": 0},
                        "data": {
                            **_variable_aggregator_data(),
                            "title": "Variable Aggregator",
                            "advanced_settings": {
                                "group_enabled": False,
                                "groups": [],
                            },
                        },
                    },
                    {
                        "id": "assign",
                        "type": "custom",
                        "position": {"x": 960, "y": 0},
                        "data": {
                            **_assigner_v2_data(),
                            "title": "Variable Assigner",
                        },
                    },
                    {
                        "id": "list",
                        "type": "custom",
                        "position": {"x": 1280, "y": 0},
                        "data": {
                            **_list_operator_data(),
                            "title": "List Operator",
                            "extract_by": {"enabled": False, "serial": "1"},
                        },
                    },
                    {
                        "id": "classifier",
                        "type": "custom",
                        "position": {"x": 1600, "y": 0},
                        "data": {
                            **_question_classifier_data(),
                            "title": "Question Classifier",
                        },
                    },
                    {
                        "id": "extractor",
                        "type": "custom",
                        "position": {"x": 1920, "y": 0},
                        "data": {
                            **_parameter_extractor_data(),
                            "title": "Parameter Extractor",
                        },
                    },
                ],
                "edges": [
                    {
                        "id": "start-http",
                        "source": "start",
                        "target": "http",
                        "sourceHandle": "source",
                        "targetHandle": "target",
                        "data": {"isInIteration": False},
                    },
                    {"source": "start", "target": "aggregate"},
                    {"source": "start", "target": "assign"},
                    {"source": "start", "target": "list"},
                    {
                        "source": "start",
                        "target": "classifier",
                        "sourceHandle": "source",
                    },
                    {
                        "source": "start",
                        "target": "extractor",
                        "sourceHandle": "source",
                    },
                ],
                "viewport": {"x": 0, "y": 0, "zoom": 1},
            },
        },
    })


def _succeeded_event(events: list[object]) -> NodeRunSucceededEvent:
    return next(event for event in events if isinstance(event, NodeRunSucceededEvent))


def _failed_event(events: list[object]) -> NodeRunFailedEvent:
    return next(event for event in events if isinstance(event, NodeRunFailedEvent))


def _dsl_node_factory(
    *,
    variables: Sequence[tuple[Sequence[str], Any]] = (),
) -> SlimDslNodeFactory:
    return SlimDslNodeFactory(
        graph_config={"nodes": [], "edges": []},
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=build_variable_pool(variables=variables),
            start_at=0,
        ),
        credentials=DslCredentials(),
        dependencies=[],
    )


def _http_request_data() -> dict[str, Any]:
    return {
        "type": "http-request",
        "method": "get",
        "url": "https://example.com/api",
        "authorization": {"type": "no-auth"},
        "headers": "",
        "params": "",
        "body": {"type": "none"},
    }


def _variable_aggregator_data() -> dict[str, Any]:
    return {
        "type": "variable-aggregator",
        "output_type": "string",
        "variables": [["missing", "value"], ["start", "candidate"]],
    }


def _assigner_v1_data() -> dict[str, Any]:
    return {
        "type": "assigner",
        "version": "1",
        "assigned_variable_selector": ["conversation", "topic"],
        "write_mode": "over-write",
        "input_variable_selector": ["start", "value"],
    }


def _assigner_v2_data() -> dict[str, Any]:
    return {
        "type": "assigner",
        "version": "2",
        "items": [
            {
                "variable_selector": ["conversation", "topic"],
                "input_type": "variable",
                "operation": "over-write",
                "value": ["start", "value"],
            },
        ],
    }


def _list_operator_data() -> dict[str, Any]:
    return {
        "type": "list-operator",
        "variable": ["start", "items"],
        "filter_by": {"enabled": False, "conditions": []},
        "order_by": {"enabled": False},
        "limit": {"enabled": False},
    }


def _model(name: str) -> dict[str, Any]:
    return {
        "provider": "langgenius/openai/openai",
        "name": name,
        "mode": "chat",
        "completion_params": {"temperature": 0.1},
    }


def _question_classifier_data() -> dict[str, Any]:
    return {
        "type": "question-classifier",
        "query_variable_selector": ["start", "query"],
        "model": _model("classifier-model"),
        "classes": [
            {"id": "billing", "name": "Billing questions", "label": "Billing"},
            {"id": "refund", "name": "Refund requests", "label": "Refunds"},
        ],
        "instruction": "Choose the best category.",
    }


def _parameter_extractor_data() -> dict[str, Any]:
    return {
        "type": "parameter-extractor",
        "query": ["start", "query"],
        "model": _model("extractor-model"),
        "parameters": [
            {
                "name": "location",
                "type": "string",
                "description": "The requested location",
                "required": True,
            },
        ],
        "instruction": "Extract the requested location.",
        "reasoning_mode": "prompt",
    }


@pytest.mark.parametrize(
    ("node_data", "expected_type"),
    [
        (_http_request_data(), HttpRequestNode),
        (_variable_aggregator_data(), VariableAggregatorNode),
        (_assigner_v1_data(), VariableAssignerNodeV1),
        (_assigner_v2_data(), VariableAssignerNodeV2),
        (_list_operator_data(), ListOperatorNode),
    ],
)
def test_default_factory_creates_builtin_runtime_node(
    node_data: dict[str, Any],
    expected_type: type[object],
) -> None:
    engine = loads(_graph_dsl_for_node(node_data))

    assert isinstance(engine.graph.nodes["node"], expected_type)


def test_variable_assigner_invalid_payload_raises_dsl_error() -> None:
    with pytest.raises(DslError) as exc_info:
        loads(_graph_dsl_for_node({"type": "assigner"}))

    assert exc_info.value.code == "node.assigner_invalid_payload"
    assert exc_info.value.path == "/nodes/node/data"


@pytest.mark.parametrize(
    ("node_data", "expected_type", "model_name"),
    [
        (_question_classifier_data(), QuestionClassifierNode, "classifier-model"),
        (_parameter_extractor_data(), ParameterExtractorNode, "extractor-model"),
    ],
)
def test_default_factory_creates_slim_backed_model_node(
    monkeypatch: pytest.MonkeyPatch,
    node_data: dict[str, Any],
    expected_type: type[object],
    model_name: str,
) -> None:
    fake_slim_llm = _patch_fake_slim_llm(monkeypatch)

    engine = loads(
        _graph_dsl_for_node(node_data),
        credentials=_openai_credentials(),
    )

    assert isinstance(engine.graph.nodes["node"], expected_type)
    model_instance = fake_slim_llm.instances[-1]
    assert model_instance.plugin_id == _OPENAI_PLUGIN_ID
    assert model_instance.provider == "openai"
    assert model_instance.model_name == model_name
    assert model_instance.credentials == {"openai_api_key": "secret-key"}
    assert model_instance.parameters == {"temperature": 0.1}


@pytest.mark.parametrize(
    ("node_data", "message"),
    [
        (
            {**_question_classifier_data(), "model": {"name": "classifier-model"}},
            "Question classifier node is missing model provider.",
        ),
        (
            {**_parameter_extractor_data(), "model": {"name": "extractor-model"}},
            "Parameter extractor node is missing model provider.",
        ),
    ],
)
def test_slim_backed_model_node_requires_provider(
    node_data: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(DslError) as exc_info:
        loads(_graph_dsl_for_node(node_data), credentials=_openai_credentials())

    assert str(exc_info.value) == message
    assert exc_info.value.code == "node.llm_missing_provider"
    assert exc_info.value.path == "/nodes/node/data/model/provider"


@pytest.mark.parametrize(
    "node_data",
    [
        _question_classifier_data(),
        _parameter_extractor_data(),
    ],
)
def test_slim_backed_model_node_requires_plugin_dependency(
    node_data: dict[str, Any],
) -> None:
    with pytest.raises(DslError) as exc_info:
        loads(
            _graph_dsl_for_node_without_dependencies(node_data),
            credentials=_openai_credentials(),
        )

    assert exc_info.value.code == "dependency.missing_plugin"
    assert exc_info.value.path == "/nodes/node/data/model/provider"


def test_dify_exported_app_loads_category_one_and_two_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_slim_llm = _patch_fake_slim_llm(monkeypatch)

    engine = loads(
        _exported_app_dsl_with_category_one_two_nodes(),
        credentials=_openai_credentials(),
        start_inputs={"query": "Where is my refund?", "items": ["first"]},
    )

    assert isinstance(engine.graph.nodes["http"], HttpRequestNode)
    assert isinstance(engine.graph.nodes["aggregate"], VariableAggregatorNode)
    assert isinstance(engine.graph.nodes["assign"], VariableAssignerNodeV2)
    assert isinstance(engine.graph.nodes["list"], ListOperatorNode)
    assert isinstance(engine.graph.nodes["classifier"], QuestionClassifierNode)
    assert isinstance(engine.graph.nodes["extractor"], ParameterExtractorNode)
    assert [instance.model_name for instance in fake_slim_llm.instances] == [
        "classifier-model",
        "extractor-model",
    ]
    env_var = engine.graph_runtime_state.variable_pool.get(["env", "api_key"])
    conversation_var = engine.graph_runtime_state.variable_pool.get([
        "conversation",
        "topic",
    ])
    assert env_var is not None
    assert conversation_var is not None
    assert env_var.to_object() == "env-secret"
    assert conversation_var.to_object() == "before"


def test_variable_aggregator_node_from_dsl_runs_first_available_selector() -> None:
    engine = loads(
        _graph_dsl_for_node(_variable_aggregator_data()),
        start_inputs={"candidate": "hello"},
    )
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    output = success.node_run_result.outputs["output"]
    assert output.to_object() == "hello"


def test_list_operator_node_from_dsl_runs_against_start_input_array() -> None:
    engine = loads(
        _graph_dsl_for_node(_list_operator_data()),
        start_inputs={"items": ["first", "second"]},
    )
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    assert success.node_run_result.outputs["first_record"] == "first"
    assert success.node_run_result.outputs["last_record"] == "second"


def test_assigner_node_from_dsl_emits_variable_update() -> None:
    engine = loads(
        _graph_dsl_for_node(_assigner_v2_data()),
        start_inputs={"value": "after"},
    )
    engine.graph_runtime_state.variable_pool.add(["conversation", "topic"], "before")
    node = engine.graph.nodes["node"]

    events = list(node.run())

    update = next(
        event for event in events if isinstance(event, NodeRunVariableUpdatedEvent)
    )
    assert update.variable.selector == ["conversation", "topic"]
    assert update.variable.value == "after"


class _FakeRequestError(Exception):
    pass


class _FakeMaxRetriesExceededError(Exception):
    pass


class _FakeHttpClient:
    def __init__(
        self,
        *,
        headers: Mapping[str, str] | None = None,
        content: bytes = b"ok",
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self.headers = dict(headers or {"content-type": "text/plain"})
        self.content = content

    @property
    def max_retries_exceeded_error(self) -> type[Exception]:
        return _FakeMaxRetriesExceededError

    @property
    def request_error(self) -> type[Exception]:
        return _FakeRequestError

    def get(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        self.calls.append({"url": url, "max_retries": max_retries, **kwargs})
        return HttpResponse(
            status_code=200,
            headers=self.headers,
            content=self.content,
            url=url,
        )

    def head(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self.get(url, max_retries=max_retries, **kwargs)

    def post(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self.get(url, max_retries=max_retries, **kwargs)

    def put(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self.get(url, max_retries=max_retries, **kwargs)

    def delete(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self.get(url, max_retries=max_retries, **kwargs)

    def patch(self, url: str, max_retries: int = 0, **kwargs: Any) -> HttpResponse:
        return self.get(url, max_retries=max_retries, **kwargs)


def test_http_request_node_from_dsl_runs_text_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http_client = _FakeHttpClient()
    monkeypatch.setattr(
        "graphon.nodes.http_request.node.get_http_client",
        lambda: http_client,
    )
    engine = loads(_graph_dsl_for_node(_http_request_data()))
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    assert http_client.calls[0]["url"] == "https://example.com/api"
    assert success.node_run_result.outputs["status_code"] == 200
    assert success.node_run_result.outputs["body"] == "ok"


def test_http_request_node_from_dsl_fails_file_response_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http_client = _FakeHttpClient(headers={"content-type": "image/png"}, content=b"png")
    monkeypatch.setattr(
        "graphon.nodes.http_request.node.get_http_client",
        lambda: http_client,
    )
    engine = loads(_graph_dsl_for_node(_http_request_data()))
    node = engine.graph.nodes["node"]

    failed = _failed_event(list(node.run()))

    assert failed.error == (
        "DSL import default HTTP request runtime only supports text responses."
    )


def test_http_request_node_file_body_download_fails_cleanly() -> None:
    file_value = File(
        file_type=FileType.IMAGE,
        transfer_method=FileTransferMethod.REMOTE_URL,
        remote_url="https://example.com/file.png",
    )
    factory = _dsl_node_factory(variables=[(["start", "file"], file_value)])
    node_data = _http_request_data()
    node_data["body"] = {
        "type": "binary",
        "data": [{"type": "file", "file": ["start", "file"]}],
    }
    node = factory.create_node(cast(NodeConfigDict, {"id": "node", "data": node_data}))

    failed = _failed_event(list(node.run()))

    assert failed.error == (
        "DSL import default HTTP request runtime only supports text requests."
    )


def test_http_file_reference_factory_fails_with_http_node_error() -> None:
    with pytest.raises(HttpRequestNodeError, match="only supports text responses"):
        _UnsupportedHttpFileReferenceFactory().build_from_mapping(
            mapping={
                "tool_file_id": "tool-file",
                "transfer_method": FileTransferMethod.TOOL_FILE,
            },
        )


def test_default_llm_file_saver_fails_with_llm_node_error() -> None:
    with pytest.raises(LLMNodeError, match="only supports text responses"):
        _TextOnlyFileSaver().save_remote_url(
            "https://example.com/image.png",
            FileType.IMAGE,
        )


def test_question_classifier_node_from_dsl_runs_with_slim_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_slim_llm = _patch_fake_slim_llm(monkeypatch)
    fake_slim_llm.responses = {
        "classifier-model": (
            '{"category_id": "refund", "category_name": "Refund requests"}'
        ),
    }
    engine = loads(
        _graph_dsl_for_node(_question_classifier_data()),
        credentials=_openai_credentials(),
        start_inputs={"query": "Where is my refund?"},
    )
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    assert success.node_run_result.outputs["class_id"] == "refund"
    assert success.node_run_result.outputs["class_label"] == "Refunds"
    assert success.node_run_result.outputs["class_name"] == "Refund requests"
    assert fake_slim_llm.instances[-1].invoke_calls[-1]["stream"] is True


def test_parameter_extractor_node_from_dsl_runs_with_slim_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_slim_llm = _patch_fake_slim_llm(monkeypatch)
    fake_slim_llm.responses = {"extractor-model": '{"location": "Paris"}'}
    engine = loads(
        _graph_dsl_for_node(_parameter_extractor_data()),
        credentials=_openai_credentials(),
        start_inputs={"query": "Book a trip to Paris"},
    )
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    assert success.node_run_result.outputs["__is_success"] == 1
    assert success.node_run_result.outputs["__reason"] is None
    assert success.node_run_result.outputs["location"] == "Paris"
    assert fake_slim_llm.instances[-1].invoke_calls[-1]["stream"] is False


def test_parameter_extractor_node_from_dsl_runs_function_call_with_slim_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_slim_llm = _patch_fake_slim_llm(monkeypatch)
    fake_slim_llm.features = [ModelFeature.TOOL_CALL]
    fake_slim_llm.tool_calls = {
        "extractor-model": [
            AssistantPromptMessage.ToolCall(
                id="call-1",
                type="function",
                function=AssistantPromptMessage.ToolCall.ToolCallFunction(
                    name="extract_parameters",
                    arguments='{"location": "Berlin"}',
                ),
            ),
        ],
    }
    engine = loads(
        _graph_dsl_for_node({
            **_parameter_extractor_data(),
            "reasoning_mode": "function_call",
        }),
        credentials=_openai_credentials(),
        start_inputs={"query": "Book a trip to Berlin"},
    )
    node = engine.graph.nodes["node"]

    success = _succeeded_event(list(node.run()))

    assert success.node_run_result.outputs["__is_success"] == 1
    assert success.node_run_result.outputs["__reason"] is None
    assert success.node_run_result.outputs["location"] == "Berlin"
    invoke_call = fake_slim_llm.instances[-1].invoke_calls[-1]
    assert invoke_call["stream"] is False
    assert [tool.name for tool in invoke_call["tools"]] == ["extract_parameters"]
