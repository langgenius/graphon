from __future__ import annotations

from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Any, ClassVar

import pytest

import graphon.dsl.node_factory as node_factory_module
from graphon.dsl import loads
from graphon.graph_events.base import GraphEngineEvent, GraphNodeEventBase
from graphon.graph_events.graph import GraphRunSucceededEvent
from graphon.graph_events.traversal import GraphEdgeTakenEvent
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

_EMPTY_MAPPING: Mapping[str, Any] = MappingProxyType({})


class FakeSlimLLM:
    instances: ClassVar[list[FakeSlimLLM]] = []
    responses: ClassVar[dict[str, list[str]]] = {}

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
        self._stop = list(stop) if stop is not None else []
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
    def stop(self) -> Sequence[str]:
        return list(self._stop)

    def get_model_schema(self) -> AIModelEntity:
        return AIModelEntity(
            model=self._model_name,
            label=I18nObject(en_US=self._model_name),
            model_type=ModelType.LLM,
            features=[ModelFeature.STREAM_TOOL_CALL],
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
    ) -> LLMResult:
        stop_values: Sequence[str] = ()
        if stop is not None:
            stop_values = list(stop)
        self.invoke_calls.append({
            "prompt_messages": list(prompt_messages),
            "model_parameters": dict(model_parameters),
            "tools": list(tools or []),
            "stop": list(stop_values),
            "stream": stream,
        })
        remaining = self.responses[self._model_name]
        if not remaining:
            msg = f"No fake Slim LLM responses left for model {self._model_name}"
            raise AssertionError(msg)
        content = remaining.pop(0)
        return LLMResult(
            model=self._model_name,
            prompt_messages=list(prompt_messages),
            message=AssistantPromptMessage(content=content),
            usage=LLMUsage.empty_usage(),
        )


def fake_slim_llm(
    monkeypatch: pytest.MonkeyPatch,
    *,
    responses: Mapping[str, Sequence[str]],
) -> type[FakeSlimLLM]:
    FakeSlimLLM.instances = []
    FakeSlimLLM.responses = {
        model_name: list(model_responses)
        for model_name, model_responses in responses.items()
    }
    monkeypatch.setitem(node_factory_module.__dict__, "SlimLLM", FakeSlimLLM)
    return FakeSlimLLM


def run_workflow(
    dsl: str,
    *,
    start_inputs: Mapping[str, Any] = _EMPTY_MAPPING,
    credentials: Mapping[str, Any] = _EMPTY_MAPPING,
) -> list[GraphEngineEvent]:
    engine = loads(
        dsl,
        start_inputs=dict(start_inputs),
        credentials=dict(credentials),
    )
    return list(engine.run())


def event_path(
    events: Sequence[GraphEngineEvent],
) -> list[tuple[str, str, str, str]]:
    """Project events to stable fields while preserving the complete event order."""
    path: list[tuple[str, str, str, str]] = []
    for event in events:
        if isinstance(event, GraphNodeEventBase):
            path.append((
                type(event).__name__,
                event.node_id,
                event.in_loop_id or "",
                event.in_iteration_id or "",
            ))
        elif isinstance(event, GraphEdgeTakenEvent):
            path.append((
                type(event).__name__,
                f"{event.source_node_id}->{event.target_node_id}",
                "",
                "",
            ))
        else:
            path.append((type(event).__name__, "", "", ""))
    return path


def final_outputs(events: Sequence[GraphEngineEvent]) -> dict[str, object]:
    for event in reversed(events):
        if isinstance(event, GraphRunSucceededEvent):
            return event.outputs
    msg = "graph did not succeed"
    raise AssertionError(msg)
