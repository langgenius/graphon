from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, ClassVar

import pytest

import graphon.dsl.node_factory as node_factory_module
from graphon.dsl import loads
from graphon.graph_events.base import GraphEngineEvent, GraphNodeEventBase
from graphon.graph_events.graph import GraphRunStartedEvent, GraphRunSucceededEvent
from graphon.graph_events.iteration import (
    NodeRunIterationNextEvent,
    NodeRunIterationStartedEvent,
    NodeRunIterationSucceededEvent,
)
from graphon.graph_events.loop import (
    NodeRunLoopNextEvent,
    NodeRunLoopStartedEvent,
    NodeRunLoopSucceededEvent,
)
from graphon.graph_events.node import NodeRunStartedEvent, NodeRunSucceededEvent
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


@dataclass(frozen=True, slots=True)
class EventCheck:
    description: str
    matches: Callable[[GraphEngineEvent], bool]


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


def expect_event_path(
    events: Sequence[GraphEngineEvent],
    checks: Sequence[EventCheck],
) -> None:
    event_index = 0
    for check in checks:
        while event_index < len(events):
            event = events[event_index]
            event_index += 1
            if check.matches(event):
                break
        else:
            remaining = ", ".join(_event_label(event) for event in events[event_index:])
            msg = f"Expected event path item not found: {check.description}"
            if remaining:
                msg = f"{msg}. Remaining events: {remaining}"
            raise AssertionError(msg)


def graph_started() -> EventCheck:
    return EventCheck(
        "graph started",
        lambda event: isinstance(event, GraphRunStartedEvent),
    )


def graph_succeeded(
    *,
    outputs: Mapping[str, object] = _EMPTY_MAPPING,
) -> EventCheck:
    return EventCheck(
        f"graph succeeded with {dict(outputs)}",
        lambda event: (
            isinstance(event, GraphRunSucceededEvent)
            and _outputs_include(event.outputs, outputs)
        ),
    )


def node_started(node_id: str) -> EventCheck:
    return EventCheck(
        f"node {node_id} started",
        lambda event: (
            isinstance(event, NodeRunStartedEvent) and event.node_id == node_id
        ),
    )


def node_succeeded(
    node_id: str,
    *,
    outputs: Mapping[str, object] = _EMPTY_MAPPING,
    in_loop: str = "",
    in_iteration: str = "",
) -> EventCheck:
    return EventCheck(
        f"node {node_id} succeeded with {dict(outputs)}",
        lambda event: (
            isinstance(event, NodeRunSucceededEvent)
            and event.node_id == node_id
            and _outputs_include(event.node_run_result.outputs, outputs)
            and _context_matches(
                event,
                in_loop=in_loop,
                in_iteration=in_iteration,
            )
        ),
    )


def loop_started(node_id: str) -> EventCheck:
    return EventCheck(
        f"loop {node_id} started",
        lambda event: (
            isinstance(event, NodeRunLoopStartedEvent) and event.node_id == node_id
        ),
    )


def loop_next(node_id: str, *, index: int) -> EventCheck:
    return EventCheck(
        f"loop {node_id} next {index}",
        lambda event: (
            isinstance(event, NodeRunLoopNextEvent)
            and event.node_id == node_id
            and event.index == index
        ),
    )


def loop_succeeded(
    node_id: str,
    *,
    steps: int,
    outputs: Mapping[str, object] = _EMPTY_MAPPING,
) -> EventCheck:
    return EventCheck(
        f"loop {node_id} succeeded after {steps} steps",
        lambda event: (
            isinstance(event, NodeRunLoopSucceededEvent)
            and event.node_id == node_id
            and event.steps == steps
            and _outputs_include(event.outputs, outputs)
        ),
    )


def iteration_started(node_id: str) -> EventCheck:
    return EventCheck(
        f"iteration {node_id} started",
        lambda event: (
            isinstance(event, NodeRunIterationStartedEvent) and event.node_id == node_id
        ),
    )


def iteration_next(node_id: str, *, index: int) -> EventCheck:
    return EventCheck(
        f"iteration {node_id} next {index}",
        lambda event: (
            isinstance(event, NodeRunIterationNextEvent)
            and event.node_id == node_id
            and event.index == index
        ),
    )


def iteration_succeeded(
    node_id: str,
    *,
    steps: int,
    outputs: Mapping[str, object] = _EMPTY_MAPPING,
) -> EventCheck:
    return EventCheck(
        f"iteration {node_id} succeeded after {steps} steps",
        lambda event: (
            isinstance(event, NodeRunIterationSucceededEvent)
            and event.node_id == node_id
            and event.steps == steps
            and _outputs_include(event.outputs, outputs)
        ),
    )


def _outputs_include(
    actual: Mapping[str, object],
    expected: Mapping[str, object],
) -> bool:
    for key, expected_value in expected.items():
        if key not in actual:
            return False
        if actual[key] != expected_value:
            return False
    return True


def _context_matches(
    event: NodeRunSucceededEvent,
    *,
    in_loop: str,
    in_iteration: str,
) -> bool:
    return not (in_loop and event.in_loop_id != in_loop) and not (
        in_iteration and event.in_iteration_id != in_iteration
    )


def _event_label(event: GraphEngineEvent) -> str:
    event_name = event.__class__.__name__
    if isinstance(event, NodeRunLoopNextEvent | NodeRunIterationNextEvent):
        return f"{event_name}({event.node_id}, {event.index})"
    if isinstance(event, GraphNodeEventBase):
        return f"{event_name}({event.node_id})"
    return event_name
