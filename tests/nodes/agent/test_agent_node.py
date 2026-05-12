from __future__ import annotations

import time
from collections.abc import Generator, Mapping
from dataclasses import dataclass, field
from typing import Any

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events.agent import NodeRunAgentLogEvent
from graphon.graph_events.node import (
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
)
from graphon.nodes.agent import AgentNode, AgentNodeData, AgentParameterValue
from graphon.nodes.runtime import AgentNodeRuntimeProtocol
from graphon.nodes.tool_runtime_entities import ToolRuntimeMessage
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool
from tests.helpers.builders import build_graph_init_params, build_variable_pool


@dataclass(slots=True)
class _FakeAgentNodeRuntime(AgentNodeRuntimeProtocol):
    """Test stub that yields a canned ``ToolRuntimeMessage`` stream."""

    messages: list[ToolRuntimeMessage] = field(default_factory=list)
    captured_params: list[Mapping[str, Any]] = field(default_factory=list)
    raise_error: BaseException | None = None

    def invoke(
        self,
        *,
        node_id: str,
        node_data: AgentNodeData,
        agent_strategy_params: Mapping[str, Any],
        variable_pool: VariablePool | None,
    ) -> Generator[ToolRuntimeMessage, None, None]:
        _ = node_id, node_data, variable_pool
        self.captured_params.append(dict(agent_strategy_params))
        if self.raise_error is not None:
            raise self.raise_error
        yield from self.messages


def _text(text: str) -> ToolRuntimeMessage:
    return ToolRuntimeMessage(
        type=ToolRuntimeMessage.MessageType.TEXT,
        message=ToolRuntimeMessage.TextMessage(text=text),
    )


def _log(
    *, label: str, status: str = "start", data: dict[str, Any] | None = None
) -> ToolRuntimeMessage:
    return ToolRuntimeMessage(
        type=ToolRuntimeMessage.MessageType.LOG,
        message=ToolRuntimeMessage.LogMessage(
            id=f"log-{label}",
            label=label,
            status=ToolRuntimeMessage.LogMessage.LogStatus(status),
            data=data or {},
        ),
    )


def _json(payload: dict[str, Any]) -> ToolRuntimeMessage:
    return ToolRuntimeMessage(
        type=ToolRuntimeMessage.MessageType.JSON,
        message=ToolRuntimeMessage.JsonMessage(json_object=payload),
    )


def _variable(name: str, value: Any) -> ToolRuntimeMessage:
    return ToolRuntimeMessage(
        type=ToolRuntimeMessage.MessageType.VARIABLE,
        message=ToolRuntimeMessage.VariableMessage(
            variable_name=name,
            variable_value=value,
        ),
    )


def _node_data(
    *,
    parameters: dict[str, AgentParameterValue] | None = None,
) -> AgentNodeData:
    return AgentNodeData(
        title="Agent",
        agent_strategy_provider_name="langgenius/agent/agent",
        agent_strategy_name="function_calling",
        plugin_unique_identifier="langgenius/agent:0.0.36@hash",
        agent_parameters=parameters or {},
    )


def _build_agent_node(
    *,
    runtime: AgentNodeRuntimeProtocol,
    parameters: dict[str, AgentParameterValue] | None = None,
    variable_pool: VariablePool | None = None,
) -> tuple[AgentNode, GraphRuntimeState]:
    state = GraphRuntimeState(
        variable_pool=variable_pool or build_variable_pool(),
        start_at=time.time(),
    )
    node = AgentNode(
        node_id="agent",
        data=_node_data(parameters=parameters),
        graph_init_params=build_graph_init_params(),
        graph_runtime_state=state,
        runtime=runtime,
    )
    return node, state


def test_run_streams_text_messages_and_concatenates_outputs() -> None:
    runtime = _FakeAgentNodeRuntime(
        messages=[_text("Hello "), _text("world!")],
    )
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())

    chunk_events = [e for e in events if isinstance(e, NodeRunStreamChunkEvent)]
    assert [e.chunk for e in chunk_events] == ["Hello ", "world!"]
    assert all(tuple(e.selector) == ("agent", "text") for e in chunk_events)

    success_events = [e for e in events if isinstance(e, NodeRunSucceededEvent)]
    assert len(success_events) == 1
    outputs = success_events[0].node_run_result.outputs
    assert outputs["text"] == "Hello world!"


def test_run_emits_log_messages_as_agent_log_events() -> None:
    runtime = _FakeAgentNodeRuntime(
        messages=[
            _log(label="thinking", status="start", data={"step": 1}),
            _text("answer"),
            _log(label="thinking", status="success", data={"step": 1}),
        ],
    )
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())

    log_events = [e for e in events if isinstance(e, NodeRunAgentLogEvent)]
    assert len(log_events) == 2
    assert [e.label for e in log_events] == ["thinking", "thinking"]
    assert [e.status for e in log_events] == ["start", "success"]
    assert log_events[0].data == {"step": 1}
    assert all(e.node_id == "agent" for e in log_events)


def test_run_collects_json_message_into_outputs() -> None:
    runtime = _FakeAgentNodeRuntime(
        messages=[
            _text("done"),
            _json({"result": 42, "kind": "answer"}),
        ],
    )
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())
    success = next(e for e in events if isinstance(e, NodeRunSucceededEvent))
    outputs = success.node_run_result.outputs
    assert outputs["text"] == "done"
    assert outputs["json"] == {"result": 42, "kind": "answer"}


def test_run_collects_multiple_json_messages_into_list() -> None:
    runtime = _FakeAgentNodeRuntime(
        messages=[
            _json({"step": 1}),
            _json({"step": 2}),
        ],
    )
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())
    success = next(e for e in events if isinstance(e, NodeRunSucceededEvent))
    assert success.node_run_result.outputs["json"] == [{"step": 1}, {"step": 2}]


def test_run_assigns_variable_messages_to_outputs() -> None:
    runtime = _FakeAgentNodeRuntime(
        messages=[
            _variable("answer", "ok"),
            _variable("hits", 7),
        ],
    )
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())
    success = next(e for e in events if isinstance(e, NodeRunSucceededEvent))
    outputs = success.node_run_result.outputs
    assert outputs["answer"] == "ok"
    assert outputs["hits"] == 7


def test_run_resolves_constant_parameter_unchanged() -> None:
    runtime = _FakeAgentNodeRuntime(messages=[_text("ok")])
    parameters = {
        "instruction": AgentParameterValue(type="constant", value="be concise"),
        "max_iterations": AgentParameterValue(type="constant", value=5),
    }
    node, _ = _build_agent_node(runtime=runtime, parameters=parameters)

    list(node.run())

    assert runtime.captured_params == [
        {"instruction": "be concise", "max_iterations": 5},
    ]


def test_run_resolves_variable_parameter_from_pool() -> None:
    pool = build_variable_pool(
        variables=[(("start", "query"), "What is the answer?")],
    )
    runtime = _FakeAgentNodeRuntime(messages=[_text("ok")])
    parameters = {
        "query": AgentParameterValue(type="variable", value=["start", "query"]),
    }
    node, _ = _build_agent_node(
        runtime=runtime,
        parameters=parameters,
        variable_pool=pool,
    )

    list(node.run())

    assert runtime.captured_params == [{"query": "What is the answer?"}]


def test_run_resolves_mixed_parameter_via_template() -> None:
    pool = build_variable_pool(
        variables=[(("start", "name"), "Ben")],
    )
    runtime = _FakeAgentNodeRuntime(messages=[_text("ok")])
    parameters = {
        "instruction": AgentParameterValue(
            type="mixed",
            value="Hello {{#start.name#}}, please respond.",
        ),
    }
    node, _ = _build_agent_node(
        runtime=runtime,
        parameters=parameters,
        variable_pool=pool,
    )

    list(node.run())

    assert runtime.captured_params == [
        {"instruction": "Hello Ben, please respond."},
    ]


def test_run_omits_missing_variable_parameter_silently() -> None:
    pool = build_variable_pool()
    runtime = _FakeAgentNodeRuntime(messages=[_text("ok")])
    parameters = {
        "query": AgentParameterValue(type="variable", value=["start", "missing"]),
    }
    node, _ = _build_agent_node(
        runtime=runtime,
        parameters=parameters,
        variable_pool=pool,
    )

    list(node.run())

    assert runtime.captured_params == [{}]


def test_run_reports_failure_when_runtime_raises() -> None:
    runtime = _FakeAgentNodeRuntime(raise_error=RuntimeError("strategy crashed"))
    node, _ = _build_agent_node(runtime=runtime)

    events = list(node.run())
    success = [e for e in events if isinstance(e, NodeRunSucceededEvent)]
    assert success == []
    failure_events = [
        e for e in events if getattr(e, "node_run_result", None) is not None
    ]
    # Look at the final node result to confirm failure was reported.
    failed = next(
        e
        for e in failure_events
        if e.node_run_result.status == WorkflowNodeExecutionStatus.FAILED
    )
    assert "strategy crashed" in failed.node_run_result.error
    assert failed.node_run_result.error_type == "RuntimeError"
