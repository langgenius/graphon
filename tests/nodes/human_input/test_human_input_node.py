from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from graphon.enums import WorkflowNodeExecutionStatus
from graphon.graph_events.node import NodeRunSucceededEvent
from graphon.nodes.human_input.enums import FormInputType, HumanInputFormStatus
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from tests.helpers import build_graph_init_params, build_variable_pool


@dataclass
class _StubHumanInputFormState:
    id: str = "form-1"
    rendered_content: str = "Name: {{#$output.name#}}"
    selected_action_id: str | None = "submit"
    submitted_data: Mapping[str, Any] | None = None
    submitted: bool = True
    status: HumanInputFormStatus = HumanInputFormStatus.SUBMITTED
    expiration_time: datetime = field(
        default_factory=lambda: (
            datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=1)
        ),
    )


class _StubHumanInputRuntime:
    def __init__(self, form: _StubHumanInputFormState | None) -> None:
        self._form = form

    def get_form(self, *, node_id: str) -> _StubHumanInputFormState | None:
        _ = node_id
        return self._form

    def create_form(self, **_: object) -> _StubHumanInputFormState:
        msg = "create_form should not be called in submitted-form tests"
        raise AssertionError(msg)


def test_submitted_form_payload_is_preserved_in_trace_inputs() -> None:
    submitted_data = {"name": "Alice", "notes": "ready for review"}
    runtime_state = GraphRuntimeState(
        variable_pool=build_variable_pool(),
        start_at=perf_counter(),
    )
    node = HumanInputNode(
        node_id="human-input",
        config=HumanInputNode.validate_node_data(
            {
                "type": "human-input",
                "title": "Human Input",
                "form_content": "Name: {{#$output.name#}}",
                "inputs": [
                    {
                        "type": FormInputType.TEXT_INPUT,
                        "output_variable_name": "name",
                    },
                    {
                        "type": FormInputType.PARAGRAPH,
                        "output_variable_name": "notes",
                    },
                ],
                "user_actions": [{"id": "submit", "title": "Submit"}],
            },
        ),
        graph_init_params=build_graph_init_params(
            graph_config={"nodes": [], "edges": []},
        ),
        graph_runtime_state=runtime_state,
        runtime=_StubHumanInputRuntime(
            _StubHumanInputFormState(submitted_data=submitted_data),
        ),
    )

    events = list(node.run())

    result_event = next(
        event for event in events if isinstance(event, NodeRunSucceededEvent)
    )

    assert result_event.node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert result_event.node_run_result.inputs == submitted_data
    assert result_event.node_run_result.outputs == {
        **submitted_data,
        "__action_id": "submit",
        "__rendered_content": "Name: Alice",
    }
