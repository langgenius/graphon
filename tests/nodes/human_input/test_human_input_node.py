from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import time

from graphon.entities.graph_init_params import GraphInitParams
from graphon.node_events.node import HumanInputFormTimeoutEvent, StreamCompletedEvent
from graphon.nodes.human_input.entities import HumanInputNodeData, UserAction
from graphon.nodes.human_input.enums import (
    ButtonStyle,
    HumanInputFormStatus,
    TimeoutUnit,
)
from graphon.nodes.human_input.human_input_node import HumanInputNode
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.variable_pool import VariablePool


@dataclass
class _FakeForm:
    id: str
    rendered_content: str
    expiration_time: datetime
    status: HumanInputFormStatus
    selected_action_id: str | None = None
    submitted_data: dict[str, str] | None = None

    @property
    def submitted(self) -> bool:
        return self.status == HumanInputFormStatus.SUBMITTED


class _FakeHumanInputRuntime:
    def __init__(self, form: _FakeForm) -> None:
        self._form = form

    def get_form(self, *, node_id: str) -> _FakeForm | None:
        assert node_id == "human_input_node"
        return self._form

    def create_form(
        self,
        *,
        node_id: str,
        node_data: HumanInputNodeData,
        rendered_content: str,
        resolved_default_values: dict[str, object],
    ) -> _FakeForm:
        _ = (node_id, node_data, rendered_content, resolved_default_values)
        msg = "create_form should not be called in these tests"
        raise AssertionError(msg)


def _build_graph_init_params() -> GraphInitParams:
    return GraphInitParams(
        workflow_id="workflow",
        graph_config={},
        run_context={},
        call_depth=0,
    )


def _build_node(*, form: _FakeForm) -> HumanInputNode:
    node_data = HumanInputNodeData(
        title="Approval",
        type="human-input",
        form_content="Selected ticket: {{#$output.ticket#}}",
        user_actions=[
            UserAction(
                id="approve",
                title="card_visa_enterprise_001_long_value",
                button_style=ButtonStyle.DEFAULT,
            ),
        ],
        timeout=3,
        timeout_unit=TimeoutUnit.DAY,
    )
    return HumanInputNode(
        node_id="human_input_node",
        config=node_data,
        graph_init_params=_build_graph_init_params(),
        graph_runtime_state=GraphRuntimeState(
            variable_pool=VariablePool(),
            start_at=time(),
        ),
        runtime=_FakeHumanInputRuntime(form),
    )


def _run_node_events(form: _FakeForm) -> list[object]:
    return list(_build_node(form=form)._run())  # noqa: SLF001


def test_user_action_title_accepts_long_business_value() -> None:
    action = UserAction(
        id="approve",
        title="card_visa_enterprise_001_long_value",
        button_style=ButtonStyle.DEFAULT,
    )

    assert action.title == "card_visa_enterprise_001_long_value"


def test_human_input_submission_emits_action_value_outputs() -> None:
    form = _FakeForm(
        id="form-1",
        rendered_content="Selected ticket: {{#$output.ticket#}}",
        expiration_time=datetime.now(UTC).replace(tzinfo=None) + timedelta(hours=1),
        status=HumanInputFormStatus.SUBMITTED,
        selected_action_id="approve",
        submitted_data={"ticket": "TICKET-1"},
    )

    events = _run_node_events(form)
    completed = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )

    assert completed.node_run_result.outputs["__action_id"] == "approve"
    assert (
        completed.node_run_result.outputs["__action_value"]
        == "card_visa_enterprise_001_long_value"
    )
    assert (
        completed.node_run_result.outputs["__rendered_content"]
        == "Selected ticket: TICKET-1"
    )


def test_human_input_timeout_emits_empty_action_value() -> None:
    form = _FakeForm(
        id="form-2",
        rendered_content="Selected ticket: {{#$output.ticket#}}",
        expiration_time=datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=1),
        status=HumanInputFormStatus.TIMEOUT,
    )

    events = _run_node_events(form)

    assert any(isinstance(event, HumanInputFormTimeoutEvent) for event in events)
    completed = next(
        event for event in events if isinstance(event, StreamCompletedEvent)
    )
    assert not completed.node_run_result.outputs["__action_id"]
    assert not completed.node_run_result.outputs["__action_value"]
