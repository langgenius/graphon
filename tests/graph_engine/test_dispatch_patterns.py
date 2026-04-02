from datetime import UTC, datetime
from time import time
from unittest.mock import MagicMock

import pytest

from graphon.enums import BuiltinNodeTypes
from graphon.graph_engine.command_channels.redis_channel import RedisChannel
from graphon.graph_engine.entities.commands import (
    AbortCommand,
    CommandType,
    PauseCommand,
    UpdateVariablesCommand,
)
from graphon.graph_engine.layers.execution_limits import (
    ExecutionLimitsLayer,
    LimitType,
)
from graphon.graph_engine.ready_queue.factory import create_ready_queue_from_state
from graphon.graph_engine.ready_queue.protocol import ReadyQueueState
from graphon.graph_events.node import NodeRunStartedEvent


@pytest.mark.parametrize(
    ("payload", "expected_command_type"),
    [
        (
            {"command_type": CommandType.ABORT.value, "reason": "stop"},
            AbortCommand,
        ),
        (
            {"command_type": CommandType.PAUSE.value, "reason": "wait"},
            PauseCommand,
        ),
        (
            {"command_type": CommandType.UPDATE_VARIABLES.value, "updates": []},
            UpdateVariablesCommand,
        ),
    ],
)
def test_redis_channel_deserializes_command_with_model_map(
    payload: dict[str, object],
    expected_command_type: type,
) -> None:
    channel = RedisChannel(redis_client=MagicMock(), channel_key="test-channel")

    command = channel._deserialize_command(payload)

    assert isinstance(command, expected_command_type)


def test_create_ready_queue_from_state_restores_queue_items() -> None:
    queue = create_ready_queue_from_state(
        ReadyQueueState(type="InMemoryReadyQueue", version="1.0", items=["a", "b"]),
    )

    assert queue.get(timeout=0.01) == "a"
    assert queue.get(timeout=0.01) == "b"


@pytest.mark.parametrize(
    ("limit_type", "expected_reason"),
    [
        (LimitType.STEP_LIMIT, "Maximum execution steps exceeded: 4 > 3"),
        (LimitType.TIME_LIMIT, "Maximum execution time exceeded:"),
    ],
)
def test_execution_limits_layer_builds_abort_reason_with_match_case(
    limit_type: LimitType,
    expected_reason: str,
) -> None:
    layer = ExecutionLimitsLayer(max_steps=3, max_time=10)
    layer.command_channel = MagicMock()
    layer._execution_started = True
    layer.step_count = 4
    layer.start_time = time() - 20

    layer._send_abort_command(limit_type)

    abort_command = layer.command_channel.send_command.call_args.args[0]
    assert isinstance(abort_command, AbortCommand)
    assert abort_command.reason is not None
    assert abort_command.reason.startswith(expected_reason)


def test_execution_limits_layer_matches_subclassed_node_start_event() -> None:
    class CustomNodeRunStartedEvent(NodeRunStartedEvent):
        pass

    layer = ExecutionLimitsLayer(max_steps=3, max_time=10)
    layer.on_graph_start()

    layer.on_event(
        CustomNodeRunStartedEvent(
            id="node-run-1",
            node_id="node-1",
            node_type=BuiltinNodeTypes.CODE,
            node_title="Code",
            start_at=datetime.now(UTC).replace(tzinfo=None),
        ),
    )

    assert layer.step_count == 1
