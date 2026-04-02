from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from graphon.enums import BuiltinNodeTypes, WorkflowNodeExecutionMetadataKey
from graphon.graph_events.graph import GraphRunSucceededEvent
from graphon.graph_events.node import NodeRunStartedEvent, NodeRunSucceededEvent
from graphon.nodes.iteration.entities import ErrorHandleMode
from graphon.nodes.iteration.iteration_node import IterationNode
from graphon.nodes.loop.loop_node import LoopNode
from graphon.variables.segments import StringSegment
from graphon.variables.variables import IntegerVariable


def test_iteration_node_single_iter_keeps_iteration_event_dispatch() -> None:
    node = IterationNode.__new__(IterationNode)
    node._node_id = "iteration-node"
    node._node_data = SimpleNamespace(
        output_selector=["iteration-node", "answer"],
        error_handle_mode=ErrorHandleMode.TERMINATED,
    )

    variable_pool = MagicMock()
    variable_pool.get.side_effect = lambda selector: {
        ("iteration-node", "index"): IntegerVariable(
            name="index",
            selector=["iteration-node", "index"],
            value=2,
        ),
        ("iteration-node", "answer"): StringSegment(value="done"),
    }.get(tuple(selector))

    child_event = NodeRunStartedEvent(
        id="child-run-1",
        node_id="child-node",
        node_type=BuiltinNodeTypes.CODE,
        node_title="Code",
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    graph_engine = SimpleNamespace(
        run=lambda: iter([child_event, GraphRunSucceededEvent()]),
    )
    outputs: list[object] = []

    yielded_events = list(
        node._run_single_iter(
            variable_pool=variable_pool,
            outputs=outputs,
            graph_engine=graph_engine,
        ),
    )

    assert yielded_events == [child_event]
    assert child_event.in_iteration_id == "iteration-node"
    assert (
        child_event.node_run_result.metadata[
            WorkflowNodeExecutionMetadataKey.ITERATION_INDEX
        ]
        == 2
    )
    assert outputs == ["done"]


def test_loop_node_single_loop_keeps_loop_end_dispatch() -> None:
    node = LoopNode.__new__(LoopNode)
    node._node_id = "loop-node"
    node._node_data = SimpleNamespace(loop_variables=None, outputs={})
    node.graph_runtime_state = SimpleNamespace(variable_pool=MagicMock())

    loop_end_event = NodeRunSucceededEvent(
        id="loop-end-1",
        node_id="loop-end-node",
        node_type=BuiltinNodeTypes.LOOP_END,
        start_at=datetime.now(UTC).replace(tzinfo=None),
    )
    graph_engine = SimpleNamespace(run=lambda: iter([loop_end_event]))
    loop_state: dict[str, bool] = {}

    yielded_events = list(
        node._run_single_loop(
            graph_engine=graph_engine,
            current_index=1,
            loop_state=loop_state,
        ),
    )

    assert yielded_events == [loop_end_event]
    assert loop_end_event.in_loop_id == "loop-node"
    assert loop_state["reach_break_node"] is True
    assert node.node_data.outputs["loop_round"] == 2
