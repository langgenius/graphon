import json
from time import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from graphon.enums import BuiltinNodeTypes, NodeState
from graphon.graph_engine.domain.graph_execution import GraphExecution
from graphon.graph_engine.ready_queue.in_memory import InMemoryReadyQueue
from graphon.model_runtime.entities.llm_entities import LLMUsage
from graphon.runtime.graph_runtime_state import GraphRuntimeState
from graphon.runtime.read_only_wrappers import ReadOnlyGraphRuntimeStateWrapper
from graphon.runtime.variable_pool import VariablePool
from graphon.variables.variables import StringVariable

CONVERSATION_VARIABLE_NODE_ID = "conversation"


class StubCoordinator:
    def __init__(self) -> None:
        self.state = "initial"

    def dumps(self) -> str:
        return json.dumps({"state": self.state})

    def loads(self, data: str) -> None:
        payload = json.loads(data)
        self.state = payload["state"]


def _remove_writable_flags(snapshot: dict[str, Any]) -> dict[str, Any]:
    variable_pool = snapshot.get("variable_pool")
    if not isinstance(variable_pool, dict):
        return snapshot

    variable_dictionary = variable_pool.get("variable_dictionary")
    if not isinstance(variable_dictionary, dict):
        return snapshot

    for node_variables in variable_dictionary.values():
        if not isinstance(node_variables, dict):
            continue
        for variable_payload in node_variables.values():
            if isinstance(variable_payload, dict):
                variable_payload.pop("writable", None)
    return snapshot


class TestGraphRuntimeState:  # noqa: PLR0904
    def test_execution_context_defaults_to_empty_context(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        with state.execution_context:
            assert state.execution_context is not None

        state.execution_context = None

        with state.execution_context:
            assert state.execution_context is not None

    def test_property_getters_and_setters(self) -> None:
        variable_pool = VariablePool()
        start_time = time()

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=start_time)

        assert state.variable_pool == variable_pool

        assert state.start_at == start_time
        new_time = time() + 100
        state.start_at = new_time
        assert state.start_at == new_time

        assert state.total_tokens == 0
        state.total_tokens = 100
        assert state.total_tokens == 100

        assert state.node_run_steps == 0
        state.node_run_steps = 5
        assert state.node_run_steps == 5

    def test_outputs_immutability(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        outputs1 = state.outputs
        outputs2 = state.outputs
        assert outputs1 == outputs2
        assert outputs1 is not outputs2

        outputs = state.outputs
        outputs["test"] = "value"
        assert "test" not in state.outputs

        state.set_output("key1", "value1")
        assert state.get_output("key1") == "value1"

        state.update_outputs({"key2": "value2", "key3": "value3"})
        assert state.get_output("key2") == "value2"
        assert state.get_output("key3") == "value3"

    def test_merge_response_outputs_appends_answer_and_overwrites_others(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        state.merge_response_outputs({"answer": "Hello", "status": "draft"})
        state.merge_response_outputs({"answer": " world", "status": "final"})

        assert state.get_output("answer") == "Hello world"
        assert state.get_output("status") == "final"

    def test_llm_usage_immutability(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        usage1 = state.llm_usage
        usage2 = state.llm_usage
        assert usage1 is not usage2

    def test_type_validation(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        with pytest.raises(ValueError, match="total_tokens must be non-negative"):
            state.total_tokens = -1

        with pytest.raises(ValueError, match="node_run_steps must be non-negative"):
            state.node_run_steps = -1

    def test_helper_methods(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        initial_steps = state.node_run_steps
        state.increment_node_run_steps()
        assert state.node_run_steps == initial_steps + 1

        initial_tokens = state.total_tokens
        state.add_tokens(50)
        assert state.total_tokens == initial_tokens + 50

        with pytest.raises(ValueError, match="tokens must be non-negative"):
            state.add_tokens(-1)

    def test_ready_queue_default_instantiation(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        queue = state.ready_queue

        assert isinstance(queue, InMemoryReadyQueue)

    def test_graph_execution_lazy_instantiation(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())

        execution = state.graph_execution

        assert isinstance(execution, GraphExecution)
        assert not execution.workflow_id
        assert state.graph_execution is execution

    def test_response_coordinator_configuration(self) -> None:
        variable_pool = VariablePool()
        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())

        with pytest.raises(
            ValueError,
            match="Graph must be attached before accessing response coordinator",
        ):
            _ = state.response_coordinator

        mock_graph = MagicMock()
        with patch(
            "graphon.graph_engine.response_coordinator.ResponseStreamCoordinator",
            autospec=True,
        ) as coordinator_cls:
            coordinator_instance = coordinator_cls.return_value
            state.configure(graph=mock_graph)

            assert state.response_coordinator is coordinator_instance
            coordinator_cls.assert_called_once_with(
                variable_pool=variable_pool,
                graph=mock_graph,
            )

            state.configure(graph=mock_graph)

        other_graph = MagicMock()
        with pytest.raises(
            ValueError,
            match="GraphRuntimeState already attached to a different graph instance",
        ):
            state.attach_graph(other_graph)

    def test_read_only_wrapper_exposes_additional_state(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        state.configure()

        wrapper = ReadOnlyGraphRuntimeStateWrapper(state)

        assert wrapper.ready_queue_size == 0
        assert wrapper.exceptions_count == 0

    def test_read_only_wrapper_serializes_runtime_state(self) -> None:
        state = GraphRuntimeState(variable_pool=VariablePool(), start_at=time())
        state.total_tokens = 5
        state.set_output("result", {"success": True})
        state.ready_queue.put("node-1")

        wrapper = ReadOnlyGraphRuntimeStateWrapper(state)

        wrapper_snapshot = json.loads(wrapper.dumps())
        state_snapshot = json.loads(state.dumps())

        assert wrapper_snapshot == state_snapshot

    def test_dumps_and_loads_roundtrip_with_response_coordinator(self) -> None:
        variable_pool = VariablePool()
        variable_pool.add(("node1", "value"), "payload")

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        state.total_tokens = 10
        state.node_run_steps = 3
        state.set_output("final", {"result": True})
        usage = LLMUsage.from_metadata({
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "total_tokens": 5,
            "total_price": "1.23",
            "currency": "USD",
            "latency": 0.5,
        })
        state.llm_usage = usage
        state.ready_queue.put("node-A")

        graph_execution = state.graph_execution
        graph_execution.workflow_id = "wf-123"
        graph_execution.exceptions_count = 4
        graph_execution.started = True

        mock_graph = MagicMock()
        stub = StubCoordinator()
        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=stub,
            autospec=True,
        ):
            state.attach_graph(mock_graph)

        stub.state = "configured"

        snapshot = state.dumps()

        restored = GraphRuntimeState.from_snapshot(snapshot)

        assert restored.total_tokens == 10
        assert restored.node_run_steps == 3
        assert restored.get_output("final") == {"result": True}
        assert restored.llm_usage.total_tokens == usage.total_tokens
        assert restored.ready_queue.qsize() == 1
        assert restored.ready_queue.get(timeout=0.01) == "node-A"

        restored_segment = restored.variable_pool.get(("node1", "value"))
        assert restored_segment is not None
        assert restored_segment.value == "payload"

        restored_execution = restored.graph_execution
        assert restored_execution.workflow_id == "wf-123"
        assert restored_execution.exceptions_count == 4
        assert restored_execution.started is True

        new_stub = StubCoordinator()
        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=new_stub,
            autospec=True,
        ):
            restored.attach_graph(mock_graph)

        assert new_stub.state == "configured"

    def test_loads_rehydrates_existing_instance(self) -> None:
        variable_pool = VariablePool()
        variable_pool.add(("node", "key"), "value")

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        state.total_tokens = 7
        state.node_run_steps = 2
        state.set_output("foo", "bar")
        state.ready_queue.put("node-1")

        execution = state.graph_execution
        execution.workflow_id = "wf-456"
        execution.started = True

        mock_graph = MagicMock()
        original_stub = StubCoordinator()
        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=original_stub,
            autospec=True,
        ):
            state.attach_graph(mock_graph)

        original_stub.state = "configured"
        snapshot = state.dumps()

        new_stub = StubCoordinator()
        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=new_stub,
            autospec=True,
        ):
            restored = GraphRuntimeState(variable_pool=VariablePool(), start_at=0.0)
            restored.attach_graph(mock_graph)
            restored.loads(snapshot)

        assert restored.total_tokens == 7
        assert restored.node_run_steps == 2
        assert restored.get_output("foo") == "bar"
        assert restored.ready_queue.qsize() == 1
        assert restored.ready_queue.get(timeout=0.01) == "node-1"

        restored_segment = restored.variable_pool.get(("node", "key"))
        assert restored_segment is not None
        assert restored_segment.value == "value"

        restored_execution = restored.graph_execution
        assert restored_execution.workflow_id == "wf-456"
        assert restored_execution.started is True

        assert new_stub.state == "configured"

    def test_snapshot_restore_preserves_updated_conversation_variable(self) -> None:
        variable_pool = VariablePool.from_bootstrap(
            conversation_variables=[
                StringVariable(name="session_name", value="before"),
            ],
        )
        variable_pool.add((CONVERSATION_VARIABLE_NODE_ID, "session_name"), "after")

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot = state.dumps()
        restored = GraphRuntimeState.from_snapshot(snapshot)

        restored_value = restored.variable_pool.get((
            CONVERSATION_VARIABLE_NODE_ID,
            "session_name",
        ))
        assert restored_value is not None
        assert restored_value.value == "after"

    def test_legacy_snapshot_restore_keeps_conversation_variables_writable(
        self,
    ) -> None:
        variable_pool = VariablePool.from_bootstrap(
            conversation_variables=[
                StringVariable(name="session_name", value="before"),
            ],
        )
        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        legacy_snapshot = _remove_writable_flags(json.loads(state.dumps()))

        restored = GraphRuntimeState.from_snapshot(legacy_snapshot)

        restored_variable = restored.variable_pool.get_variable((
            CONVERSATION_VARIABLE_NODE_ID,
            "session_name",
        ))
        assert restored_variable is not None
        assert restored_variable.writable is True

    def test_attach_graph_recovers_legacy_loop_and_iteration_working_mutability(
        self,
    ) -> None:
        variable_pool = VariablePool.empty()
        variable_pool.add(("loop-node", "counter"), 1, writable=True)
        variable_pool.add(("iteration-node", "index"), 2, writable=True)
        variable_pool.add(("iteration-node", "item"), "value", writable=True)
        variable_pool.add(("plain-node", "result"), "frozen")

        state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        legacy_snapshot = _remove_writable_flags(json.loads(state.dumps()))
        restored = GraphRuntimeState.from_snapshot(legacy_snapshot)

        loop_variable = restored.variable_pool.get_variable(("loop-node", "counter"))
        iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        iteration_item = restored.variable_pool.get_variable((
            "iteration-node",
            "item",
        ))
        plain_variable = restored.variable_pool.get_variable(("plain-node", "result"))
        assert loop_variable is not None
        assert iteration_index is not None
        assert iteration_item is not None
        assert plain_variable is not None
        assert loop_variable.writable is False
        assert iteration_index.writable is False
        assert iteration_item.writable is False
        assert plain_variable.writable is False

        graph = SimpleNamespace(
            nodes={
                "loop-node": SimpleNamespace(
                    id="loop-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.LOOP,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(
                        loop_variables=[SimpleNamespace(label="counter")],
                    ),
                ),
                "iteration-node": SimpleNamespace(
                    id="iteration-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.ITERATION,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
                "plain-node": SimpleNamespace(
                    id="plain-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.CODE,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
            },
            edges={},
            root_node=SimpleNamespace(
                id="loop-node",
                state=NodeState.UNKNOWN,
                node_type=BuiltinNodeTypes.LOOP,
                execution_type=MagicMock(),
            ),
            get_outgoing_edges=MagicMock(return_value=[]),
        )

        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=StubCoordinator(),
            autospec=True,
        ):
            restored.attach_graph(cast(Any, graph))

        restored_loop_variable = restored.variable_pool.get_variable((
            "loop-node",
            "counter",
        ))
        restored_iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        restored_iteration_item = restored.variable_pool.get_variable((
            "iteration-node",
            "item",
        ))
        restored_plain_variable = restored.variable_pool.get_variable((
            "plain-node",
            "result",
        ))
        assert restored_loop_variable is not None
        assert restored_iteration_index is not None
        assert restored_iteration_item is not None
        assert restored_plain_variable is not None
        assert restored_loop_variable.writable is True
        assert restored_iteration_index.writable is True
        assert restored_iteration_item.writable is True
        assert restored_plain_variable.writable is False

    def test_loads_completed_loop_snapshot_keeps_outputs_read_only(self) -> None:
        variable_pool = VariablePool.empty()
        variable_pool.add(("loop-node", "counter"), 1, writable=True)
        variable_pool.add(("iteration-node", "index"), 2, writable=True)
        variable_pool.add(("plain-node", "result"), "frozen")

        snapshot_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot_execution = snapshot_state.graph_execution
        snapshot_execution.start()
        snapshot_execution.complete()
        legacy_snapshot = _remove_writable_flags(json.loads(snapshot_state.dumps()))

        graph = SimpleNamespace(
            nodes={
                "loop-node": SimpleNamespace(
                    id="loop-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.LOOP,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(
                        loop_variables=[SimpleNamespace(label="counter")],
                    ),
                ),
                "iteration-node": SimpleNamespace(
                    id="iteration-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.ITERATION,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
                "plain-node": SimpleNamespace(
                    id="plain-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.CODE,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
            },
            edges={},
            root_node=SimpleNamespace(
                id="loop-node",
                state=NodeState.UNKNOWN,
                node_type=BuiltinNodeTypes.LOOP,
                execution_type=MagicMock(),
            ),
            get_outgoing_edges=MagicMock(return_value=[]),
        )

        restored = GraphRuntimeState(
            variable_pool=VariablePool.empty(),
            start_at=time(),
        )
        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=StubCoordinator(),
            autospec=True,
        ):
            restored.attach_graph(cast(Any, graph))
            restored.loads(legacy_snapshot)

        restored_loop_variable = restored.variable_pool.get_variable((
            "loop-node",
            "counter",
        ))
        restored_iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        restored_plain_variable = restored.variable_pool.get_variable((
            "plain-node",
            "result",
        ))
        assert restored_loop_variable is not None
        assert restored_iteration_index is not None
        assert restored_plain_variable is not None
        assert restored_loop_variable.writable is False
        assert restored_iteration_index.writable is False
        assert restored_plain_variable.writable is False

    def test_attach_graph_keeps_completed_loop_outputs_read_only_in_paused_workflow(
        self,
    ) -> None:
        variable_pool = VariablePool.empty()
        variable_pool.add(("loop-node", "counter"), 1, writable=False)
        variable_pool.add(("iteration-node", "index"), 2, writable=True)
        variable_pool.add(("iteration-node", "item"), "value", writable=True)
        variable_pool.add(("plain-node", "result"), "frozen")

        snapshot_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot_execution = snapshot_state.graph_execution
        snapshot_execution.start()
        legacy_snapshot = json.loads(snapshot_state.dumps())
        legacy_snapshot["graph_state"] = {
            "nodes": {
                "loop-node": NodeState.TAKEN.value,
                "iteration-node": NodeState.UNKNOWN.value,
                "plain-node": NodeState.UNKNOWN.value,
            },
            "edges": {},
        }
        legacy_snapshot = _remove_writable_flags(legacy_snapshot)

        restored = GraphRuntimeState.from_snapshot(legacy_snapshot)

        graph = SimpleNamespace(
            nodes={
                "loop-node": SimpleNamespace(
                    id="loop-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.LOOP,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(
                        loop_variables=[SimpleNamespace(label="counter")],
                    ),
                ),
                "iteration-node": SimpleNamespace(
                    id="iteration-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.ITERATION,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
                "plain-node": SimpleNamespace(
                    id="plain-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.CODE,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
            },
            edges={},
            root_node=SimpleNamespace(
                id="loop-node",
                state=NodeState.UNKNOWN,
                node_type=BuiltinNodeTypes.LOOP,
                execution_type=MagicMock(),
            ),
            get_outgoing_edges=MagicMock(return_value=[]),
        )

        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=StubCoordinator(),
            autospec=True,
        ):
            restored.attach_graph(cast(Any, graph))

        restored_loop_variable = restored.variable_pool.get_variable((
            "loop-node",
            "counter",
        ))
        restored_iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        restored_iteration_item = restored.variable_pool.get_variable((
            "iteration-node",
            "item",
        ))
        restored_plain_variable = restored.variable_pool.get_variable((
            "plain-node",
            "result",
        ))
        assert restored_loop_variable is not None
        assert restored_iteration_index is not None
        assert restored_iteration_item is not None
        assert restored_plain_variable is not None
        assert graph.nodes["loop-node"].state == NodeState.TAKEN
        assert graph.nodes["iteration-node"].state == NodeState.UNKNOWN
        assert restored_loop_variable.writable is False
        assert restored_iteration_index.writable is True
        assert restored_iteration_item.writable is True
        assert restored_plain_variable.writable is False

    def test_from_snapshot_current_paused_workflow_freezes_completed_loop_node(
        self,
    ) -> None:
        variable_pool = VariablePool.empty()
        variable_pool.add(("loop-node", "counter"), 1, writable=True)
        variable_pool.add(("iteration-node", "index"), 2, writable=True)
        variable_pool.add(("iteration-node", "item"), "value", writable=True)
        variable_pool.add(("plain-node", "result"), "frozen")

        graph = SimpleNamespace(
            nodes={
                "loop-node": SimpleNamespace(
                    id="loop-node",
                    state=NodeState.TAKEN,
                    node_type=BuiltinNodeTypes.LOOP,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(
                        loop_variables=[SimpleNamespace(label="counter")],
                    ),
                ),
                "iteration-node": SimpleNamespace(
                    id="iteration-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.ITERATION,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
                "plain-node": SimpleNamespace(
                    id="plain-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.CODE,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
            },
            edges={},
            root_node=SimpleNamespace(
                id="loop-node",
                state=NodeState.TAKEN,
                node_type=BuiltinNodeTypes.LOOP,
                execution_type=MagicMock(),
            ),
            get_outgoing_edges=MagicMock(return_value=[]),
        )

        snapshot_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot_execution = snapshot_state.graph_execution
        snapshot_execution.start()

        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=StubCoordinator(),
            autospec=True,
        ):
            snapshot_state.attach_graph(cast(Any, graph))

        snapshot = json.loads(snapshot_state.dumps())
        restored = GraphRuntimeState.from_snapshot(snapshot)

        restored_loop_variable = restored.variable_pool.get_variable((
            "loop-node",
            "counter",
        ))
        restored_iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        restored_iteration_item = restored.variable_pool.get_variable((
            "iteration-node",
            "item",
        ))
        restored_plain_variable = restored.variable_pool.get_variable((
            "plain-node",
            "result",
        ))
        assert restored_loop_variable is not None
        assert restored_iteration_index is not None
        assert restored_iteration_item is not None
        assert restored_plain_variable is not None
        assert restored_loop_variable.writable is False
        assert restored_iteration_index.writable is True
        assert restored_iteration_item.writable is True
        assert restored_plain_variable.writable is False

    def test_loads_current_aborted_snapshot_freezes_terminal_working_variables(
        self,
    ) -> None:
        variable_pool = VariablePool.empty()
        variable_pool.add(("loop-node", "counter"), 1, writable=True)
        variable_pool.add(("iteration-node", "index"), 2, writable=True)
        variable_pool.add(("iteration-node", "item"), "value", writable=True)
        variable_pool.add(("plain-node", "result"), "frozen")

        snapshot_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time())
        snapshot_execution = snapshot_state.graph_execution
        snapshot_execution.start()
        snapshot_execution.abort("user requested stop")

        graph = SimpleNamespace(
            nodes={
                "loop-node": SimpleNamespace(
                    id="loop-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.LOOP,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(
                        loop_variables=[SimpleNamespace(label="counter")],
                    ),
                ),
                "iteration-node": SimpleNamespace(
                    id="iteration-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.ITERATION,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
                "plain-node": SimpleNamespace(
                    id="plain-node",
                    state=NodeState.UNKNOWN,
                    node_type=BuiltinNodeTypes.CODE,
                    execution_type=MagicMock(),
                    node_data=SimpleNamespace(),
                ),
            },
            edges={},
            root_node=SimpleNamespace(
                id="loop-node",
                state=NodeState.UNKNOWN,
                node_type=BuiltinNodeTypes.LOOP,
                execution_type=MagicMock(),
            ),
            get_outgoing_edges=MagicMock(return_value=[]),
        )

        with patch.object(
            GraphRuntimeState,
            "_build_response_coordinator",
            return_value=StubCoordinator(),
            autospec=True,
        ):
            snapshot_state.attach_graph(cast(Any, graph))

        snapshot = json.loads(snapshot_state.dumps())
        restored = GraphRuntimeState.from_snapshot(snapshot)

        restored_loop_variable = restored.variable_pool.get_variable((
            "loop-node",
            "counter",
        ))
        restored_iteration_index = restored.variable_pool.get_variable((
            "iteration-node",
            "index",
        ))
        restored_iteration_item = restored.variable_pool.get_variable((
            "iteration-node",
            "item",
        ))
        restored_plain_variable = restored.variable_pool.get_variable((
            "plain-node",
            "result",
        ))
        assert restored_loop_variable is not None
        assert restored_iteration_index is not None
        assert restored_iteration_item is not None
        assert restored_plain_variable is not None
        assert restored_loop_variable.writable is False
        assert restored_iteration_index.writable is False
        assert restored_iteration_item.writable is False
        assert restored_plain_variable.writable is False
